import os
import pandas as pd
import json
import xml.etree.ElementTree as ET
import requests
import uvicorn
import random
import string
import time
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException, Response, Body
from fastapi.responses import FileResponse, HTMLResponse
import tempfile
import shutil
from dotenv import load_dotenv
import psutil  # For system metrics like CPU usage
import platform  # For OS-specific details

# Load environment variables from .env file
load_dotenv()

# Configure logging
LOG_FILE = "salesforce_standard_object_metrics_app.log"  # Specify the log file name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
    filemode='a'
)
logger = logging.getLogger(__name__)

# Configuration variables
SALESFORCE_CLIENT_ID = os.getenv("SALESFORCE_CLIENT_ID")
SALESFORCE_CLIENT_SECRET = os.getenv("SALESFORCE_CLIENT_SECRET")
SALESFORCE_USERNAME = os.getenv("SALESFORCE_USERNAME")
SALESFORCE_PASSWORD = os.getenv("SALESFORCE_PASSWORD")
SALESFORCE_TOKEN_URL = os.getenv("SALESFORCE_TOKEN_URL")

BATCH_SIZE = 200  # For potential batch operations
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5
MAX_RECENT_RECORDS = 10

FIELDS = [
    "Id", "Name", "AccountNumber", "Site", "Type", "Industry",
    "AnnualRevenue", "Rating", "Phone", "Fax", "Website",
    "TickerSymbol", "Ownership", "NumberOfEmployees"
]

app = FastAPI()

class SalesforceError(Exception):
    pass

class FailedRecord:
    def __init__(self, record: Dict[str, Any], error: str):
        self.record = record
        self.error = error
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'record': self.record,
            'error': self.error,
            'timestamp': self.timestamp
        }

class RecentDataManager:
    def __init__(self, max_records: int = MAX_RECENT_RECORDS):
        self.max_records = max_records
        self.recent_records: List[Dict[str, Any]] = []

    def add_records(self, records: List[Dict[str, Any]], operation_type: str = "insert"):
        for record in records:
            record['operation_type'] = operation_type
        self.recent_records = (records + self.recent_records)[:self.max_records]

    def get_records(self) -> List[Dict[str, Any]]:
        return self.recent_records

recent_data_manager = RecentDataManager()

def get_system_metrics() -> Dict[str, Any]:
    """Collects detailed system metrics."""
    metrics = {
        "cpu_usage_percent": psutil.cpu_percent(interval=0.1),
        "memory_usage_percent": psutil.virtual_memory().percent,
        "disk_usage_percent": psutil.disk_usage('/').percent,  # Assuming root partition
        "system_load_avg": psutil.getloadavg(),  # (1 min, 5 min, 15 min) load averages
        "cpu_count": psutil.cpu_count(),  # Number of logical CPUs
        "cpu_freq_current": psutil.cpu_freq().current,
        "cpu_freq_min": psutil.cpu_freq().min,
        "cpu_freq_max": psutil.cpu_freq().max,
        "system_uptime": time.time() - psutil.boot_time(),  # Uptime in seconds
        "network_bytes_sent": psutil.net_io_counters().bytes_sent,
        "network_bytes_recv": psutil.net_io_counters().bytes_recv,
        "operating_system": platform.platform(),  # Full OS details
        "python_version": platform.python_version(),
    }
    return metrics


def read_file_data(file_path: str) -> List[Dict[str, Any]]:
    """Read data from CSV, JSON, or XML file."""
    logger.info(f"Reading data from file: {file_path}")
    file_ext = os.path.splitext(file_path)[1].lower()

    try:
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
            return df.to_dict('records')
        elif file_ext == '.json':
            with open(file_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'records' in data:
                    return data['records']
                return [data]
        elif file_ext == '.xml':
            tree = ET.parse(file_path)
            root = tree.getroot()
            records = []
            for record in root.findall('.//record'):
                record_dict = {}
                for field in record:
                    record_dict[field.tag] = field.text
                records.append(record_dict)
            return records
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise

def save_to_file(data: List[Dict[str, Any]], file_path: str) -> None:
    """Save data to CSV, JSON, or XML file."""
    logger.info(f"Saving data to file: {file_path}")
    file_ext = os.path.splitext(file_path)[1].lower()

    try:
        if file_ext == '.csv':
            pd.DataFrame(data).to_csv(file_path, index=False)
        elif file_ext == '.json':
            with open(file_path, 'w') as f:
                json.dump({'records': data}, f, indent=4)
        elif file_ext == '.xml':
            root = ET.Element('records')
            for item in data:
                record = ET.SubElement(root, 'record')
                for key, value in item.items():
                    field = ET.SubElement(record, key)
                    field.text = str(value) if value is not None else ""
            def indent_xml(elem: ET.Element, level: int = 0) -> None:
                """Add proper indentation to XML elements."""
                i = "\n" + level * "  "
                if len(elem):
                    if not elem.text or not elem.text.strip():
                        elem.text = i + "  "
                    if not elem.tail or not elem.tail.strip():
                        elem.tail = i
                    for elem in elem:
                        indent_xml(elem, level + 1)
                    if not elem.tail or not elem.tail.strip():
                        elem.tail = i
                else:
                    if level and (not elem.tail or not elem.tail.strip()):
                        elem.tail = i
            indent_xml(root)
            tree = ET.ElementTree(root)
            with open(file_path, 'wb') as f:
                f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
                tree.write(f, encoding='utf-8', xml_declaration=False)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    except Exception as e:
        logger.error(f"Error saving data to file {file_path}: {e}")
        raise

def authenticate_salesforce() -> Optional['Salesforce']:
    """Authenticate with Salesforce and return a Salesforce instance."""
    try:
        if not all([SALESFORCE_CLIENT_ID, SALESFORCE_CLIENT_SECRET,
                    SALESFORCE_USERNAME, SALESFORCE_PASSWORD]):
            raise SalesforceError("Missing required Salesforce credentials")
        response = requests.post(
            SALESFORCE_TOKEN_URL,
            data={
                'grant_type': 'password',
                'client_id': SALESFORCE_CLIENT_ID,
                'client_secret': SALESFORCE_CLIENT_SECRET,
                'username': SALESFORCE_USERNAME,
                'password': SALESFORCE_PASSWORD
            }
        )
        response.raise_for_status()
        auth_data = response.json()
        from simple_salesforce import Salesforce  # Import here to ensure credentials are loaded
        sf = Salesforce(
            instance_url=auth_data['instance_url'],
            session_id=auth_data['access_token']
        )
        logger.info("Salesforce authentication successful!")
        return sf
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise SalesforceError(f"Authentication failed: {str(e)}")

def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    logger.debug(f"Cleaning record: {record}")
    cleaned = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = 0 if key in ['AnnualRevenue', 'NumberOfEmployees'] else None
        elif isinstance(value, float):
            cleaned[key] = int(value) if key in ['AnnualRevenue', 'NumberOfEmployees'] else str(value)
        else:
            cleaned[key] = value
    logger.debug(f"Cleaned record: {cleaned}")
    return cleaned

def check_storage_availability(sf: 'Salesforce') -> bool:
    logger.info("Checking Salesforce storage availability...")
    try:
        test_record = {
            "Name": f"Test_Storage_Check_{random.randint(1000, 9999)}"
        }
        response = sf.Account.create(test_record)
        if response.get('success'):
            sf.Account.delete(response['id'])
            logger.info("Storage availability check successful.")
            return True
        logger.warning("Storage availability check: Test record creation failed.")
        return False
    except Exception as e:
        if "STORAGE_LIMIT_EXCEEDED" in str(e):
            logger.error("Storage limit has been reached.")
        else:
            logger.error(f"Error checking storage availability: {str(e)}")
        return False

def insert_with_retry(
    sf: 'Salesforce',
    record: Dict[str, Any],
    max_attempts: int = RETRY_ATTEMPTS
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    logger.info(f"Attempting to insert record: {record.get('Name')}")
    for attempt in range(max_attempts):
        try:
            response = sf.Account.create(record)
            if response.get('success'):
                logger.info(f"Successfully inserted record: {record.get('Name')} (ID: {response['id']})")
                return response, None
            logger.warning(f"Insert failed for record {record.get('Name')}: {response}")
            return None, "Insert failed"
        except Exception as e:
            if "STORAGE_LIMIT_EXCEEDED" in str(e):
                logger.error(f"Storage limit exceeded when inserting record {record.get('Name')}")
                return None, "Storage limit exceeded"
            if attempt < max_attempts - 1:
                logger.warning(f"Retry attempt {attempt + 1} for record {record.get('Name')}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"Failed to insert record {record.get('Name')} after {max_attempts} attempts: {e}")
                return None, str(e)
    return None, "Max retry attempts reached"

def batch_insert_data(
    sf: 'Salesforce',
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[FailedRecord], Dict[str, Any]]:
    logger.info(f"Starting batch insert of {len(records)} records...")
    start_time = time.time()
    system_metrics_before = get_system_metrics()  # Capture metrics before insertion
    successful_records = []
    failed_records = []

    if not check_storage_availability(sf):
        logger.error("Storage limit check failed. Cannot proceed with insertions.")
        failed_records = [FailedRecord(record, "Storage limit exceeded") for record in records]
        insertion_time = time.time() - start_time
        system_metrics_after = get_system_metrics() # Capture metrics after operation
        metrics = {
            "insertion_time": insertion_time,
            "system_metrics_before": system_metrics_before,
            "system_metrics_after": system_metrics_after,
            "records_processed": len(records),
            "successful_records": 0,
            "failed_records": len(failed_records)
        }
        return successful_records, failed_records, metrics

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1} of {len(batch)} records")
        for record in batch:
            response, error = insert_with_retry(sf, record)
            if error:
                failed_records.append(FailedRecord(record, error))
                if "Storage limit exceeded" in error:
                    remaining_records = records[i:]
                    failed_records.extend([
                        FailedRecord(r, 'Storage limit exceeded')
                        for r in remaining_records
                    ])
                    insertion_time = time.time() - start_time
                    system_metrics_after = get_system_metrics()
                    metrics = {
                        "insertion_time": insertion_time,
                        "system_metrics_before": system_metrics_before,
                        "system_metrics_after": system_metrics_after,
                        "records_processed": len(records),
                        "successful_records": len(successful_records),
                        "failed_records": len(failed_records)
                    }
                    return successful_records, failed_records, metrics
            else:
                record_data = {
                    'id': response['id'],
                    'name': record.get('Name'),
                    'timestamp': datetime.now().isoformat()
                }
                record_data.update(record)
                successful_records.append(record_data)
        time.sleep(2)

    insertion_time = time.time() - start_time
    system_metrics_after = get_system_metrics() # Capture metrics after the operation
    metrics = {
        "insertion_time": insertion_time,
        "system_metrics_before": system_metrics_before,
        "system_metrics_after": system_metrics_after,
        "records_processed": len(records),
        "successful_records": len(successful_records),
        "failed_records": len(failed_records)
    }
    logger.info(f"Batch insert completed. {len(successful_records)} successful, {len(failed_records)} failed.")
    return successful_records, failed_records, metrics

def insert_data_from_file(
    sf: 'Salesforce',
    file_path: str,
    salesforce_object: str = "Account"
) -> Tuple[List[FailedRecord], List[Dict[str, Any]], Dict[str, Any]]:
    try:
        records = read_file_data(file_path)
        logger.info(f"Inserting data from {file_path} into {salesforce_object}...")
        records = [clean_record({k: v for k, v in record.items() if k in FIELDS and k != 'Id'})
                   for record in records]
        successful_records, failed_records, insertion_metrics = batch_insert_data(sf, records)
        if failed_records:
            failed_file = f'failed_records_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            save_to_file([fr.to_dict() for fr in failed_records], failed_file)
            logger.info(f"Failed records saved to '{failed_file}'")
        if successful_records:
            recent_data_manager.add_records(successful_records, "insert")
        return failed_records, successful_records, insertion_metrics
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise

def retrieve_data_to_file(
    sf: 'Salesforce',
    salesforce_object: str = "Account",
    output_format: str = "csv"
) -> Tuple[str, Optional[List[Dict[str, Any]]], Dict[str, Any]]:
    try:
        logger.info(f"Retrieving data from Salesforce object: {salesforce_object}")
        start_time = time.time()
        system_metrics_before = get_system_metrics()  # Capture metrics before retrieval

        query = f"SELECT {', '.join(FIELDS)} FROM {salesforce_object}"
        data = sf.query_all(query)
        records = [{k: v for k, v in record.items() if k != 'attributes'}
                   for record in data['records']]
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"salesforce_data_{timestamp}.{output_format}")
        save_to_file(records, output_file)
        end_time = time.time()
        system_metrics_after = get_system_metrics()  # Capture metrics after retrieval
        retrieval_time = end_time - start_time
        metrics = {
            "retrieval_time": retrieval_time,
            "system_metrics_before": system_metrics_before,
            "system_metrics_after": system_metrics_after,
            "record_count": len(records)
        }
        logger.info(f"Data saved to {output_file} with {len(records)} records.")
        return output_file, records, metrics
    except Exception as e:
        logger.error(f"Failed to retrieve data: {str(e)}")
        raise

def update_record_by_id(
    sf: 'Salesforce',
    record_id: str,
    update_data: Dict[str, Any],
    max_attempts: int = RETRY_ATTEMPTS,
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Updates a single record in Salesforce by its ID.

    Returns:
        A tuple: (True, None, metrics) if successful, (False, error_message, metrics) if failed.
    """
    logger.info(f"Attempting to update record with Id: {record_id}")
    start_time = time.time()
    system_metrics_before = get_system_metrics() # System metrics before update
    for attempt in range(max_attempts):
        try:
            response = sf.Account.update(record_id, update_data)
            if response == 204:
                update_time = time.time() - start_time
                system_metrics_after = get_system_metrics() # System metrics after update
                metrics = {
                    "update_time": update_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after,
                }
                logger.info(f"Successfully updated record with Id: {record_id}")
                return True, None, metrics
            else:
                update_time = time.time() - start_time
                system_metrics_after = get_system_metrics() # System metrics after failed update
                metrics = {
                    "update_time": update_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after,
                }
                logger.warning(f"Update failed for record Id {record_id} with status code: {response}")
                return False, f"Update failed with status code: {response}", metrics
        except Exception as e:
            if "ENTITY_IS_DELETED" in str(e):
                update_time = time.time() - start_time
                system_metrics_after = get_system_metrics()  # Metrics even if deleted
                metrics = {
                    "update_time": update_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after,
                }
                logger.warning(f"Record {record_id} has been deleted. Skipping update.")
                return False, "Entity is deleted", metrics
            if attempt < max_attempts - 1:
                logger.warning(f"Retry attempt {attempt + 1} for record Id {record_id}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                update_time = time.time() - start_time
                system_metrics_after = get_system_metrics()  # Metrics after max retries
                metrics = {
                    "update_time": update_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after
                }
                logger.error(f"Failed to update record Id {record_id} after {max_attempts} attempts: {e}")
                return False, str(e), metrics
    system_metrics_after = get_system_metrics() # System metrics after all attempts
    return False, "Max retry attempts reached", {"system_metrics_before": system_metrics_before, "system_metrics_after": system_metrics_after} #Return empty metrics after retry attempts


def delete_record_by_id(
    sf: 'Salesforce',
    record_id: str,
    max_attempts: int = RETRY_ATTEMPTS
) -> Tuple[bool, Optional[str], Dict[str, Any]]:
    """Deletes a single record from Salesforce by its ID.

    Returns:
        A tuple: (True, None, metrics) if successful, (False, error_message, metrics) if failed.
    """
    logger.info(f"Attempting to delete record with Id: {record_id}")
    start_time = time.time()
    system_metrics_before = get_system_metrics() # System metrics before deletion
    for attempt in range(max_attempts):
        try:
            response = sf.Account.delete(record_id)
            if response == 204:
                delete_time = time.time() - start_time
                system_metrics_after = get_system_metrics() # System metrics after deletion
                metrics = {
                    "delete_time": delete_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after,
                }
                logger.info(f"Successfully deleted record with Id: {record_id}")
                return True, None, metrics
            else:
                delete_time = time.time() - start_time
                system_metrics_after = get_system_metrics() # Metrics after failed attempt
                metrics = {
                    "delete_time": delete_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after,
                }
                logger.warning(f"Delete failed for record Id {record_id} with status code: {response}")
                return False, f"Delete failed with status code: {response}", metrics
        except Exception as e:
            if attempt < max_attempts - 1:
                logger.warning(f"Retry attempt {attempt + 1} for record Id {record_id}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                delete_time = time.time() - start_time
                system_metrics_after = get_system_metrics() # Metrics after final failed attempt
                metrics = {
                    "delete_time": delete_time,
                    "system_metrics_before": system_metrics_before,
                    "system_metrics_after": system_metrics_after
                }
                logger.error(f"Failed to delete record Id {record_id} after {max_attempts} attempts: {e}")
                return False, str(e), metrics

    system_metrics_after = get_system_metrics()  # System metrics after all retries fail
    return False, "Max retry attempts reached", {"system_metrics_before": system_metrics_before, "system_metrics_after": system_metrics_after}


def process_uploaded_file(file_path: str) -> Dict[str, Any]:
    logger.info(f"Processing uploaded file: {file_path}")
    sf = authenticate_salesforce()
    if not sf:
        return {"status": "error", "message": "Failed to authenticate with Salesforce"}
    try:
        failed_records, successful_records, insertion_metrics = insert_data_from_file(sf, file_path)
        result = {
            "status": "success",
            "total_processed": len(failed_records) + len(successful_records),
            "successful_records": len(successful_records),
            "failed_records": len(failed_records),
            "timestamp": datetime.now().isoformat(),
            "metrics": insertion_metrics
        }
        if successful_records:
            result["recent_data"] = recent_data_manager.get_records()
        logger.info(f"File processing result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}")
        return {"status": "error", "message": str(e)}

def retrieve_latest_data(output_format: str = "csv") -> Dict[str, Any]:
    logger.info(f"Retrieving latest data in {output_format} format...")
    sf = authenticate_salesforce()
    if not sf:
        return {"status": "error", "message": "Failed to authenticate with Salesforce"}
    try:
        output_file, records, retrieval_metrics = retrieve_data_to_file(sf, output_format=output_format)
        if records:
            recent_data_manager.add_records(records)
        result = {
            "status": "success",
            "file_path": output_file,
            "record_count": len(records) if records else 0,
            "timestamp": datetime.now().isoformat(),
            "metrics": retrieval_metrics,
            "recent_data": recent_data_manager.get_records()
        }
        logger.info(f"Data retrieval result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error retrieving data: {e}")
        return {"status": "error", "message": str(e)}

@app.post("/upload")
async def upload_file_api(file: UploadFile = File(...)):
    logger.info(f"Received file upload request: {file.filename}")
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp_file:
        shutil.copyfileobj(file.file, tmp_file)
        tmp_path = tmp_file.name
    result = process_uploaded_file(tmp_path)
    os.unlink(tmp_path)
    logger.info(f"File upload request processed: {result}")
    return result

@app.get("/retrieve/{format}")
async def retrieve_data_api(format: str = "csv"):
    logger.info(f"Received data retrieval request, format: {format}")
    if format not in ["csv", "json", "xml"]:
        raise HTTPException(status_code=400, detail="Invalid format. Supported formats are: csv, json, xml")
    result = retrieve_latest_data(format)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    if format == "csv":
        return FileResponse(result["file_path"], media_type="text/csv", filename=os.path.basename(result["file_path"]))
    elif format == "json":
        with open(result["file_path"], "r") as f:
            data = json.load(f)
        return {"data": data, "metrics": result.get("metrics")}
    elif format == "xml":
        return FileResponse(result["file_path"], media_type="application/xml", filename=os.path.basename(result["file_path"]))

@app.put("/update/{record_id}")
async def update_record_api(record_id: str, update_data: Dict[str, Any] = Body(...)):
    logger.info(f"Received update request for record ID: {record_id}")
    sf = authenticate_salesforce()
    if not sf:
        raise HTTPException(status_code=500, detail="Failed to authenticate with Salesforce")
    try:
        success, message, update_metrics = update_record_by_id(sf, record_id, update_data)
        if success:
            return {
                "status": "success",
                "message": f"Record with ID {record_id} updated successfully.",
                "metrics": update_metrics
            }
        else:
            raise HTTPException(status_code=400, detail=message or "Failed to update record.")
    except Exception as e:
        logger.error(f"Error processing update request: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete/{record_id}")
async def delete_record_api(record_id: str):
    logger.info(f"Received delete request for record ID: {record_id}")
    sf = authenticate_salesforce()
    if not sf:
        raise HTTPException(status_code=500, detail="Failed to authenticate with Salesforce")
    try:
        success, message, delete_metrics = delete_record_by_id(sf, record_id)
        if success:
            return {
                "status": "success",
                "message": f"Record with ID {record_id} deleted successfully.",
                "metrics": delete_metrics
            }
        else:
            raise HTTPException(status_code=400, detail=message or "Failed to delete record.")
    except Exception as e:
        logger.error(f"Error processing deletion request: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    try:
        os.makedirs("data", exist_ok=True)
        os.makedirs("output", exist_ok=True)
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        logger.error(f"An error occurred during execution: {str(e)}")
        raise