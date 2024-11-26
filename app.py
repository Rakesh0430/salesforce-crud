import os
import pandas as pd
import json
import xml.etree.ElementTree as ET
import requests
import random
import string
from simple_salesforce import Salesforce
import time
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
import logging
import gradio as gr
from fastapi import FastAPI, UploadFile, File
import tempfile
import shutil
import uvicorn
from dotenv import load_dotenv

# Initialize logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration variables with defaults
SALESFORCE_CLIENT_ID = os.getenv("SALESFORCE_CLIENT_ID","3MVG9GCMQoQ6rpzSptSEYMuRgsRYDC14Wkpic_v58a3C4_REZKWTh2zCtdszeqAk.o7iunn1FWk7MvivFmguO")
SALESFORCE_CLIENT_SECRET = os.getenv("SALESFORCE_CLIENT_SECRET","316B02C399045B59970DD0A6A9C70F78B46EF0149264D45D1978ECA7409DF7D")
SALESFORCE_USERNAME = os.getenv("SALESFORCE_USERNAME","rakesh@iscs.sandbox")
SALESFORCE_PASSWORD = os.getenv("SALESFORCE_PASSWORD","12345678@LrsI0n7e39KgO2UtsBRMb5iNilc")
SALESFORCE_TOKEN_URL = os.getenv("SALESFORCE_TOKEN_URL", "https://login.salesforce.com/services/oauth2/token")

# Constants
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
MAX_RECENT_RECORDS = int(os.getenv("MAX_RECENT_RECORDS", "10"))

# Salesforce fields to process
FIELDS = [
    "Name", "AccountNumber", "Site", "Type", "Industry", 
    "AnnualRevenue", "Rating", "Phone", "Fax", "Website", 
    "TickerSymbol", "Ownership", "NumberOfEmployees"
]

# Initialize FastAPI
app = FastAPI(
    title="Salesforce Data Integration",
    description="API for Salesforce data integration with support for multiple file formats",
    version="1.0.0"
)

class SalesforceError(Exception):
    """Custom exception for Salesforce-related errors"""
    pass

class FailedRecord:
    """Class to handle failed record processing"""
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
    """Manages recent data records with a maximum limit"""
    def __init__(self, max_records: int = MAX_RECENT_RECORDS):
        self.max_records = max_records
        self.recent_records: List[Dict[str, Any]] = []
    
    def add_records(self, records: List[Dict[str, Any]]):
        self.recent_records = (records + self.recent_records)[:self.max_records]
    
    def get_records(self) -> List[Dict[str, Any]]:
        return self.recent_records

# Initialize recent data manager
recent_data_manager = RecentDataManager()

def read_file_data(file_path: str) -> List[Dict[str, Any]]:
    """Read data from various file formats"""
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
        logger.error(f"Error reading file {file_path}: {str(e)}")
        raise

def save_to_file(data: List[Dict[str, Any]], file_path: str) -> None:
    """Save data to various file formats"""
    try:
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            pd.DataFrame(data).to_csv(file_path, index=False)
        
        elif file_ext == '.json':
            with open(file_path, 'w') as f:
                json.dump({'records': data}, f, indent=2)
        
        elif file_ext == '.xml':
            root = ET.Element('records')
            for item in data:
                record = ET.SubElement(root, 'record')
                for key, value in item.items():
                    field = ET.SubElement(record, key)
                    field.text = str(value)
            tree = ET.ElementTree(root)
            tree.write(file_path, encoding='utf-8', xml_declaration=True)
        
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
            
    except Exception as e:
        logger.error(f"Error saving file {file_path}: {str(e)}")
        raise

def authenticate_salesforce() -> Optional[Salesforce]:
    """Authenticate with Salesforce"""
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
        
        sf = Salesforce(
            instance_url=auth_data['instance_url'],
            session_id=auth_data['access_token']
        )
        logger.info("Salesforce authentication successful")
        return sf
        
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}")
        raise SalesforceError(f"Authentication failed: {str(e)}")

def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and validate record data"""
    cleaned = {}
    for key, value in record.items():
        if pd.isna(value):
            cleaned[key] = 0 if key in ['AnnualRevenue', 'NumberOfEmployees'] else None
        elif isinstance(value, float):
            cleaned[key] = int(value) if key in ['AnnualRevenue', 'NumberOfEmployees'] else str(value)
        else:
            cleaned[key] = str(value) if value is not None else None
    return cleaned

def check_storage_availability(sf: Salesforce) -> bool:
    """Check if Salesforce storage limit is not exceeded"""
    try:
        test_record = {
            "Name": f"Test_Storage_Check_{random.randint(1000, 9999)}"
        }
        response = sf.Account.create(test_record)
        if response.get('success'):
            sf.Account.delete(response['id'])
            return True
        return False
    except Exception as e:
        logger.error(f"Storage check failed: {str(e)}")
        return False

def insert_with_retry(
    sf: Salesforce,
    record: Dict[str, Any],
    max_attempts: int = RETRY_ATTEMPTS
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Insert a record with retry mechanism"""
    for attempt in range(max_attempts):
        try:
            response = sf.Account.create(record)
            if response.get('success'):
                return response, None
            return None, "Insert failed"
        except Exception as e:
            if "STORAGE_LIMIT_EXCEEDED" in str(e):
                return None, "Storage limit exceeded"
            if attempt < max_attempts - 1:
                logger.warning(f"Retry attempt {attempt + 1} for record {record.get('Name')}")
                time.sleep(RETRY_DELAY * (attempt + 1))
                continue
            return None, str(e)
    return None, "Max retry attempts reached"

def batch_insert_data(
    sf: Salesforce,
    records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[FailedRecord]]:
    """Insert records in batches"""
    successful_records = []
    failed_records = []
    
    if not check_storage_availability(sf):
        logger.error("Storage limit check failed")
        failed_records = [FailedRecord(record, "Storage limit exceeded") for record in records]
        return successful_records, failed_records
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1}")
        
        for record in batch:
            response, error = insert_with_retry(sf, record)
            
            if error:
                failed_records.append(FailedRecord(record, error))
                logger.error(f"Failed to insert {record.get('Name')}: {error}")
                if "Storage limit exceeded" in error:
                    remaining_records = records[i:]
                    failed_records.extend([
                        FailedRecord(r, 'Storage limit exceeded')
                        for r in remaining_records
                    ])
                    return successful_records, failed_records
            else:
                record_data = {
                    'id': response['id'],
                    'name': record.get('Name'),
                    'timestamp': datetime.now().isoformat()
                }
                record_data.update(record)
                successful_records.append(record_data)
                logger.info(f"Successfully inserted: {record.get('Name')}")
        
        time.sleep(2)  # Rate limiting
    
    return successful_records, failed_records

def insert_data_from_file(
    sf: Salesforce,
    file_path: str,
    salesforce_object: str = "Account"
) -> Tuple[List[FailedRecord], List[Dict[str, Any]]]:
    """Process file data and insert into Salesforce"""
    try:
        records = read_file_data(file_path)
        logger.info(f"Processing {len(records)} records from {file_path}")
        
        records = [clean_record({k: v for k, v in record.items() if k in FIELDS}) 
                  for record in records]
        
        successful_records, failed_records = batch_insert_data(sf, records)
        
        if failed_records:
            failed_file = f'failed_records_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            save_to_file([fr.to_dict() for fr in failed_records], failed_file)
            logger.info(f"Failed records saved to {failed_file}")
        
        if successful_records:
            recent_data_manager.add_records(successful_records)
        
        return failed_records, successful_records
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise

def retrieve_data_to_file(
    sf: Salesforce,
    salesforce_object: str = "Account",
    output_format: str = "csv"
) -> Tuple[str, Optional[List[Dict[str, Any]]]]:
    """Retrieve data from Salesforce and save to file"""
    try:
        query = f"SELECT {', '.join(FIELDS)} FROM {salesforce_object}"
        data = sf.query_all(query)
        records = [{k: v for k, v in record.items() if k != 'attributes'} 
                  for record in data['records']]
        
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"salesforce_data_{timestamp}.{output_format}")
        
        save_to_file(records, output_file)
        logger.info(f"Retrieved {len(records)} records")
        return output_file, records
        
    except Exception as e:
        logger.error(f"Failed to retrieve data: {str(e)}")
        raise

def process_uploaded_file(file_path: str) -> Dict[str, Any]:
    """Process uploaded file and return results"""
    sf = authenticate_salesforce()
    if not sf:
        return {"status": "error", "message": "Failed to authenticate with Salesforce"}
    
    try:
        failed_records, successful_records = insert_data_from_file(sf, file_path)
        result = {
            "status": "success",
            "total_processed": len(failed_records) + len(successful_records),
            "successful_records": len(successful_records),
            "failed_records": len(failed_records),
            "timestamp": datetime.now().isoformat()
        }
        
        if successful_records:
            result["recent_data"] = recent_data_manager.get_records()
        
        return result
        
    except Exception as e:
        return {"status": "error", "message": str(e)}

def retrieve_latest_data(output_format: str = "csv") -> Dict[str, Any]:
    """Retrieve latest data from Salesforce"""
    sf = authenticate_salesforce()
    if not sf:
        return {"status": "error", "message": "Failed to authenticate with Salesforce"}
    
    try:
        output_file, records = retrieve_data_to_file(sf, output_format=output_format)
        if records:
            recent_data_manager.add_records(records)
        
        return {
            "status": "success",
            "file_path": output_file,
            "record_count": len(records) if records else 0,
            "timestamp": datetime.now().isoformat(),
            "recent_data": recent_data_manager.get_records()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# FastAPI endpoints
# Continue from previous code...

# FastAPI endpoints
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """API endpoint for file upload"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp_file:
        shutil.copyfileobj(file.file, tmp_file)
        tmp_path = tmp_file.name
    
    try:
        result = process_uploaded_file(tmp_path)
        return result
    finally:
        os.unlink(tmp_path)

@app.get("/retrieve/{format}")
async def retrieve_data(format: str = "csv"):
    """API endpoint for data retrieval"""
    if format not in ["csv", "json", "xml"]:
        return {"status": "error", "message": "Unsupported format"}
    return retrieve_latest_data(format)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        sf = authenticate_salesforce()
        if sf:
            return {"status": "healthy", "message": "Service is running and can connect to Salesforce"}
        return {"status": "unhealthy", "message": "Cannot connect to Salesforce"}
    except Exception as e:
        return {"status": "unhealthy", "message": str(e)}

# Gradio Interface Functions
def format_recent_data(data: List[Dict[str, Any]]) -> str:
    """Format recent data for display"""
    if not data:
        return "No recent records"
    
    formatted = "Recent Records:\n\n"
    for record in data:
        formatted += f"Name: {record.get('Name', 'N/A')}\n"
        formatted += f"Type: {record.get('Type', 'N/A')}\n"
        formatted += f"Industry: {record.get('Industry', 'N/A')}\n"
        formatted += f"Annual Revenue: {record.get('AnnualRevenue', 'N/A')}\n"
        formatted += f"Timestamp: {record.get('timestamp', 'N/A')}\n"
        formatted += "-" * 50 + "\n"
    return formatted

def upload_file_gradio(file) -> Tuple[str, str]:
    """Handle file upload in Gradio interface"""
    if file is None:
        return "No file uploaded", "No data to display"
    
    try:
        result = process_uploaded_file(file.name)
        status_message = f"""
        Upload Status: {result['status']}
        Total Processed: {result.get('total_processed', 0)}
        Successful: {result.get('successful_records', 0)}
        Failed: {result.get('failed_records', 0)}
        Timestamp: {result.get('timestamp', 'N/A')}
        """
        recent_data = format_recent_data(result.get("recent_data", []))
        return status_message, recent_data
    except Exception as e:
        return f"Error: {str(e)}", "No data to display"

def retrieve_data_gradio(format_type: str) -> Tuple[str, str]:
    """Handle data retrieval in Gradio interface"""
    try:
        result = retrieve_latest_data(format_type)
        if result["status"] == "success":
            status_message = f"""
            Data retrieved successfully
            File: {result['file_path']}
            Records: {result['record_count']}
            Timestamp: {result['timestamp']}
            """
            recent_data = format_recent_data(result.get("recent_data", []))
        else:
            status_message = f"Error: {result.get('message', 'Unknown error')}"
            recent_data = "No data to display"
        return status_message, recent_data
    except Exception as e:
        return f"Error: {str(e)}", "No data to display"

# Create Gradio interface
def create_gradio_interface():
    """Create and configure Gradio interface"""
    with gr.Blocks(title="Salesforce Data Integration") as interface:
        gr.Markdown("# Salesforce Data Integration Tool")
        
        with gr.Tab("Upload Data"):
            with gr.Row():
                with gr.Column():
                    file_input = gr.File(
                        label="Upload File (CSV/JSON/XML)",
                        file_types=["csv", "json", "xml"]
                    )
                    upload_button = gr.Button("Upload to Salesforce", variant="primary")
                
                with gr.Column():
                    upload_output = gr.Textbox(
                        label="Upload Status",
                        lines=6,
                        max_lines=10
                    )
                    recent_data_output = gr.Textbox(
                        label="Recent Records",
                        lines=10,
                        max_lines=20
                    )
            
            upload_button.click(
                fn=upload_file_gradio,
                inputs=file_input,
                outputs=[upload_output, recent_data_output]
            )
        
        with gr.Tab("Retrieve Data"):
            with gr.Row():
                with gr.Column():
                    format_dropdown = gr.Dropdown(
                        choices=["csv", "json", "xml"],
                        value="csv",
                        label="Output Format"
                    )
                    retrieve_button = gr.Button("Retrieve from Salesforce", variant="primary")
                
                with gr.Column():
                    retrieve_output = gr.Textbox(
                        label="Retrieve Status",
                        lines=6,
                        max_lines=10
                    )
                    retrieve_recent_output = gr.Textbox(
                        label="Recent Records",
                        lines=10,
                        max_lines=20
                    )
            
            retrieve_button.click(
                fn=retrieve_data_gradio,
                inputs=format_dropdown,
                outputs=[retrieve_output, retrieve_recent_output]
            )
        
        gr.Markdown("""
        ### Instructions
        1. **Upload Data**: Upload a CSV, JSON, or XML file containing account records
        2. **Retrieve Data**: Download existing Salesforce account records in your preferred format
        3. **Recent Records**: View the most recent records processed
        
        ### Support
        For issues or questions, please check the logs or contact support.
        """)
    
    return interface

# Main execution
if __name__ == "__main__":
    # Create required directories
    os.makedirs("data", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    try:
        # Create and launch Gradio interface
        interface = create_gradio_interface()
        
        # Configure for Hugging Face Spaces
        interface.launch(
            server_name="0.0.0.0",
            server_port=7860,
            share=False,
            auth=None,  # Set authentication if needed
            ssl_verify=False,  # Adjust based on your needs
            debug=False
        )
        
    except Exception as e:
        logger.error(f"Application startup failed: {str(e)}")
        raise