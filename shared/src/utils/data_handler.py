# src/utils/data_handler.py
import csv
import json
import xml.etree.ElementTree as ET
from io import StringIO, BytesIO
import logging
from typing import List, Dict, Any, Optional

from fastapi import UploadFile, HTTPException, status
from core.config import settings

logger = logging.getLogger(settings.APP_NAME)

class FileParsingError(Exception):
    """Custom exception for file parsing errors."""
    pass

async def read_file_data_for_bulk(file: UploadFile) -> List[Dict[str, Any]]:
    """
    Reads data from an uploaded file (CSV, JSON) and returns a list of records (dictionaries).
    Designed for preparing data for Salesforce Bulk API operations.
    """
    filename = file.filename
    content = await file.read()
    await file.close() # Ensure file is closed after reading content

    records: List[Dict[str, Any]] = []

    try:
        if filename.endswith('.csv'):
            # Decode bytes to string for CSV parsing
            try:
                decoded_content = content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    decoded_content = content.decode('latin-1') # Try common alternative
                except UnicodeDecodeError as ude:
                    logger.error(f"Failed to decode CSV file {filename} with utf-8 and latin-1: {ude}")
                    raise FileParsingError(f"Unsupported file encoding for CSV: {filename}. Please use UTF-8 or Latin-1.")

            csv_file = StringIO(decoded_content)
            reader = csv.DictReader(csv_file)
            for row in reader:
                # Clean empty strings to None for better Salesforce processing,
                # especially for number/date fields.
                cleaned_row = {k: (v if v != "" else None) for k, v in row.items()}
                records.append(cleaned_row)
            logger.info(f"Successfully parsed {len(records)} records from CSV file: {filename}")

        elif filename.endswith('.json'):
            try:
                decoded_content = content.decode('utf-8')
            except UnicodeDecodeError as ude:
                logger.error(f"Failed to decode JSON file {filename} with utf-8: {ude}")
                raise FileParsingError(f"Unsupported file encoding for JSON: {filename}. Please use UTF-8.")

            data = json.loads(decoded_content)
            if isinstance(data, list): # Expecting a list of records
                records = data
            elif isinstance(data, dict) and 'records' in data and isinstance(data['records'], list): # Common wrapper
                records = data['records']
            else:
                logger.error(f"JSON file {filename} does not contain a list of records at the root or under a 'records' key.")
                raise FileParsingError("Invalid JSON structure: Expected a list of records or a dictionary with a 'records' key containing a list.")
            logger.info(f"Successfully parsed {len(records)} records from JSON file: {filename}")

        elif filename.endswith('.xml'):
            try:
                decoded_content = content.decode('utf-8')
            except UnicodeDecodeError as ude:
                logger.error(f"Failed to decode XML file {filename} with utf-8: {ude}")
                raise FileParsingError(f"Unsupported file encoding for XML: {filename}. Please use UTF-8.")
            try:
                tree = ET.fromstring(decoded_content)
                # Expecting <records><record>...</record>...</records>
                # Or simply a root element containing <record> elements directly
                if tree.tag != 'records' and not tree.findall('./record'):
                     logger.warning(f"XML file {filename} does not have a root <records> tag nor direct <record> children. Trying to find <record> tags anywhere.") # Allow flexibility

                for record_node in tree.findall('.//record'): # Find all 'record' elements anywhere under the root
                    record_dict = {child.tag: child.text for child in record_node}
                    if record_dict: # Ensure the record_dict is not empty
                        records.append(record_dict)
                logger.info(f"Successfully parsed {len(records)} records from XML file: {filename}")
            except ET.ParseError as etpe:
                logger.error(f"XML parsing error for file {filename}: {str(etpe)}")
                raise FileParsingError(f"Invalid XML format in {filename}: {str(etpe)}")

        else:
            logger.error(f"Unsupported file type for bulk processing: {filename}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {filename}. Please use CSV, JSON, or XML."
            )

        if not records:
            logger.warning(f"No records found or parsed from file: {filename}")
            # Depending on requirements, this could be an error or just an empty operation.
            # For bulk, usually an empty file is not an error itself but leads to no operation.

        return records

    except HTTPException as he:
        raise he
    except FileParsingError as fpe:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(fpe))
    except json.JSONDecodeError as jde:
        logger.error(f"JSON decoding error for file {filename}: {jde.msg} at line {jde.lineno} col {jde.colno}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid JSON format: {jde.msg}")
    except ET.ParseError as etpe:
        logger.error(f"XML parsing error for file {filename} (direct catch): {str(etpe)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid XML format: {str(etpe)}")
    except csv.Error as csve:
        logger.error(f"CSV parsing error for file {filename}: {csve}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid CSV format: {str(csve)}")
    except Exception as e:
        logger.error(f"Unexpected error processing file {filename}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not process file {filename}: {str(e)}"
        )


def convert_records_to_csv_string(records: List[Dict[str, Any]], field_order: Optional[List[str]] = None) -> str:
    """
    Converts a list of record dictionaries into a CSV formatted string.
    If field_order is provided, it dictates the column order in the CSV.
    Otherwise, field order is taken from the keys of the first record.
    """
    if not records:
        return ""

    output = StringIO()

    # Determine fieldnames: use provided order, or keys from first record, or an empty list
    if field_order:
        fieldnames = field_order
    elif records:
        fieldnames = list(records[0].keys())
    else: # Should not happen if records is not empty, but as a fallback
        fieldnames = []

    if not fieldnames: # If records were empty or first record had no keys
        return ""

    # Use LF line terminator as recommended for Salesforce Bulk API
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator='\n', extrasaction='ignore')
    writer.writeheader()
    writer.writerows(records)

    return output.getvalue()


def parse_csv_string_to_records(csv_string: str) -> List[Dict[str, Any]]:
    """
    Parses a CSV formatted string into a list of record dictionaries.
    """
    if not csv_string.strip():
        return []

    reader = csv.DictReader(StringIO(csv_string))
    records = [dict(row) for row in reader]
    return records

# Potential future functions:
# - save_data_to_file (generic CSV/JSON/XML writer)
# - read_data_from_local_file (for paths specified in payloads like the original request)
# - functions to handle specific XML structures if needed.

async def read_data_from_local_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Reads data from a local file path (CSV, JSON, or XML).
    Used when file_path is provided in the payload.
    """
    records: List[Dict[str, Any]] = []
    logger.info(f"Reading data from local file: {file_path}")

    try:
        with open(file_path, 'rb') as f_bytes: # Read as bytes first for robust decoding
            content_bytes = f_bytes.read()

        if file_path.endswith('.csv'):
            try:
                decoded_content = content_bytes.decode('utf-8-sig') # Handle BOM
            except UnicodeDecodeError:
                try:
                    decoded_content = content_bytes.decode('latin-1')
                except UnicodeDecodeError as ude:
                    logger.error(f"Failed to decode CSV file {file_path} with utf-8-sig and latin-1: {ude}")
                    raise FileParsingError(f"Unsupported file encoding for CSV: {file_path}. Please use UTF-8 or Latin-1.")

            csv_file = StringIO(decoded_content)
            reader = csv.DictReader(csv_file)
            for row in reader:
                cleaned_row = {k: (v if v != "" else None) for k, v in row.items()}
                records.append(cleaned_row)

        elif file_path.endswith('.json'):
            try:
                decoded_content = content_bytes.decode('utf-8')
            except UnicodeDecodeError as ude:
                logger.error(f"Failed to decode JSON file {file_path}: {ude}")
                raise FileParsingError(f"Unsupported file encoding for JSON: {file_path}. Please use UTF-8.")

            data = json.loads(decoded_content)
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and 'records' in data and isinstance(data['records'], list):
                records = data['records']
            else:
                raise FileParsingError("Invalid JSON structure in local file.")

        elif file_path.endswith('.xml'):
            try:
                decoded_content = content_bytes.decode('utf-8')
            except UnicodeDecodeError as ude:
                logger.error(f"Failed to decode XML file {file_path}: {ude}")
                raise FileParsingError(f"Unsupported file encoding for XML: {file_path}. Please use UTF-8.")

            try:
                root = ET.fromstring(decoded_content)
                # Similar logic to read_file_data_for_bulk for flexibility
                if root.tag != 'records' and not root.findall('./record'):
                    logger.warning(f"XML file {file_path} does not have a root <records> tag nor direct <record> children. Trying to find <record> tags anywhere.")

                for record_node in root.findall('.//record'): # Find all 'record' elements
                    record_dict = {child.tag: child.text for child in record_node}
                    if record_dict:
                        records.append(record_dict)
                # The original warning about structure or empty is still relevant if records list is empty
                if not records:
                     logger.warning(f"No records parsed from XML file {file_path}. It might be empty or not contain <record> elements in the expected structure.")

            except ET.ParseError as etpe:
                logger.error(f"XML parsing error for local file {file_path}: {str(etpe)}")
                # This will be caught by the generic ET.ParseError handler below,
                # but raising FileParsingError here would also work and be caught by its handler.
                # For consistency with how JSON/CSV decoding errors are handled (raising FileParsingError first),
                # it might be slightly better to do:
                # raise FileParsingError(f"Invalid XML format in local file {file_path}: {str(etpe)}")
                # However, the current structure will still result in the correct HTTPException.
                # Let's keep the direct ET.ParseError catch below, which is more specific.
                raise # Re-raise to be caught by the specific ET.ParseError handler

        else:
            raise FileParsingError(f"Unsupported local file type: {file_path}. Only CSV, JSON, or XML supported.")

        logger.info(f"Successfully parsed {len(records)} records from local file: {file_path}")
        return records

    except FileNotFoundError:
        logger.error(f"Local file not found: {file_path}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Local file not found: {file_path}")
    except FileParsingError as fpe:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(fpe))
    except json.JSONDecodeError as jde:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid JSON in local file {file_path}: {jde.msg}")
    except csv.Error as csve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid CSV in local file {file_path}: {str(csve)}")
    except ET.ParseError as etpe:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid XML in local file {file_path}: {str(etpe)}")
    except Exception as e:
        logger.error(f"Error reading local file {file_path}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Could not process local file {file_path}.")
