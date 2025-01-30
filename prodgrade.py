import os
import pandas as pd
import json
import xml.etree.ElementTree as ET
import requests
import uvicorn
import random
import string
import time
import logging
import tempfile
import shutil
import threading
from typing import Dict, List, Tuple, Optional, Any, Union, Callable
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

from simple_salesforce import Salesforce, SalesforceAuthenticationFailed, SalesforceExpiredSession, SalesforceMalformedRequest
from fastapi import FastAPI, UploadFile, File, HTTPException, Response, Body, Depends, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator, ValidationError
from dotenv import load_dotenv

# --- Configuration and Constants ---

load_dotenv()

LOG_FILE = "app.log"
SALESFORCE_CLIENT_ID = os.getenv("SALESFORCE_CLIENT_ID")
SALESFORCE_CLIENT_SECRET = os.getenv("SALESFORCE_CLIENT_SECRET")
SALESFORCE_USERNAME = os.getenv("SALESFORCE_USERNAME")
SALESFORCE_PASSWORD = os.getenv("SALESFORCE_PASSWORD")
SALESFORCE_TOKEN_URL = os.getenv("SALESFORCE_TOKEN_URL")

BATCH_SIZE = 200
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2
MAX_RECENT_RECORDS = 50
MAX_WORKERS = 10
DATA_DIR = "data"
OUTPUT_DIR = "output"

# --- Logging Setup ---

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=LOG_FILE,
    filemode='a'
)
logger = logging.getLogger(__name__)

# --- Pydantic Models for Data Validation ---

class SalesforceAccount(BaseModel):
    """Pydantic model for Salesforce Account object with enhanced validation."""
    Id: Optional[str] = Field(None, description="Salesforce record ID")
    Name: str = Field(..., min_length=1, max_length=255, description="Account Name")
    AccountNumber: Optional[str] = Field(None, max_length=40, description="Account Number")
    Site: Optional[str] = Field(None, max_length=80, description="Account Site")
    Type: Optional[str] = Field(None, max_length=255, description="Account Type")
    Industry: Optional[str] = Field(None, max_length=255, description="Industry")
    AnnualRevenue: Optional[float] = Field(None, ge=0, description="Annual Revenue")
    Rating: Optional[str] = Field(None, max_length=255, description="Account Rating")
    Phone: Optional[str] = Field(None, max_length=40, description="Phone Number")
    Fax: Optional[str] = Field(None, max_length=40, description="Fax Number")
    Website: Optional[str] = Field(None, max_length=255, description="Website URL")
    TickerSymbol: Optional[str] = Field(None, max_length=20, description="Ticker Symbol")
    Ownership: Optional[str] = Field(None, max_length=255, description="Ownership Type")
    NumberOfEmployees: Optional[int] = Field(None, ge=0, description="Number of Employees")
    
    @validator('AnnualRevenue', 'NumberOfEmployees', pre=True)
    def clean_numeric_fields(cls, v):
        """Handle NaN values and type conversion for numeric fields."""
        if pd.isna(v):
            return None
        if isinstance(v, (int, float)):
            return int(v) if isinstance(v, float) and v.is_integer() else v
        try:
            return float(v) if '.' in str(v) else int(v)
        except (ValueError, TypeError):
            return None

    @validator('*', pre=True)
    def empty_string_to_none(cls, v):
        """Convert empty strings and whitespace-only strings to None."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @validator('Website', pre=True)
    def validate_website(cls, v):
        """Basic website URL validation."""
        if v is None or not isinstance(v, str):
            return v
        v = v.strip()
        if not v:
            return None
        if not v.startswith(('http://', 'https://')):
            v = 'https://' + v
        return v

    @validator('Phone', 'Fax', pre=True)
    def validate_phone(cls, v):
        """Basic phone/fax number validation."""
        if v is None or not isinstance(v, str):
            return v
        v = ''.join(c for c in v if c.isdigit() or c in '+-().')
        return v if v else None

class SalesforceError(Exception):
    """Custom exception for Salesforce-related errors with error details."""
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

class FailedRecord(BaseModel):
    """Model to store information about failed records with enhanced error tracking."""
    record: Dict[str, Any]
    error: str
    error_code: Optional[str] = None
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    retry_count: int = Field(default=0)

    def to_dict(self) -> Dict[str, Any]:
        return self.dict(exclude_none=True)

    @validator('error')
    def validate_error(cls, v):
        """Ensure error message is not empty."""
        if not v.strip():
            raise ValueError("Error message cannot be empty")
        return v.strip()

# --- Data Management ---

class RecentDataManager:
    """Thread-safe manager for recently processed records with enhanced functionality."""
    def __init__(self, max_records: int = MAX_RECENT_RECORDS):
        self.max_records = max_records
        self.recent_records: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def add_records(self, records: List[Dict[str, Any]], operation_type: str = "insert"):
        """Adds records to the cache with metadata."""
        if not records:
            return

        with self._lock:
            current_time = datetime.now(timezone.utc)
            new_records = []
            for record in records:
                record_copy = record.copy()
                record_copy.update({
                    'operation_type': operation_type,
                    'timestamp': current_time.isoformat(),
                    'processed_at': current_time
                })
                new_records.append(record_copy)
            
            self.recent_records = new_records + self.recent_records
            self.recent_records = self.recent_records[:self.max_records]

    def get_records(self, operation_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieves cached records with optional filtering."""
        with self._lock:
            if operation_type is None:
                return self.recent_records.copy()
            return [r.copy() for r in self.recent_records if r.get('operation_type') == operation_type]

    def clear_records(self):
        """Clears all cached records."""
        with self._lock:
            self.recent_records.clear()

recent_data_manager = RecentDataManager()

# --- File Handling ---

def _read_csv(file_path: str) -> List[Dict[str, Any]]:
    """Reads and parses a CSV file with enhanced error handling."""
    try:
        df = pd.read_csv(
            file_path,
            dtype=str,  # Read all columns as strings initially
            na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', 
                      '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a', 'null'],
            keep_default_na=True
        )
        df.columns = df.columns.str.strip()
        
        # Convert numeric columns appropriately
        numeric_columns = ['AnnualRevenue', 'NumberOfEmployees']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.to_dict('records')
    except pd.errors.EmptyDataError:
        logger.error(f"Empty CSV file: {file_path}")
        raise ValueError("The CSV file is empty")
    except pd.errors.ParserError as pe:
        logger.error(f"Error parsing CSV file {file_path}: {pe}")
        raise ValueError(f"Invalid CSV format: {pe}")
    except Exception as e:
        logger.error(f"Unexpected error reading CSV file {file_path}: {e}")
        raise

def _read_json(file_path: str) -> List[Dict[str, Any]]:
    """Reads and parses a JSON file with enhanced validation."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get('records', [data])
        else:
            raise ValueError("Invalid JSON structure: must be an object or array")

        if not records:
            raise ValueError("No records found in JSON file")

        # Validate each record has required fields
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("Each record must be an object")
            if 'Name' not in record:
                raise ValueError("Each record must contain a 'Name' field")

        return records
    except json.JSONDecodeError as je:
        logger.error(f"Error decoding JSON in file {file_path}: {je}")
        raise ValueError(f"Invalid JSON format: {je}")
    except Exception as e:
        logger.error(f"Unexpected error reading JSON file {file_path}: {e}")
        raise

def _read_xml(file_path: str) -> List[Dict[str, Any]]:
    """Reads and parses an XML file with enhanced validation and error handling."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        records = []
        for record in root.findall('.//record'):
            record_dict = {}
            for field in record:
                # Handle empty elements
                value = field.text.strip() if field.text else None
                
                # Convert numeric values
                if field.tag in ['AnnualRevenue', 'NumberOfEmployees']:
                    try:
                        value = float(value) if value and '.' in value else \
                               int(value) if value else None
                    except (ValueError, TypeError):
                        value = None
                
                record_dict[field.tag] = value
            
            # Ensure required fields
            if 'Name' not in record_dict or not record_dict['Name']:
                logger.warning("Found record without required 'Name' field")
                continue
                
            records.append(record_dict)
            
        if not records:
            raise ValueError("No valid records found in XML file")
            
        return records
    except ET.ParseError as pe:
        logger.error(f"Error parsing XML in file {file_path}: {pe}")
        raise ValueError(f"Invalid XML format: {pe}")
    except Exception as e:
        logger.error(f"Unexpected error reading XML file {file_path}: {e}")
        raise

def read_file_data(file_path: str) -> List[Dict[str, Any]]:
    """Reads data from a file with comprehensive validation."""
    logger.info(f"Reading data from file: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
        
    if os.path.getsize(file_path) == 0:
        raise ValueError("File is empty")
        
    file_ext = os.path.splitext(file_path)[1].lower()
    readers = {
        '.csv': _read_csv,
        '.json': _read_json,
        '.xml': _read_xml
    }
    
    reader = readers.get(file_ext)
    if not reader:
        raise ValueError(f"Unsupported file format: {file_ext}")
        
    try:
        records = reader(file_path)
        logger.info(f"Successfully read {len(records)} records from {file_path}")
        return records
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise

def _write_csv(data: List[Dict[str, Any]], file_path: str):
    """Writes data to a CSV file with proper encoding and handling."""
    try:
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False, encoding='utf-8-sig',
                 date_format='%Y-%m-%dT%H:%M:%S%z')
    except Exception as e:
        logger.error(f"Error writing CSV file {file_path}: {e}")
        raise

def _write_json(data: List[Dict[str, Any]], file_path: str):
    """Writes data to a JSON file with proper formatting and encoding."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({'records': data}, f, indent=2, ensure_ascii=False,
                     default=str)  # Handle datetime objects
    except Exception as e:
        logger.error(f"Error writing JSON file {file_path}: {e}")
        raise

def _write_xml(data: List[Dict[str, Any]], file_path: str):
    """Writes data to an XML file with proper formatting and encoding."""
    try:
        root = ET.Element('records')
        for item in data:
            record = ET.SubElement(root, 'record')
            for key, value in item.items():
                if value is not None:  # Skip None values
                    field = ET.SubElement(record, str(key))
                    field.text = str(value)

        def indent_xml(elem: ET.Element, level: int = 0):
            """Adds proper indentation to XML elements."""
            i = "\n" + level * "  "
            if len(elem):
                if not elem.text or not elem.text.strip():
                    elem.text = i + "  "
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
                for subelem in elem:
                    indent_xml(subelem, level + 1)
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
            else:
                if level and (not elem.tail or not elem.tail.strip()):
                    elem.tail = i

        indent_xml(root)
        tree = ET.ElementTree(root)
        
        # Write with proper XML declaration and encoding
        with open(file_path, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            tree.write(f, encoding='utf-8', xml_declaration=False)
    except Exception as e:
        logger.error(f"Error writing XML