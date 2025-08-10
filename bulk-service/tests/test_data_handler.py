import pytest
import csv
import json
import os
from io import BytesIO
from fastapi import UploadFile, HTTPException

from utils.data_handler import (
    read_file_data_for_bulk,
    read_data_from_local_file,
    convert_records_to_csv_string,
    parse_csv_string_to_records,
    FileParsingError
)
from core.config import settings

# Mark all tests in this module as asyncio if they involve async functions
# For data_handler, most functions are synchronous, but read_file_data_for_bulk is async
pytestmark = pytest.mark.asyncio


# --- Tests for read_file_data_for_bulk ---

async def test_read_file_data_for_bulk_csv_success():
    csv_content = "Name,Email\nTest User,test@example.com\nAnother User,another@example.com"
    file_bytes = BytesIO(csv_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.csv", file=file_bytes)

    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 2
    assert records[0] == {"Name": "Test User", "Email": "test@example.com"}
    assert records[1] == {"Name": "Another User", "Email": "another@example.com"}

async def test_read_file_data_for_bulk_csv_empty_strings_to_none():
    csv_content = "Name,Value\nTest1,123\nTest2,"
    file_bytes = BytesIO(csv_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.csv", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 2
    assert records[1] == {"Name": "Test2", "Value": None}


async def test_read_file_data_for_bulk_json_list_success():
    json_content = '[{"Name": "Test User", "Age": 30}, {"Name": "Jane Doe", "Age": 25}]'
    file_bytes = BytesIO(json_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.json", file=file_bytes)

    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 2
    assert records[0] == {"Name": "Test User", "Age": 30}

async def test_read_file_data_for_bulk_json_dict_success():
    json_content = '{"records": [{"Name": "Test User"}, {"Name": "Jane Doe"}]}'
    file_bytes = BytesIO(json_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.json", file=file_bytes)

    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 2
    assert records[1] == {"Name": "Jane Doe"}

async def test_read_file_data_for_bulk_unsupported_type():
    file_bytes = BytesIO(b"some content")
    upload_file = UploadFile(filename="test.txt", file=file_bytes)
    with pytest.raises(HTTPException) as exc_info:
        await read_file_data_for_bulk(upload_file)
    assert exc_info.value.status_code == 400
    assert "Unsupported file type" in exc_info.value.detail
    assert "CSV, JSON, or XML" in exc_info.value.detail

async def test_read_file_data_for_bulk_malformed_json():
    json_content = '[{"Name": "Test User", "Age": 30}, {"Name": "Jane Doe", "Age": 25' # Malformed
    file_bytes = BytesIO(json_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.json", file=file_bytes)
    with pytest.raises(HTTPException) as exc_info:
        await read_file_data_for_bulk(upload_file)
    assert exc_info.value.status_code == 400
    assert "Invalid JSON format" in exc_info.value.detail

async def test_read_file_data_for_bulk_empty_csv():
    csv_content = "Name,Email\n" # Only header
    file_bytes = BytesIO(csv_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.csv", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 0

async def test_read_file_data_for_bulk_empty_json_list():
    json_content = '[]'
    file_bytes = BytesIO(json_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.json", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 0

async def test_read_file_data_for_bulk_xml_success_records_root():
    xml_content = "<records><record><Name>Test User</Name><ID>1</ID></record><record><Name>Another</Name><ID>2</ID></record></records>"
    file_bytes = BytesIO(xml_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.xml", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 2
    assert records[0] == {"Name": "Test User", "ID": "1"}
    assert records[1] == {"Name": "Another", "ID": "2"}

async def test_read_file_data_for_bulk_xml_success_custom_root():
    xml_content = "<data><item><Name>Test User</Name></item></data>".replace("item>", "record>") # use record tag
    file_bytes = BytesIO(xml_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.xml", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 1
    assert records[0] == {"Name": "Test User"}

async def test_read_file_data_for_bulk_xml_malformed():
    xml_content = "<records><record><Name>Test User</Name><ID>1</ID></record><record><Name>Another</Name><ID>2</ID></records" # Malformed
    file_bytes = BytesIO(xml_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.xml", file=file_bytes)
    with pytest.raises(HTTPException) as exc_info:
        await read_file_data_for_bulk(upload_file)
    assert exc_info.value.status_code == 400
    assert "Invalid XML format" in exc_info.value.detail

async def test_read_file_data_for_bulk_xml_empty_records_tag():
    xml_content = "<records></records>"
    file_bytes = BytesIO(xml_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.xml", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 0

async def test_read_file_data_for_bulk_xml_no_record_elements():
    xml_content = "<data><item><Name>Test User</Name></item></data>" # No 'record' elements
    file_bytes = BytesIO(xml_content.encode('utf-8'))
    upload_file = UploadFile(filename="test.xml", file=file_bytes)
    records = await read_file_data_for_bulk(upload_file)
    assert len(records) == 0

async def test_read_file_data_for_bulk_xml_unsupported_encoding():
    xml_content = "<records><record><Name>Test User</Name></record></records>"
    file_bytes = BytesIO(xml_content.encode('latin-1')) # Encode with latin-1
    upload_file = UploadFile(filename="test.xml", file=file_bytes, headers={"content-type": "application/xml; charset=latin-1"}) # Specify charset
    # Temporarily modify the function to simulate it trying to decode with only utf-8 for this test case, or expect FileParsingError
    # The function is now hardcoded to try utf-8 first for XML.
    # If content is not utf-8, it should raise FileParsingError.
    with pytest.raises(HTTPException) as exc_info:
         # Create a new UploadFile with latin-1 encoded content
        latin1_content = "<récords><récord><Náme>Tést Üser</Náme></récord></récords>".encode('latin-1')
        file_bytes_latin1 = BytesIO(latin1_content)
        upload_file_latin1 = UploadFile(filename="test_latin1.xml", file=file_bytes_latin1)
        await read_file_data_for_bulk(upload_file_latin1) # Function expects utf-8

    assert exc_info.value.status_code == 400
    assert "Unsupported file encoding for XML" in exc_info.value.detail


# --- Tests for read_data_from_local_file ---
# These require creating temporary files

@pytest.fixture
def temp_csv_file(tmp_path):
    file_path = tmp_path / "temp.csv"
    content = "Header1,Header2\nVal1,Val2\nVal3,"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    return str(file_path)

@pytest.fixture
def temp_json_file(tmp_path):
    file_path = tmp_path / "temp.json"
    data = [{"id": 1, "data": "test"}, {"id": 2, "data": None}]
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return str(file_path)

@pytest.fixture
def temp_xml_file(tmp_path): # Basic XML for testing structure
    file_path = tmp_path / "temp.xml"
    content = "<records><record><fieldA>A1</fieldA><fieldB>B1</fieldB></record><record><fieldA>A2</fieldA><fieldB>B2</fieldB></record></records>"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    return str(file_path)

@pytest.fixture
def temp_malformed_xml_file(tmp_path):
    file_path = tmp_path / "malformed.xml"
    content = "<records><record><fieldA>A1</fieldA><fieldB>B1</record></records>" # Missing closing fieldB tag
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    return str(file_path)

@pytest.fixture
def temp_empty_xml_file(tmp_path):
    file_path = tmp_path / "empty.xml"
    content = "<records></records>"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    return str(file_path)

@pytest.fixture
def temp_no_records_xml_file(tmp_path):
    file_path = tmp_path / "no_records.xml"
    content = "<data><item>A1</item></data>" # No 'record' elements
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    return str(file_path)


async def test_read_data_from_local_file_csv(temp_csv_file):
    records = await read_data_from_local_file(temp_csv_file)
    assert len(records) == 2
    assert records[0] == {"Header1": "Val1", "Header2": "Val2"}
    assert records[1] == {"Header1": "Val3", "Header2": None} # Empty string to None

async def test_read_data_from_local_file_json(temp_json_file):
    records = await read_data_from_local_file(temp_json_file)
    assert len(records) == 2
    assert records[0] == {"id": 1, "data": "test"}

async def test_read_data_from_local_file_xml(temp_xml_file):
    records = await read_data_from_local_file(temp_xml_file)
    assert len(records) == 2
    assert records[0] == {"fieldA": "A1", "fieldB": "B1"}
    assert records[1] == {"fieldA": "A2", "fieldB": "B2"}

async def test_read_data_from_local_file_xml_malformed(temp_malformed_xml_file):
    with pytest.raises(HTTPException) as exc_info:
        await read_data_from_local_file(temp_malformed_xml_file)
    assert exc_info.value.status_code == 400
    assert "Invalid XML in local file" in exc_info.value.detail

async def test_read_data_from_local_file_xml_empty(temp_empty_xml_file):
    records = await read_data_from_local_file(temp_empty_xml_file)
    assert len(records) == 0

async def test_read_data_from_local_file_xml_no_records(temp_no_records_xml_file):
    records = await read_data_from_local_file(temp_no_records_xml_file)
    assert len(records) == 0

async def test_read_data_from_local_file_xml_custom_root_finds_records(tmp_path):
    file_path = tmp_path / "custom_root.xml"
    # Root is <myData>, but contains <record> tags
    content = "<myData><record><fieldX>X1</fieldX></record></myData>"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    records = await read_data_from_local_file(str(file_path))
    assert len(records) == 1
    assert records[0] == {"fieldX": "X1"}


async def test_read_data_from_local_file_not_found():
    with pytest.raises(HTTPException) as exc_info:
        await read_data_from_local_file("non_existent_file.csv")
    assert exc_info.value.status_code == 404

async def test_read_data_from_local_file_unsupported(tmp_path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("hello")
    with pytest.raises(HTTPException) as exc_info:
        await read_data_from_local_file(str(file_path))
    assert exc_info.value.status_code == 400
    assert "Unsupported local file type" in exc_info.value.detail
    assert "CSV, JSON, or XML" in exc_info.value.detail


# --- Tests for convert_records_to_csv_string ---

def test_convert_records_to_csv_string_basic():
    records = [{"Name": "Alice", "Age": "30"}, {"Name": "Bob", "Age": "24"}]
    # Order of keys from first record if field_order is None
    expected_csv_string = "Name,Age\nAlice,30\nBob,24\n"
    csv_string = convert_records_to_csv_string(records)
    assert csv_string == expected_csv_string

def test_convert_records_to_csv_string_with_field_order():
    records = [{"Name": "Alice", "Age": "30", "City": "NY"}, {"Name": "Bob", "Age": "24", "City": "LA"}]
    field_order = ["Name", "City", "Age"] # Different order
    expected_csv_string = "Name,City,Age\nAlice,NY,30\nBob,LA,24\n"
    csv_string = convert_records_to_csv_string(records, field_order=field_order)
    assert csv_string == expected_csv_string

def test_convert_records_to_csv_string_empty_records():
    assert convert_records_to_csv_string([]) == ""

def test_convert_records_to_csv_string_missing_keys_with_ignore():
    # convert_records_to_csv_string uses DictWriter default (extrasaction='raise')
    # If we wanted to test 'ignore', the function would need to support it.
    # Current behavior: if a record misses a field in fieldnames, it writes empty.
    # If a record has extra fields not in fieldnames, they are ignored.
    records = [{"Name": "Alice", "Age": "30"}, {"Name": "Bob"}] # Bob is missing Age
    field_order = ["Name", "Age"]
    expected_csv_string = "Name,Age\nAlice,30\nBob,\n" # Empty for Bob's age
    csv_string = convert_records_to_csv_string(records, field_order=field_order)
    assert csv_string == expected_csv_string

    records_extra = [{"Name": "Alice", "Age": "30", "Extra": "E1"}, {"Name": "Bob", "Age": "24"}]
    expected_csv_string_extra = "Name,Age\nAlice,30\nBob,24\n" # Extra field ignored
    csv_string_extra = convert_records_to_csv_string(records_extra, field_order=field_order)
    assert csv_string_extra == expected_csv_string_extra


# --- Tests for parse_csv_string_to_records ---

def test_parse_csv_string_to_records_basic():
    csv_string = "Header1,Header2\nVal1,Val2\nVal3,Val4"
    records = parse_csv_string_to_records(csv_string)
    assert len(records) == 2
    assert records[0] == {"Header1": "Val1", "Header2": "Val2"}

def test_parse_csv_string_to_records_empty_string():
    assert parse_csv_string_to_records("") == []
    assert parse_csv_string_to_records("  \n  ") == []


def test_parse_csv_string_to_records_only_header():
    csv_string = "Id,Name,Value\n"
    records = parse_csv_string_to_records(csv_string)
    assert len(records) == 0

def test_parse_csv_string_to_records_empty_values():
    csv_string = "Id,Name\n1,\n,NoName"
    records = parse_csv_string_to_records(csv_string)
    assert len(records) == 2
    assert records[0] == {"Id": "1", "Name": ""} # CSV empty values are read as empty strings
    assert records[1] == {"Id": "", "Name": "NoName"}
