import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
import os

# To allow tests to run from the root directory and import src modules
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from salesforce_custom_object_metric import app

@pytest.fixture
def client():
    return TestClient(app)

@patch('psutil.cpu_percent')
@patch('psutil.virtual_memory')
@patch('psutil.disk_usage')
@patch('psutil.net_io_counters')
@patch('psutil.boot_time')
@patch('psutil.getloadavg')
@patch('psutil.cpu_freq')
@patch('platform.platform')
@patch('platform.python_version')
def test_get_detailed_metrics(
    mock_python_version, mock_platform, mock_cpu_freq, mock_loadavg, mock_boot_time,
    mock_net, mock_disk, mock_mem, mock_cpu
):
    # Mock the return values of the psutil and platform calls
    mock_cpu.return_value = 10.0
    mock_mem.return_value.percent = 50.0
    mock_disk.return_value.percent = 60.0
    mock_net.return_value.bytes_sent = 100
    mock_net.return_value.bytes_recv = 200
    mock_boot_time.return_value = 1678886400
    mock_loadavg.return_value = (0.1, 0.2, 0.3)
    mock_cpu_freq.return_value.current = 2.5
    mock_platform.return_value = "Linux"
    mock_python_version.return_value = "3.11"

    client = TestClient(app)
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert data['cpu_usage_percent'] == 10.0
    assert data['memory_usage_percent'] == 50.0
    assert data['disk_usage_percent'] == 60.0
    assert data['network_bytes_sent'] == 100
    assert data['network_bytes_recv'] == 200
    assert data['system_load_avg'] == [0.1, 0.2, 0.3]
    assert data['operating_system'] == 'Linux'
    assert data['python_version'] == '3.11'
