# Salesforce Integration API

Production-grade API for dynamic Salesforce object integration, supporting generic CRUD operations (Create, Retrieve, Update, Delete, Upsert) and Bulk API 2.0 functionalities for any standard or custom Salesforce SObject.

## Features

- **Dynamic SObject Handling**: Perform operations on any Salesforce object by specifying its API name.
- **Standard REST API Operations for Single Records**:
    - Create new records.
    - Retrieve records by ID, with optional field selection.
    - Update existing records by ID.
    - Delete records by ID.
    - Upsert records based on an external ID.
    - Describe SObject metadata.
- **Batch REST API Operations**: Process a list of records from a direct payload using iterative REST API calls via the `/records/batch` endpoint.
- **Bulk API 2.0 Support**:
    - Submit DML jobs: `insert`, `update`, `upsert`, `delete`, `hardDelete`.
    - Process data from uploaded files (CSV, JSON).
    - Process data from local server file paths (as specified in payload).
    - Process data from a direct list of records in the payload.
    - Submit asynchronous SOQL query jobs (`query`, `queryAll`).
    - Endpoints to check job status and retrieve results (successful, failed, unprocessed records as CSV).
- **Flexible Processing Control**: Use the `use_bulk_api` flag for file-based and direct batch processing to choose between Bulk API and iterative REST API calls.
- **Salesforce Authentication**: Secure OAuth 2.0 Password Flow with automatic token management (caching, refresh).
- **Configuration**: Environment variable-driven configuration for Salesforce credentials and application settings.
- **Logging**: Comprehensive request and application logging with rotating file handler.
- **Error Handling**: Consistent error responses.
- **Asynchronous**: Built with FastAPI and `httpx` for non-blocking I/O.
- **Dockerized**: Comes with `Dockerfile` and `docker-compose.yml` for easy setup and deployment.
- **Kubernetes Ready**: Includes example Kubernetes manifests for deployment (`Deployment`, `Service`, `HPA`, `ConfigMap`, `PV`, `PVC`, `Secrets example`).
- **CI/CD Pipeline**: Basic GitHub Actions workflow for linting, testing, and Docker image building.

## Project Structure

```
.
├── .github/workflows/        # CI/CD pipeline (ci.yml)
├── kubernetes/               # Kubernetes manifests
│   ├── configmap.yaml
│   ├── deployment.yaml
│   ├── hpa.yaml
│   ├── pv.yaml
│   ├── pvc.yaml
│   ├── secrets.yaml.example
│   └── service.yaml
├── src/
│   ├── app/                  # FastAPI application, routers (sfdc.py), main entrypoint (main.py)
│   ├── core/                 # Core logic: config.py, schemas.py, models.py
│   ├── salesforce/           # Salesforce specific logic: auth.py, client.py, operations.py
│   ├── utils/                # Utility functions: data_handler.py, logger.py
│   └── tests/                # Unit and integration tests (conftest.py, test_*.py)
├── .env.example              # Example environment variables
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── README.md
└── requirements.txt
```

## Prerequisites

- Python 3.10+ (as per `Dockerfile`, `python:3.11-slim`)
- Docker & Docker Compose (for containerized setup)
- Salesforce Connected App credentials (Client ID, Client Secret)
- Salesforce user credentials (Username, Password + Security Token)

## Setup and Running Locally

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd <repository_name>
    ```

2.  **Create and configure environment file:**
    Copy `.env.example` to `.env` and fill in your Salesforce Connected App and user credentials:
    ```bash
    cp .env.example .env
    # Edit .env with your details
    ```
    Key variables to set:
    - `SALESFORCE_CLIENT_ID`
    - `SALESFORCE_CLIENT_SECRET`
    - `SALESFORCE_USERNAME`
    - `SALESFORCE_PASSWORD` (append security token if needed, e.g., `mypasswordMYTOKEN`)
    - `SALESFORCE_TOKEN_URL` (if using a My Domain or different login server, e.g., `https://yourdomain.my.salesforce.com/services/oauth2/token`)

3.  **Build and run with Docker Compose:**
    This is the recommended way to run for development and testing.
    ```bash
    # (Optional) Create local directories for data volume mounts if specified in docker-compose.yml
    # mkdir -p local_data/input local_data/output local_data/failed logs

    docker-compose up --build
    ```
    The API will be available at `http://localhost:8000`.
    Interactive API documentation (Swagger UI) at `http://localhost:8000/docs`.
    Alternative API documentation (ReDoc) at `http://localhost:8000/redoc`.

4.  **Running without Docker (Virtual Environment - for development):**
    *   Create a virtual environment:
        ```bash
        python -m venv venv
        source venv/bin/activate  # On Windows: venv\Scripts\activate
        ```
    *   Install dependencies:
        ```bash
        pip install -r requirements.txt
        ```
    *   Run the FastAPI application:
        ```bash
        uvicorn src.app.main:app --host 0.0.0.0 --port 8000 --reload
        ```

## API Endpoints

Base URL: `http://localhost:8000/api/v1` (or as configured by `API_V1_STR`)

Refer to the OpenAPI documentation at `/docs` for detailed information on request/response schemas and available endpoints. Key endpoints include:

**Single Record REST API Operations:**
*   `POST /records/create`: Create a single record.
    *   Payload: `{ "object_name": "Account", "data": {"Name": "New Account"} }`
*   `GET /records/{object_name}/{record_id}`: Retrieve a record by its Salesforce ID.
    *   Optional query param: `fields=Name,Industry`
*   `PATCH /records/update`: Update a single record by its Salesforce ID.
    *   Payload: `{ "object_name": "Account", "record_id": "001...", "data": {"Industry": "Technology"} }`
*   `POST /records/upsert`: Upsert a single record using an external ID.
    *   Payload: `{ "object_name": "Contact", "external_id_field": "LegacyID__c", "record_id": "legacy-contact-123", "data": {"LastName": "Smith", "Email": "smith@example.com"} }` (Note: `record_id` here holds the external ID value).
*   `DELETE /records/{object_name}/{record_id}`: Delete a record by its Salesforce ID.
*   `GET /sobjects/{object_name}/describe`: Get SObject metadata.

**Batch & Bulk Operations:**
*   `POST /records/batch`: Process a list of records provided directly in the payload.
    *   Payload: `{ "object_name": "Lead", "operation_type": "create", "records": [{"Company": "Lead1"}, {"Company": "Lead2"}], "use_bulk_api": false, "external_id_field": "optional_for_upsert" }`
    *   Switches between iterative REST calls or a Bulk API job based on `use_bulk_api` flag.
*   `POST /bulk/dml-file-upload?object_name=...&operation_type=...&external_id_field=...`: Submit a Bulk API DML job using an uploaded file (CSV/JSON).
*   `POST /bulk/local-file-process`: Process records from a local server file path (specified in payload), using Bulk API or iterative REST based on `use_bulk_api` flag.
    *   Payload: `{ "object_name": "Account", "use_bulk_api": true, "file_path": "/mnt/data/input/accounts.csv", "operation_type": "insert" }`
*   `POST /bulk/dml-direct-payload`: Submit a Bulk API DML job with a list of records in the payload.
    *   Payload: `{ "object_name": "Case", "operation": "update", "records": [{"Id": "500...", "Status": "Closed"}] }`
*   `POST /bulk/query-submit`: Submit a Bulk API SOQL query job.
    *   Payload: `{ "object_name": "Contact", "soql_query": "SELECT Id, Email FROM Contact WHERE CustomField__c = 'value'", "operation": "query" }`
*   `GET /bulk/job/{job_id}/status?is_query_job=false&include_results_data=false`: Get Bulk API job status. Optionally include results if job is complete.

**Health & Metrics:**
*   `GET /health`: Basic health check.
*   `GET /metrics`: Detailed system and application metrics.

## Configuration

The application is configured via environment variables. See `.env.example` for all available options.

| Variable                         | Description                                                                 | Default (from `src/core/config.py`)      |
| -------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------- |
| `DEBUG_MODE`                     | Enable debug mode (e.g., for verbose logging, auto-reload)                | `False`                                        |
| `APP_NAME`                       | Application Name for logging and documentation                            | `SalesforceIntegrationAPI`                     |
| `APP_VERSION`                    | Application Version                                                         | `1.0.0`                                        |
| `API_V1_STR`                     | API version prefix for routes                                               | `/api/v1`                                      |
| `SALESFORCE_CLIENT_ID`           | Salesforce Connected App Client ID                                          | **Required in `.env`**                         |
| `SALESFORCE_CLIENT_SECRET`       | Salesforce Connected App Client Secret                                      | **Required in `.env`**                         |
| `SALESFORCE_USERNAME`            | Salesforce Username                                                         | **Required in `.env`**                         |
| `SALESFORCE_PASSWORD`            | Salesforce Password (append security token if IP restrictions not set up)   | **Required in `.env`**                         |
| `SALESFORCE_TOKEN_URL`           | Salesforce OAuth token endpoint                                             | `https://login.salesforce.com/services/oauth2/token` |
| `SALESFORCE_API_VERSION`         | Salesforce API version to use                                               | `v58.0`                                        |
| `SALESFORCE_TOKEN_REFRESH_BUFFER`| Seconds before token expiry to attempt refresh                              | `300`                                          |
| `LOG_LEVEL`                      | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)                       | `INFO`                                         |
| `LOG_FILENAME`                   | Path to log file. If empty, logs only to console.                           | `sfdc_api.log` (or None if env var missing)  |
| `LOG_MAX_BYTES`                  | Max bytes for rotating log file                                             | `10485760` (10MB)                            |
| `LOG_BACKUP_COUNT`               | Number of backup log files to keep                                          | `5`                                            |
| `BACKEND_CORS_ORIGINS`           | Comma-separated list of allowed CORS origins (e.g. "http://localhost,https://my.app") | `[]` (empty list)                            |
| `DATA_PATH_INPUT`                | Default path for input files (used by `/bulk/local-file-process`)           | `/app/data/input` (within container)         |
| `DATA_PATH_OUTPUT`               | Default path for output files (if any generated)                            | `/app/data/output` (within container)        |
| `DATA_PATH_FAILED`               | Default path for storing details of failed records from file processing     | `/app/data/failed` (within container)        |


## Kubernetes Deployment

Example Kubernetes manifests are provided in the `/kubernetes` directory:
- `configmap.yaml`: For managing non-sensitive environment variables.
- `secrets.yaml.example`: Template for creating Kubernetes Secrets for sensitive data (like Salesforce credentials). **You must create your own `secrets.yaml` from this template.**
- `pv.yaml` & `pvc.yaml`: Example for persistent storage (e.g., if processing files from a shared volume). These are generic and need adaptation to your cluster's storage solution (avoid `hostPath` in production).
- `deployment.yaml`: Defines the application deployment, referencing the ConfigMap and Secrets.
- `service.yaml`: Exposes the application within the cluster (default `ClusterIP`). Includes notes on `NodePort` and `LoadBalancer` types.
- `hpa.yaml`: Configures Horizontal Pod Autoscaler based on CPU/memory.

**Note:** These are example manifests. You'll need to customize them for your specific Kubernetes environment, especially regarding ConfigMap data, actual secret creation, ingress controllers, persistent volume details, and image paths in `deployment.yaml`.

## CI/CD

A basic CI/CD pipeline is defined in `.github/workflows/ci.yml`. It includes:
1.  **Linting**: Using Flake8.
2.  **Testing**: Running unit and integration tests with Pytest.
3.  **Docker Build**: Building the Docker image.
    (Pushing to a registry is commented out and requires secrets configuration).

## Contributing

Please refer to `CONTRIBUTING.md` for guidelines (to be created).

## License

This project is licensed under the MIT License - see the `LICENSE` file for details (to be created or confirmed).
```
