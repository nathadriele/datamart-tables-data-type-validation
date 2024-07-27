## Data Mart Tables Data Type Validation

### Overview
This project is a Data Engineering solution implemented in Mage.ai locally for validating the data types of columns in PostgreSQL tables within a Data Mart. It ensures that data stored in the tables conforms to the expected data types, enhancing data integrity and reliability. This validation is crucial for maintaining high-quality data, which is essential for accurate data analysis and reporting.

## Getting Started

### Prerequisites
Ensure you have the following software installed:

- Python 3.7 or higher
- PostgreSQL
- A tool to manage virtual environments (e.g., virtualenv or conda)

### Setup
1. Clone the repository:

```py
git clone https://github.com/nathadriele/datamart-tables-data-type-validation.git
cd datamart-tables-data-type-validation
```

2. Create and activate a virtual environment:

```py
virtualenv venv
source venv/bin/activate
```

3. Install the required dependencies:

```py
pip install -r requirements.txt
```

4. Set up environment variables:

Configure your environment variables for PostgreSQL connection details. You can use a .env file or set them directly in your environment.

### Running the Validation Script
Execute the data validation script using:

```py
python data_validation.py
```

### Detailed Functionality
`data_validation.py`

### Functions
- `connect_to_postgres()`
   - Establishes and returns a connection to the PostgreSQL database.
   - Uses secrets to fetch connection details.
   - Importance: Ensures secure and reliable connection to the database.

- `validate_data_types(cursor, table_name, columns, limit=1000)`
   - Validates column data types in the specified table.
   - Returns rows with data type mismatches.
   - Importance: Ensures data integrity by verifying that the data types in the database match the expected types.

- `validate_type(value, expected_type)`
   - Validates if the value matches the expected data type.
   - Importance: Provides a mechanism to check each value against its expected data type, enhancing data accuracy.

- `send_event_to_notifier(event_name, status)`
   - Sends a notification about the validation event to an external service.
   - Importance: Alerts the system about validation results, allowing for proactive data management.

- `check_table_updates(postgres_conn)`
   - Checks for updates in specified tables and validates data types.
   - Sends validation results as notifications.
   - Importance: Automates the validation process, ensuring continuous data quality monitoring.

### Configuration
The tables and columns to be validated are defined in the check_table_updates function. You can modify the tables_columns dictionary to include your specific tables and columns.

### Logging
The script uses the Python logging module for logging information, errors, and debug messages. This helps in tracking the script's execution and identifying issues.

### Testing
The script includes basic testing functions to ensure the data loader function works as expected.

### Contribution to Data Engineering
This project plays a vital role in Data Engineering by ensuring that the data stored in PostgreSQL tables conforms to the expected data types. Data Engineers rely on accurate and reliable data to build data pipelines, perform ETL processes, and generate meaningful insights.
By validating data types:
   - Data Integrity: Ensures that the data is consistent and conforms to the defined schema.
   - Error Reduction: Identifies and mitigates data type mismatches early, reducing errors in downstream processes.
   - Automated Monitoring: Automates data validation, allowing Data Engineers to focus on more complex tasks.
   - Proactive Alerts: Sends notifications about validation results, enabling timely interventions to maintain data quality.







