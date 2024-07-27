import psycopg2
import requests
import logging
from datetime import date, datetime
from typing import Dict, List, Tuple, Any
from decimal import Decimal
from contextlib import contextmanager
from mage_ai.data_preparation.shared.secrets import get_secret_value

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@contextmanager
def connect_to_postgres():
    """Context manager for PostgreSQL connection."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=get_secret_value('postgres_host'),
            port=get_secret_value('postgres_port'),
            dbname=get_secret_value('postgres_dbname'),
            user=get_secret_value('postgres_user'),
            password=get_secret_value('postgres_pw')
        )
        logging.info("Connection to PostgreSQL database established successfully!")
        yield conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("PostgreSQL connection closed.")

def validate_data_types(cursor: psycopg2.extensions.cursor, table_name: str, columns: Dict[str, str], limit: int = 1000) -> List[Tuple[str, str, Any, str, Tuple]]:
    """Validates column data types in tables and returns rows with errors from the latest records of the current date."""
    validation_errors = []
    query = f"SELECT {', '.join(columns.keys())} FROM {table_name} ORDER BY ref_date DESC LIMIT %s"
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    for row in rows:
        row_errors = []
        for idx, (column, expected_type) in enumerate(columns.items()):
            value = row[idx]
            if not validate_type(value, expected_type):
                row_errors.append((table_name, column, value, expected_type))
        if row_errors:
            for error in row_errors:
                validation_errors.append(error + (row,))
    return validation_errors

def validate_type(value: Any, expected_type: str) -> bool:
    """Validates if the value matches the expected format."""
    type_validators = {
        'character varying': lambda v: isinstance(v, str),
        'text': lambda v: isinstance(v, str),
        'integer': lambda v: isinstance(v, int),
        'smallint': lambda v: isinstance(v, int) and -32768 <= v <= 32767,
        'bigint': lambda v: isinstance(v, int) and -9223372036854775808 <= v <= 9223372036854775807,
        'numeric': lambda v: isinstance(v, (int, float, Decimal)),
        'real': lambda v: isinstance(v, float),
        'double precision': lambda v: isinstance(v, float),
        'date': lambda v: isinstance(v, date),
        'timestamp without time zone': lambda v: isinstance(v, datetime)
    }
    return type_validators.get(expected_type, lambda _: False)(value)

def send_event_to_notifier(event_name: str, status: int) -> None:
    """Sends an event to the notifier service."""
    try:
        response = requests.post(
            'https://api.notifier.engineering.test.com/events',
            json={'name': event_name, 'status': status}
        )
        logging.info(f"Event '{event_name}' sent to Notifier: {response.status_code}")
    except requests.RequestException as e:
        logging.error(f"Error sending event to Notifier: {e}")

def check_table_updates(postgres_conn: psycopg2.extensions.connection) -> None:
    """Checks table updates and validates data types."""
tables_columns = {
    'datamart_test_main.card_transaction_summary': {
        'transaction_id': 'integer',
        'seller_id': 'integer',
        'seller_name': 'character varying',
        'buyer_id': 'integer',
        'buyer_name': 'character varying',
        'buyer_email': 'character varying',
        'transaction_date': 'date',
        'transaction_time': 'timestamp without time zone',
        'transaction_amount': 'numeric',
        'card_number': 'character varying',
        'card_type': 'character varying',
        'payment_status': 'text',
        'authorization_code': 'character varying',
        'installments': 'integer',
        'ref_date': 'date'
    },
    'datamart_test_main.pix_transaction_summary': {
        'transaction_id': 'integer',
        'seller_id': 'integer',
        'seller_name': 'character varying',
        'buyer_id': 'integer',
        'buyer_name': 'character varying',
        'buyer_email': 'character varying',
        'transaction_date': 'date',
        'transaction_time': 'timestamp without time zone',
        'transaction_amount': 'numeric',
        'pix_key': 'character varying',
        'payment_status': 'text',
        'reference_code': 'character varying',
        'ref_date': 'date'
    }
}
    try:
        with postgres_conn.cursor() as cursor:
            validation_errors = []
            for table, columns in tables_columns.items():
                errors = validate_data_types(cursor, table, columns)
                validation_errors.extend(errors)
            if validation_errors:
                for table, column, value, expected_type, row in validation_errors:
                    event_name = f"validation_error_{table}_{column}"
                    send_event_to_notifier(event_name, 0)
                    logging.error(f"Validation error in table {table}, column {column}:")
                    logging.error(f"  Value: {value}")
                    logging.error(f"  Expected type: {expected_type}")
                    logging.error("---")
            else:
                send_event_to_notifier('validation_success', 1)
    except psycopg2.Error as e:
        logging.error(f"Error checking record format(s) in table: {e}")
        raise

@data_loader
def data_loader(*args, **kwargs) -> Dict[str, int]:
    """Data loader function to check table updates."""
    with connect_to_postgres() as postgres_conn:
        check_table_updates(postgres_conn)
    return {'status': 200}

@test
def test_output(output: Dict[str, int], *args) -> None:
    """Test function to validate the output of the data loader."""
    assert output is not None, 'Output is undefined'
    assert output.get('status') == 200, 'Output status is not 200'
