# db_layer/__init__.py

import logging

# Configure logging for the db_layer package
# This ensures that log messages from modules within db_layer are handled.
# If the main application sets up logging, these will propagate to that handler.
# If not, this provides a default basic configuration.
logger = logging.getLogger(__name__) # __name__ will be 'db_layer'
if not logger.hasHandlers():
    # Add a default handler if no handlers are configured by the application
    # This is useful for standalone testing of the db_layer or if app doesn't configure logging
    # For production, the application (app.py) should configure logging.
    # handler = logging.StreamHandler()
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # handler.setFormatter(formatter)
    # logger.addHandler(handler)
    # logger.setLevel(logging.INFO) # Or logging.DEBUG for more verbosity
    pass # Let the application configure logging

# Import functions and constants from schema_manager to make them available at db_layer level
from .schema_manager import (
    init_db,
    EXPECTED_SAMPLE_COLUMNS,
    CSV_SAMPLE_ID_COLUMN,
    EXPECTED_CELL_POPULATIONS
)

# Import functions from data_loader
from .data_loader import (
    load_csv_to_db,
    append_csv_to_db
)

# Import functions from crud_ops
from .crud_ops import (
    add_sample,
    remove_sample
)

# Import functions from query_executor
from .query_executor import (
    get_distinct_values,
    get_filtered_data,
    get_all_data,
    get_data_for_frequency_table,
    get_data_for_treatment_response_analysis,
    get_data_for_baseline_analysis,
    get_data_for_custom_baseline_query, # Added for custom query
    get_all_sample_ids_from_samples_table
)

# Import functions from admin_manager
from .admin_manager import (
    log_operation,
    get_operation_log,
    create_db_checkpoint,
    list_db_checkpoints,
    revert_to_db_checkpoint
)

# You can define an __all__ list if you want to specify what `from db_layer import *` imports
# This is good practice.
__all__ = [
    # schema_manager
    'init_db',
    'EXPECTED_SAMPLE_COLUMNS',
    'CSV_SAMPLE_ID_COLUMN',
    'EXPECTED_CELL_POPULATIONS',
    # data_loader
    'load_csv_to_db',
    'append_csv_to_db',
    # crud_ops
    'add_sample',
    'remove_sample',
    # query_executor
    'get_distinct_values',
    'get_filtered_data',
    'get_all_data',
    'get_data_for_frequency_table',
    'get_data_for_treatment_response_analysis',
    'get_data_for_baseline_analysis',
    'get_data_for_custom_baseline_query',
    'get_all_sample_ids_from_samples_table', # Ensure all query_executor functions are listed
    # admin_manager
    'log_operation',
    'get_operation_log',
    'create_db_checkpoint',
    'list_db_checkpoints',
    'revert_to_db_checkpoint'
]

logger.info("db_layer package initialized.")
