import logging
import os
import streamlit as st
import pandas as pd
import db_layer as database

logger = logging.getLogger(__name__)

def initialize_app(db_file, csv_file):
    """Initializes the database if it doesn't exist and loads initial data."""
    logger.info(f"Checking for database file: {db_file}")
    if not os.path.exists(db_file):
        logger.info(f"Database file {db_file} not found. Initializing and loading data.")
        database.init_db(db_file)
        database.load_csv_to_db(db_file, csv_file)
        logger.info(f"Database initialized and data loaded from {csv_file} into {db_file}.")
    else:
        logger.info(f"Database file {db_file} found. Skipping initialization.")

def initialize_session_state(db_file):
    """Initializes Streamlit session state variables."""
    logger.info("Initializing session state...")
    if 'all_samples_df' not in st.session_state:
        st.session_state.all_samples_df = database.get_all_data(db_file)
        logger.info("Initial 'all_samples_df' loaded into session state.")

    filter_columns = ['sample_type', 'treatment'] # Changed 'cell_type' to 'sample_type' to match CSV
    for col in filter_columns:
        state_key = f'distinct_{col}s'
        if state_key not in st.session_state:
            try:
                values = database.get_distinct_values(db_file, col)
                if col in []: # No numeric columns left here for now
                    try:
                        numeric_values = sorted(list(set(pd.to_numeric(values, errors='coerce').dropna().unique())))
                        st.session_state[state_key] = numeric_values
                    except Exception: # Fallback to string sort if conversion fails
                        st.session_state[state_key] = sorted(list(set(values)))
                else:
                    st.session_state[state_key] = sorted(list(set(values)))
                logger.info(f"Loaded distinct values for '{col}' into session state.")
            except Exception as e:
                logger.error(f"Failed to load distinct values for '{col}': {e}")
                st.session_state[state_key] = []

    if 'confirm_removal_sample_id' not in st.session_state:
        st.session_state.confirm_removal_sample_id = None
    if 'confirm_removal_sample_id_target' not in st.session_state:
        st.session_state.confirm_removal_sample_id_target = None
    if 'show_revert_confirmation' not in st.session_state:
        st.session_state.show_revert_confirmation = False

    logger.info("Session state initialization complete.")
