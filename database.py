import sqlite3
import pandas as pd
import streamlit as st
import logging
from uuid import uuid4
import json
from datetime import datetime
import shutil # For file operations like copy
import os # For directory creation

logger = logging.getLogger(__name__)

def init_db(db_file):
    logger.info(f"init_db: Connecting to {db_file}")
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    logger.info(f"init_db: Connection and cursor established for {db_file}")
    # Drop existing tables if they exist
    c.execute('DROP TABLE IF EXISTS cell_counts')
    c.execute('DROP TABLE IF EXISTS samples')
    # Create tables
    c.execute('''
        CREATE TABLE samples (
            sample_id TEXT PRIMARY KEY,
            project TEXT,
            subject TEXT,
            condition TEXT,
            age INTEGER,
            sex TEXT,
            treatment TEXT,
            response TEXT,
            sample_type TEXT,
            time_from_treatment_start INTEGER
        )
    ''')
    logger.info("init_db: CREATE TABLE IF NOT EXISTS samples statement executed.")
    c.execute('''
        CREATE TABLE cell_counts (
            id TEXT PRIMARY KEY,
            sample_id TEXT,
            population TEXT,
            count INTEGER,
            FOREIGN KEY (sample_id) REFERENCES samples (sample_id)
        )
    ''')
    logger.info("init_db: CREATE TABLE IF NOT EXISTS cell_counts statement executed.")

    # Add Indexes
    logger.info("init_db: Creating indexes...")
    c.execute('CREATE INDEX IF NOT EXISTS idx_samples_sample_id ON samples (sample_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_samples_time_from_treatment_start ON samples (time_from_treatment_start)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_cell_counts_sample_id ON cell_counts (sample_id)')
    logger.info("init_db: Indexes created.")

    # Create operation_log table
    c.execute('''
        CREATE TABLE IF NOT EXISTS operation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            operation_type TEXT NOT NULL,
            sample_id TEXT,
            details TEXT
        )
    ''')
    logger.info("init_db: CREATE TABLE IF NOT EXISTS operation_log statement executed.")

    logger.info("init_db: Attempting to commit changes.")
    conn.commit()
    logger.info("init_db: Commit successful.")
    conn.close()
    logger.info(f"init_db: Connection to {db_file} closed.")

def load_csv_to_db(db_file, csv_file, chunk_size=1000):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    logger.info(f"load_csv_to_db: Loading CSV '{csv_file}' in chunks of {chunk_size}.")
    
    processed_rows = 0
    for chunk_df in pd.read_csv(csv_file, chunksize=chunk_size):
        logger.info(f"load_csv_to_db: Processing chunk {processed_rows // chunk_size + 1} with {len(chunk_df)} rows.")
        for _, row in chunk_df.iterrows():
            try:
                # Validate sample data (basic example, can be expanded)
                sample_id = row['sample']
                if not sample_id:
                    logger.warning(f"load_csv_to_db: Skipping row due to missing sample_id: {row.to_dict()}")
                    continue

                # Validate age and time_from_treatment_start (must be numbers or NaN)
                try:
                    age = pd.to_numeric(row['age'], errors='coerce')
                    time_from_treatment_start = pd.to_numeric(row['time_from_treatment_start'], errors='coerce')
                except Exception as e:
                    logger.warning(f"load_csv_to_db: Error converting age/time for sample {sample_id}: {e}. Skipping sample.")
                    continue

                c.execute('''
                    INSERT OR REPLACE INTO samples (
                        sample_id, project, subject, condition, age, sex, 
                        treatment, response, sample_type, time_from_treatment_start
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sample_id, 
                    str(row['project']).strip() if pd.notna(row['project']) else None,
                    str(row['subject']).strip() if pd.notna(row['subject']) else None, 
                    str(row['condition']).strip().lower() if pd.notna(row['condition']) else None, 
                    None if pd.isna(age) else int(age), 
                    str(row['sex']).strip() if pd.notna(row['sex']) else None, 
                    str(row['treatment']).strip() if pd.notna(row['treatment']) else None, 
                    str(row['response']).strip() if pd.notna(row['response']) else None, 
                    str(row['sample_type']).strip() if pd.notna(row['sample_type']) else None, 
                    None if pd.isna(time_from_treatment_start) else int(time_from_treatment_start)
                ))
                
                for pop in ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']:
                    if pop in row and pd.notna(row[pop]):
                        try:
                            count = int(row[pop])
                            if count < 0:
                                logger.warning(f"load_csv_to_db: Invalid cell count ({count}) for sample '{sample_id}', population '{pop}'. Skipping this cell count entry.")
                                continue # Skip this specific cell count entry
                            c.execute('''
                                INSERT OR REPLACE INTO cell_counts (id, sample_id, population, count)
                                VALUES (?, ?, ?, ?)
                            ''', (str(uuid4()), sample_id, pop, count))
                        except ValueError:
                            logger.warning(f"load_csv_to_db: Invalid cell count value ('{row[pop]}') for sample '{sample_id}', population '{pop}'. Skipping this cell count entry.")
                            continue # Skip this specific cell count entry
            except Exception as e:
                logger.error(f"load_csv_to_db: Error processing row: {row.to_dict()}. Error: {e}")
                # Decide if you want to skip the row or stop the process
                # For now, we'll skip the row and continue
                continue
        processed_rows += len(chunk_df)
        conn.commit() # Commit after each chunk
        logger.info(f"load_csv_to_db: Committed chunk. Total rows processed so far: {processed_rows}")

    logger.info(f"load_csv_to_db: Finished loading. Total rows processed: {processed_rows}")
    conn.close()

def add_sample(db_file, sample_data):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    try:
        sample_id = sample_data.get('sample_id')
        if not sample_id:
            logger.error("add_sample: 'sample_id' is missing or empty. Sample not added.")
            return False

        # Validate and prepare numeric fields
        try:
            age = sample_data.get('age')
            age = int(age) if age is not None and str(age).strip() != "" else None
        except ValueError:
            logger.warning(f"add_sample: Invalid age value '{sample_data.get('age')}' for sample '{sample_id}'. Storing as NULL.")
            age = None

        try:
            time_from_treatment_start = sample_data.get('time_from_treatment_start')
            time_from_treatment_start = int(time_from_treatment_start) if time_from_treatment_start is not None and str(time_from_treatment_start).strip() != "" else None
        except ValueError:
            logger.warning(f"add_sample: Invalid time_from_treatment_start value '{sample_data.get('time_from_treatment_start')}' for sample '{sample_id}'. Storing as NULL.")
            time_from_treatment_start = None

        c.execute('''
            INSERT INTO samples (
                sample_id, project, subject, condition, age, sex, 
                treatment, response, sample_type, time_from_treatment_start
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            sample_id, 
            sample_data.get('project'), 
            sample_data.get('subject'),
            str(sample_data.get('condition')).lower() if sample_data.get('condition') else None,
            age, 
            sample_data.get('sex'),
            sample_data.get('treatment'), 
            sample_data.get('response'), 
            sample_data.get('sample_type'),
            time_from_treatment_start
        ))
        logger.info(f"add_sample: Inserted sample '{sample_id}' into 'samples' table.")

        if 'cell_counts' in sample_data and isinstance(sample_data['cell_counts'], dict):
            for pop, count_val in sample_data['cell_counts'].items():
                try:
                    count = int(count_val)
                    if count < 0:
                        logger.warning(f"add_sample: Invalid cell count ({count}) for sample '{sample_id}', population '{pop}'. Skipping this cell count entry.")
                        continue
                    c.execute('''
                        INSERT INTO cell_counts (id, sample_id, population, count)
                        VALUES (?, ?, ?, ?)
                    ''', (str(uuid4()), sample_id, pop, count))
                    logger.debug(f"add_sample: Inserted cell count for sample '{sample_id}', population '{pop}'.")
                except ValueError:
                    logger.warning(f"add_sample: Invalid cell count value ('{count_val}') for sample '{sample_id}', population '{pop}'. Skipping this cell count entry.")
                    continue
        conn.commit()
        logger.info(f"add_sample: Successfully added sample '{sample_id}' and its cell counts.")
        # Log the add operation
        log_operation(db_file, 'add_sample', sample_id=sample_id, details=sample_data)
        return True
    except sqlite3.Error as e:
        logger.error(f"add_sample: Database error for sample_id '{sample_data.get('sample_id')}': {e}")
        conn.rollback() # Rollback changes in case of error
        return False
    except Exception as e:
        logger.error(f"add_sample: Unexpected error for sample_id '{sample_data.get('sample_id')}': {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def log_operation(db_file, operation_type, sample_id=None, details=None):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    serialized_details = None
    if details is not None:
        try:
            serialized_details = json.dumps(details)
        except TypeError as e:
            logger.error(f"log_operation: Could not serialize details to JSON for op '{operation_type}', sid '{sample_id}'. Err: {e}. Fallback to str.")
            serialized_details = str(details) # Fallback to string if JSON serialization fails
            
    try:
        c.execute('''
            INSERT INTO operation_log (timestamp, operation_type, sample_id, details)
            VALUES (?, ?, ?, ?)
        ''', (timestamp, operation_type, sample_id, serialized_details))
        conn.commit()
        logger.info(f"log_operation: Logged '{operation_type}' for sample_id '{sample_id}'.")
    except sqlite3.Error as e:
        logger.error(f"log_operation: Failed to log op '{operation_type}' for sid '{sample_id}'. SQLite Err: {e}")
        conn.rollback() # Ensure rollback on error
    finally:
        conn.close()

def remove_sample(db_file, sample_id):
    logger.info(f"Attempting to remove sample_id: {sample_id}")
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM cell_counts WHERE sample_id = ?", (sample_id,))
        c.execute("DELETE FROM samples WHERE sample_id = ?", (sample_id,))
        conn.commit()
        logger.info(f"remove_sample: Successfully removed sample_id: {sample_id}")
        # Log the remove operation
        log_operation(db_file, 'remove_sample', sample_id=sample_id)
        return True
    except sqlite3.Error as e:
        logger.error(f"remove_sample: Database error for sample_id '{sample_id}': {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

@st.cache_data
def get_distinct_values(db_file, column_name, table_name='samples'):
    conn = sqlite3.connect(db_file)
    try:
        query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL AND {column_name} != ''"
        df = pd.read_sql(query, conn)
        return sorted(df[column_name].tolist()) # Sorted for consistent UI
    finally:
        conn.close()

def get_filtered_data(db_file, selected_project=None,
                      selected_condition=None,
                      selected_treatment=None, selected_response=None):
    conn = sqlite3.connect(db_file)
    try:
        logger.info(f"get_filtered_data called with: projects={selected_project}, conditions={selected_condition}, treatments={selected_treatment}, responses={selected_response}")
        query = """
            SELECT s.*,
                   b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte
            FROM samples s
            LEFT JOIN (
                SELECT
                    sample_id,
                    MAX(CASE WHEN population = 'b_cell' THEN count END) as b_cell,
                    MAX(CASE WHEN population = 'cd8_t_cell' THEN count END) as cd8_t_cell,
                    MAX(CASE WHEN population = 'cd4_t_cell' THEN count END) as cd4_t_cell,
                    MAX(CASE WHEN population = 'nk_cell' THEN count END) as nk_cell,
                    MAX(CASE WHEN population = 'monocyte' THEN count END) as monocyte
                FROM cell_counts
                GROUP BY sample_id
            ) c ON s.sample_id = c.sample_id
        """
        conditions_clauses = []
        params = []

        if selected_project: # This will be a list from st.multiselect
            placeholders = ','.join(['?'] * len(selected_project))
            conditions_clauses.append(f"s.project IN ({placeholders})")
            params.extend(selected_project)
        
        if selected_condition: # This will be a list
            placeholders = ','.join(['?'] * len(selected_condition))
            conditions_clauses.append(f"s.condition IN ({placeholders})")
            params.extend(selected_condition)

        if selected_treatment: # This will be a list
            placeholders = ','.join(['?'] * len(selected_treatment))
            conditions_clauses.append(f"s.treatment IN ({placeholders})")
            params.extend(selected_treatment)

        if selected_response: # This will be a list
            # In app.py, responses_options = ['y', 'n', ''] where '' means 'All'
            # We only want to filter if 'y' or 'n' are explicitly selected.
            # If '' is selected (meaning 'All'), we don't add a response filter unless 'y' or 'n' are also selected.
            actual_responses_to_filter = [r for r in selected_response if r in ['y', 'n']]
            
            if actual_responses_to_filter: # Only add IN clause if 'y' or 'n' are selected
                placeholders = ','.join(['?'] * len(actual_responses_to_filter))
                conditions_clauses.append(f"s.response IN ({placeholders})")
                params.extend(actual_responses_to_filter)
            # If selected_response is empty or contains only '', no response-specific WHERE clause is added.

        if conditions_clauses:
            query += " WHERE " + " AND ".join(conditions_clauses)

        # Ensure params is a tuple for sqlite3, or None if empty
        final_params = tuple(params) if params else None
        logger.info(f"get_filtered_data SQL Query: {query}")
        logger.info(f"get_filtered_data SQL Params: {final_params}")
        df = pd.read_sql(query, conn, params=final_params)
        if not df.empty:
            logger.info(f"get_filtered_data returned sample_ids: {df['sample_id'].tolist()}")
        else:
            logger.info("get_filtered_data returned empty DataFrame")
        return df
    finally:
        conn.close()

@st.cache_data
def get_all_data(db_file):
    logger.info(f"get_all_data: Connecting to {db_file} to fetch all samples.")
    conn = sqlite3.connect(db_file)
    try:
        query = """
            SELECT s.*, 
                   b_cell, cd8_t_cell, cd4_t_cell, nk_cell, monocyte
            FROM samples s
            LEFT JOIN (
                SELECT 
                    sample_id,
                    MAX(CASE WHEN population = 'b_cell' THEN count END) as b_cell,
                    MAX(CASE WHEN population = 'cd8_t_cell' THEN count END) as cd8_t_cell,
                    MAX(CASE WHEN population = 'cd4_t_cell' THEN count END) as cd4_t_cell,
                    MAX(CASE WHEN population = 'nk_cell' THEN count END) as nk_cell,
                    MAX(CASE WHEN population = 'monocyte' THEN count END) as monocyte
                FROM cell_counts
                GROUP BY sample_id
            ) c ON s.sample_id = c.sample_id
        """
        logger.info(f"get_all_data: SQL Query: {query}")
        df = pd.read_sql_query(query, conn)
        if not df.empty:
            logger.info(f"get_all_data: Fetched {len(df)} samples. Sample IDs: {df['sample_id'].tolist()}")
        else:
            logger.info("get_all_data: Fetched 0 samples.")
        return df
    finally:
        logger.info(f"get_all_data: Closing connection to {db_file}.")
        conn.close()

def get_data_for_frequency_table(db_file):
    conn = sqlite3.connect(db_file)
    try:
        df = pd.read_sql_query('''
            SELECT s.sample_id, c.population, c.count,
                   SUM(c.count) OVER (PARTITION BY s.sample_id) as total_count
            FROM samples s
            JOIN cell_counts c ON s.sample_id = c.sample_id
        ''', conn)
        return df
    finally:
        conn.close()

def get_data_for_treatment_response_analysis(db_file):
    conn = sqlite3.connect(db_file)
    try:
        df = pd.read_sql_query('''
            SELECT s.sample_id, s.condition, s.treatment, s.response, s.sample_type,
                   c.population, c.count,
                   SUM(c.count) OVER (PARTITION BY s.sample_id) as total_count
            FROM samples s
            JOIN cell_counts c ON s.sample_id = c.sample_id
            WHERE s.condition = 'melanoma' AND s.treatment = 'tr1' AND s.sample_type = 'PBMC'
        ''', conn)
        return df
    finally:
        conn.close()

def get_data_for_baseline_analysis(db_file):
    conn = sqlite3.connect(db_file)
    try:
        df = pd.read_sql_query('''
            SELECT project, subject, response, sex
            FROM samples
            WHERE condition = 'melanoma' AND treatment = 'tr1' 
            AND sample_type = 'PBMC' AND time_from_treatment_start = 0
        ''', conn)
        return df
    finally:
        conn.close()

def get_operation_log(db_file, limit=50):
    conn = sqlite3.connect(db_file)
    try:
        # Ensure the table exists before querying, or handle the error gracefully
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='operation_log';")
        if cursor.fetchone() is None:
            logger.warning("get_operation_log: 'operation_log' table does not exist. Returning empty DataFrame.")
            return pd.DataFrame(columns=['id', 'timestamp', 'operation_type', 'sample_id', 'details'])
        
        df = pd.read_sql_query(f"SELECT id, timestamp, operation_type, sample_id, details FROM operation_log ORDER BY timestamp DESC, id DESC LIMIT {limit}", conn)
        
        def try_parse_json(data):
            if data and isinstance(data, str):
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data # Return original string if not valid JSON
            return data
        if 'details' in df.columns:
            df['details'] = df['details'].apply(try_parse_json)
        return df
    except sqlite3.Error as e:
        logger.error(f"get_operation_log: Failed to fetch operation log. Error: {e}")
        return pd.DataFrame(columns=['id', 'timestamp', 'operation_type', 'sample_id', 'details'])
    finally:
        conn.close()

def create_db_checkpoint(db_file, checkpoint_dir="checkpoints"):
    # Ensure the checkpoint directory is relative to the db_file's directory or an absolute path
    base_dir = os.path.dirname(db_file) if not os.path.isabs(checkpoint_dir) else ""
    actual_checkpoint_dir = os.path.join(base_dir, checkpoint_dir)

    if not os.path.exists(actual_checkpoint_dir):
        try:
            os.makedirs(actual_checkpoint_dir)
            logger.info(f"create_db_checkpoint: Created checkpoint directory '{actual_checkpoint_dir}'.")
        except OSError as e:
            logger.error(f"create_db_checkpoint: Failed to create checkpoint directory '{actual_checkpoint_dir}'. Error: {e}")
            return None

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Use the actual database file name in the checkpoint name for clarity if multiple dbs were managed
    db_filename_stem = os.path.splitext(os.path.basename(db_file))[0]
    checkpoint_filename = f"{db_filename_stem}_checkpoint_{timestamp_str}.db"
    checkpoint_path = os.path.join(actual_checkpoint_dir, checkpoint_filename)

    try:
        shutil.copy2(db_file, checkpoint_path) # copy2 preserves metadata
        logger.info(f"create_db_checkpoint: Database checkpoint created at '{checkpoint_path}'.")
        log_operation(db_file, 'create_checkpoint', details={'checkpoint_path': checkpoint_path})
        return checkpoint_path
    except Exception as e:
        logger.error(f"create_db_checkpoint: Failed to create checkpoint. Error: {e}")
        return None

def list_db_checkpoints(db_file, checkpoint_dir="checkpoints"):
    base_dir = os.path.dirname(db_file) if not os.path.isabs(checkpoint_dir) else ""
    actual_checkpoint_dir = os.path.join(base_dir, checkpoint_dir)

    if not os.path.exists(actual_checkpoint_dir):
        logger.info(f"list_db_checkpoints: Checkpoint directory '{actual_checkpoint_dir}' does not exist.")
        return []
    
    checkpoints = []
    try:
        db_filename_stem = os.path.splitext(os.path.basename(db_file))[0]
        expected_prefix = f"{db_filename_stem}_checkpoint_"
        for f_name in os.listdir(actual_checkpoint_dir):
            if f_name.startswith(expected_prefix) and f_name.endswith(".db"):
                checkpoints.append(os.path.join(actual_checkpoint_dir, f_name))
        # Sort by filename (which includes timestamp) for chronological order, newest first
        checkpoints.sort(reverse=True)
        return checkpoints
    except Exception as e:
        logger.error(f"list_db_checkpoints: Error listing checkpoints from '{actual_checkpoint_dir}'. Error: {e}")
        return []

def revert_to_db_checkpoint(current_db_file, checkpoint_db_file):
    if not os.path.exists(checkpoint_db_file):
        logger.error(f"revert_to_db_checkpoint: Checkpoint file '{checkpoint_db_file}' not found.")
        return False
    try:
        # Ensure current_db_file is not locked by another process if possible
        # For safety, one might first backup current_db_file before overwriting
        # temp_backup_path = current_db_file + ".bak_before_revert_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        # shutil.copy2(current_db_file, temp_backup_path)
        # logger.info(f"revert_to_db_checkpoint: Created temporary backup '{temp_backup_path}'.")

        shutil.copy2(checkpoint_db_file, current_db_file)
        logger.info(f"revert_to_db_checkpoint: DB '{current_db_file}' reverted using '{checkpoint_db_file}'.")
        
        # Log the revert operation to the now-reverted database
        log_operation(current_db_file, 'revert_to_checkpoint', details={'reverted_to': os.path.basename(checkpoint_db_file)})
        return True
    except Exception as e:
        logger.error(f"revert_to_db_checkpoint: Failed to revert database. Error: {e}")
        # If temp backup was made, consider restoring it or notifying user
        # if os.path.exists(temp_backup_path):
        #     shutil.copy2(temp_backup_path, current_db_file) # Attempt to restore original
        #     logger.error(f"revert_to_db_checkpoint: Restored original DB from temp backup due to error.")
        return False


# Expected columns for the samples table (excluding primary key if auto-generated)
EXPECTED_SAMPLE_COLUMNS = [
    'sample_id', 'project', 'subject', 'condition', 'age', 'sex',
    'treatment', 'response', 'sample_type', 'time_from_treatment_start'
]
# Column name in CSV that maps to 'sample_id' in the database
CSV_SAMPLE_ID_COLUMN = 'sample' 

# Expected cell population columns
EXPECTED_CELL_POPULATIONS = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']

def append_csv_to_db(db_file, uploaded_file_object, chunk_size=1000):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    logger.info(f"append_csv_to_db: Attempting to append CSV data from '{getattr(uploaded_file_object, 'name', 'N/A')}' in chunks of {chunk_size}.")

    rows_processed = 0
    samples_added_count = 0
    samples_skipped_existing_count = 0
    cell_counts_added_count = 0
    rows_with_errors_count = 0
    
    column_mapping = {CSV_SAMPLE_ID_COLUMN: 'sample_id'}

    try:
        for chunk_df in pd.read_csv(uploaded_file_object, chunksize=chunk_size, iterator=True):
            logger.info(f"append_csv_to_db: Processing chunk with {len(chunk_df)} rows.")
            
            # Rename CSV columns to match database schema where necessary
            current_chunk_columns = chunk_df.columns.tolist()
            renamed_cols = {csv_col: db_col for csv_col, db_col in column_mapping.items() if csv_col in current_chunk_columns}
            chunk_df.rename(columns=renamed_cols, inplace=True)

            # Ensure 'sample_id' is present after potential rename
            if 'sample_id' not in chunk_df.columns:
                logger.error(f"append_csv_to_db: Critical error - 'sample_id' (expected from '{CSV_SAMPLE_ID_COLUMN}') column missing in uploaded CSV. Aborting append for this chunk.")
                rows_with_errors_count += len(chunk_df)
                continue 

            # Filter DataFrame to include only expected sample columns and cell populations
            relevant_sample_cols_in_chunk = [col for col in EXPECTED_SAMPLE_COLUMNS if col in chunk_df.columns]
            relevant_cell_cols_in_chunk = [col for col in EXPECTED_CELL_POPULATIONS if col in chunk_df.columns]
            
            for _, row in chunk_df.iterrows():
                rows_processed += 1
                try:
                    sample_id = row.get('sample_id')
                    if not sample_id or pd.isna(sample_id):
                        logger.warning(f"append_csv_to_db: Skipping row {rows_processed} due to missing or invalid sample_id: {row.to_dict()}")
                        rows_with_errors_count += 1
                        continue

                    sample_data_to_insert = {}
                    for col in relevant_sample_cols_in_chunk:
                        if col in row and pd.notna(row[col]):
                            sample_data_to_insert[col] = str(row[col]).strip() if isinstance(row[col], str) else row[col]
                        else:
                            sample_data_to_insert[col] = None

                    try:
                        age_val = sample_data_to_insert.get('age')
                        sample_data_to_insert['age'] = int(pd.to_numeric(age_val, errors='raise')) if pd.notna(age_val) else None
                    except (ValueError, TypeError):
                        logger.warning(f"append_csv_to_db: Invalid age value '{age_val}' for sample '{sample_id}'. Storing as NULL.")
                        sample_data_to_insert['age'] = None
                    
                    try:
                        tfts_val = sample_data_to_insert.get('time_from_treatment_start')
                        sample_data_to_insert['time_from_treatment_start'] = int(pd.to_numeric(tfts_val, errors='raise')) if pd.notna(tfts_val) else None
                    except (ValueError, TypeError):
                        logger.warning(f"append_csv_to_db: Invalid time_from_treatment_start value '{tfts_val}' for sample '{sample_id}'. Storing as NULL.")
                        sample_data_to_insert['time_from_treatment_start'] = None

                    if sample_data_to_insert.get('condition'):
                         sample_data_to_insert['condition'] = str(sample_data_to_insert['condition']).lower()

                    # Use a list of values corresponding to relevant_sample_cols_in_chunk for insertion
                    sample_values_tuple = tuple(sample_data_to_insert.get(col) for col in relevant_sample_cols_in_chunk)

                    c.execute(f'''
                        INSERT OR IGNORE INTO samples ({', '.join(relevant_sample_cols_in_chunk)})
                        VALUES ({', '.join(['?'] * len(relevant_sample_cols_in_chunk))})
                    ''', sample_values_tuple)

                    if c.rowcount > 0:
                        samples_added_count += 1
                    else:
                        samples_skipped_existing_count +=1

                    for pop in relevant_cell_cols_in_chunk:
                        if pop in row and pd.notna(row[pop]):
                            try:
                                count = int(row[pop])
                                if count < 0:
                                    logger.warning(f"append_csv_to_db: Invalid cell count ({count}) for sample '{sample_id}', population '{pop}'. Skipping this cell count entry.")
                                    rows_with_errors_count +=1 
                                    continue
                                c.execute('''
                                    INSERT OR IGNORE INTO cell_counts (id, sample_id, population, count)
                                    VALUES (?, ?, ?, ?)
                                ''', (str(uuid4()), sample_id, pop, count))
                                if c.rowcount > 0:
                                    cell_counts_added_count += 1
                            except ValueError:
                                logger.warning(f"append_csv_to_db: Invalid cell count value ('{row[pop]}') for sample '{sample_id}', population '{pop}'. Skipping this cell count entry.")
                                rows_with_errors_count +=1 
                                continue
                except Exception as e_row:
                    logger.error(f"append_csv_to_db: Error processing row {rows_processed} for sample_id '{row.get('sample_id', 'UNKNOWN')}': {e_row}. Row data: {row.to_dict()}")
                    rows_with_errors_count += 1
                    continue
            
            conn.commit()
            logger.info(f"append_csv_to_db: Committed chunk. Total rows processed so far: {rows_processed}")

        summary_details = {
            'file_name': getattr(uploaded_file_object, 'name', 'N/A'),
            'rows_processed': rows_processed,
            'samples_added': samples_added_count,
            'samples_skipped_existing': samples_skipped_existing_count,
            'cell_counts_added': cell_counts_added_count,
            'rows_with_errors': rows_with_errors_count
        }
        log_operation(db_file, 'append_csv_data', details=summary_details)
        logger.info(f"append_csv_to_db: Finished appending data. Summary: {summary_details}")
        return True, summary_details

    except pd.errors.EmptyDataError:
        logger.warning("append_csv_to_db: The uploaded CSV file is empty.")
        return False, {"error": "Uploaded CSV file is empty."}
    except Exception as e_main:
        logger.error(f"append_csv_to_db: Failed to append CSV data. Error: {e_main}")
        conn.rollback()
        return False, {"error": str(e_main)}
    finally:
        conn.close()
