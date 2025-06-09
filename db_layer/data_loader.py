import sqlite3
import pandas as pd
import logging
from uuid import uuid4
import json
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
from reporting_tools.cache_manager import cache_dataframe, get_cached_dataframe, invalidate_cache

# Assuming admin_manager.py will be in the same directory for log_operation
from .admin_manager import log_operation 
# Assuming schema_manager.py will be in the same directory for constants
from .schema_manager import CSV_SAMPLE_ID_COLUMN, EXPECTED_CELL_POPULATIONS, EXPECTED_SAMPLE_COLUMNS

logger = logging.getLogger(__name__)

def load_csv_to_db(db_file, csv_file, chunk_size=1000):
    # Check cache first
    cache_key = f"csv_data:{csv_file}:{chunk_size}"
    cached_df = get_cached_dataframe(cache_key)
    if cached_df is not None:
        logger.info(f"load_csv_to_db: Using cached data for {csv_file}")
        return cached_df

    # Open DB connection synchronously
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    csv_df = pd.read_csv(csv_file, na_filter=False)
    if CSV_SAMPLE_ID_COLUMN != 'sample_id' and CSV_SAMPLE_ID_COLUMN in csv_df.columns:
        csv_df = csv_df.rename(columns={CSV_SAMPLE_ID_COLUMN: 'sample_id'})
        logger.info(f"load_csv_to_db: Loading CSV '{csv_file}' in chunks of {chunk_size}.")
        start_time = datetime.now()
        
        processed_rows = 0
        chunks = pd.read_csv(csv_file, chunksize=chunk_size, na_filter=False)
        for chunk_df in chunks:
            logger.info(f"load_csv_to_db: Processing chunk {processed_rows // chunk_size + 1} with {len(chunk_df)} rows.")
        for _, row in chunk_df.iterrows():
            try:
                sample_id_val = str(row[CSV_SAMPLE_ID_COLUMN]).strip() if CSV_SAMPLE_ID_COLUMN in row and str(row[CSV_SAMPLE_ID_COLUMN]).strip() != "" else None
                logger.info(f"load_csv_to_db: Processing row with CSV sample value: '{row.get(CSV_SAMPLE_ID_COLUMN)}', parsed as sample_id: '{sample_id_val}'")
                # Cache individual chunks for faster future processing
                chunk_cache_key = f"csv_chunk:{csv_file}:{processed_rows // chunk_size}"
                cache_dataframe(chunk_df, chunk_cache_key, expire_seconds=3600)
                if not sample_id_val:
                    logger.warning(f"load_csv_to_db: Skipping row due to missing or empty sample_id. Original CSV value: '{row.get(CSV_SAMPLE_ID_COLUMN)}'. Row data: {row.to_dict()}")
                    continue
                sample_id = sample_id_val # Use the validated and stripped sample_id

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
                
                for pop_col_name in EXPECTED_CELL_POPULATIONS:
                    count_to_insert = None # Default to NULL
                    if pop_col_name in row: # Check if column exists in CSV row
                        cell_count_value_str = str(row[pop_col_name]).strip()
                        if cell_count_value_str != "": # If CSV cell count is not an empty string, try to parse it
                            try:
                                parsed_count = int(pd.to_numeric(cell_count_value_str, errors='raise'))
                                if parsed_count < 0:
                                    logger.warning(f"load_csv_to_db: Negative cell count ({parsed_count}) for sample '{sample_id}', population '{pop_col_name}'. Storing as NULL.")
                                    # count_to_insert remains None
                                else:
                                    count_to_insert = parsed_count
                            except (ValueError, TypeError):
                                logger.warning(f"load_csv_to_db: Invalid cell count value ('{cell_count_value_str}') for sample '{sample_id}', population '{pop_col_name}'. Storing as NULL.")
                                # count_to_insert remains None
                        # If cell_count_value_str was empty, count_to_insert is already None.
                    # else: column not in CSV row, count_to_insert remains None
                    logger.debug(f"load_csv_to_db: For sample '{sample_id}', population '{pop_col_name}', determined count_to_insert: {count_to_insert} (Original CSV: '{row.get(pop_col_name)}')")
                    
                    # Always attempt to insert, using None for count if it was missing, empty, or invalid
                    c.execute('''
                        INSERT OR REPLACE INTO cell_counts (id, sample_id, population, count)
                        VALUES (?, ?, ?, ?)
                    ''', (str(uuid4()), sample_id, pop_col_name, count_to_insert))
                    # No c.rowcount check needed here for counts_added as it's initial load
            except Exception as e:
                logger.error(f"load_csv_to_db: Error processing row: {row.to_dict()}. Error: {e}")
                continue
        processed_rows += len(chunk_df)
        conn.commit()
        logger.info(f"load_csv_to_db: Committed chunk. Total rows processed so far: {processed_rows}")

    logger.info(f"load_csv_to_db: Finished loading. Total rows processed: {processed_rows}")
    logger.info(f"load_csv_to_db: Total time taken: {datetime.now() - start_time}")
    
    # Cache the final DataFrame
    cache_dataframe(chunk_df, cache_key, expire_seconds=3600)
    
    return chunk_df
    conn.close()

def append_csv_to_db(db_file, uploaded_file_object, chunk_size=1000):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    file_name_for_logging = getattr(uploaded_file_object, 'name', 'N/A')
    logger.info(f"append_csv_to_db: Starting to append CSV data from '{file_name_for_logging}' in chunks of {chunk_size}.")

    rows_processed = 0
    samples_added_count = 0
    samples_skipped_existing_count = 0
    cell_counts_added_count = 0
    rows_with_errors_count = 0

    try:
        for chunk_df in pd.read_csv(uploaded_file_object, chunksize=chunk_size, na_filter=False):
            if CSV_SAMPLE_ID_COLUMN != 'sample_id' and CSV_SAMPLE_ID_COLUMN in chunk_df.columns:
                chunk_df = chunk_df.rename(columns={CSV_SAMPLE_ID_COLUMN: 'sample_id'})
            logger.info(f"append_csv_to_db: Processing chunk {rows_processed // chunk_size + 1} with {len(chunk_df)} rows.")
            
            # Determine which cell population columns are actually in this chunk
            relevant_cell_cols_in_chunk = [col for col in EXPECTED_CELL_POPULATIONS if col in chunk_df.columns]

            for _, row in chunk_df.iterrows():
                rows_processed += 1
                current_sample_id_from_csv = "<UNKNOWN_SAMPLE_ID>" # For logging in case of early error
                try:
                    # 1. Extract and validate sample_id (from CSV 'sample' column)
                    current_sample_id_from_csv = str(row[CSV_SAMPLE_ID_COLUMN]).strip() if CSV_SAMPLE_ID_COLUMN in row and str(row[CSV_SAMPLE_ID_COLUMN]).strip() != "" else None
                    if not current_sample_id_from_csv:
                        logger.warning(f"append_csv_to_db: Skipping row {rows_processed} due to missing or empty sample_id. Data: {row.to_dict()}")
                        rows_with_errors_count += 1
                        continue

                    # 2. Prepare sample_data_for_db (dictionary with DB column names as keys)
                    sample_data_for_db = {'sample_id': current_sample_id_from_csv}
                    for db_col_name in EXPECTED_SAMPLE_COLUMNS:
                        if db_col_name == 'sample_id': # Already handled
                            continue
                        
                        # Assume CSV column name is the same as DB column name for other metadata
                        csv_col_name = db_col_name 
                        if csv_col_name in row and str(row[csv_col_name]).strip() != "":
                            value = str(row[csv_col_name]).strip()
                            if db_col_name == 'age':
                                try: sample_data_for_db[db_col_name] = int(pd.to_numeric(value, errors='raise'))
                                except (ValueError, TypeError): 
                                    logger.warning(f"append_csv_to_db: Invalid age '{value}' for sample '{current_sample_id_from_csv}'. Storing as NULL.")
                                    sample_data_for_db[db_col_name] = None
                            elif db_col_name == 'time_from_treatment_start':
                                try: sample_data_for_db[db_col_name] = int(pd.to_numeric(value, errors='raise'))
                                except (ValueError, TypeError): 
                                    logger.warning(f"append_csv_to_db: Invalid time_from_treatment_start '{value}' for sample '{current_sample_id_from_csv}'. Storing as NULL.")
                                    sample_data_for_db[db_col_name] = None
                            elif db_col_name == 'condition':
                                sample_data_for_db[db_col_name] = value.lower()
                            else:
                                sample_data_for_db[db_col_name] = value
                        else:
                            sample_data_for_db[db_col_name] = None # If CSV col missing or empty string, store as NULL

                    # 3. Build column list and value tuple for SQL INSERT into 'samples' table
                    db_cols_for_sql_insert = []
                    db_values_for_sql_list = []
                    for col_name_in_order in EXPECTED_SAMPLE_COLUMNS: # Iterate to maintain defined column order
                        db_cols_for_sql_insert.append(col_name_in_order)
                        db_values_for_sql_list.append(sample_data_for_db.get(col_name_in_order)) # .get() is safer, defaults to None
                    
                    db_values_for_sql_tuple = tuple(db_values_for_sql_list)
                    
                    c.execute(f'''
                        INSERT OR IGNORE INTO samples ({', '.join(db_cols_for_sql_insert)})
                        VALUES ({', '.join(['?'] * len(db_cols_for_sql_insert))})
                    ''', db_values_for_sql_tuple)

                    if c.rowcount > 0:
                        samples_added_count += 1
                    else:
                        samples_skipped_existing_count += 1 # Assumed skipped due to existing PK

                    # 4. Insert cell counts for the current sample
                    for pop_col_name in relevant_cell_cols_in_chunk: # Iterate only over cell populations present in CSV
                        cell_count_value_str = str(row[pop_col_name]).strip()
                        count_to_insert = None # Default to NULL

                        if cell_count_value_str != "": # If CSV cell count is not an empty string, try to parse it
                            try:
                                parsed_count = int(pd.to_numeric(cell_count_value_str, errors='raise'))
                                if parsed_count < 0:
                                    logger.warning(f"append_csv_to_db: Negative cell count ({parsed_count}) for sample '{current_sample_id_from_csv}', population '{pop_col_name}'. Storing as NULL.")
                                    # count_to_insert remains None (or you could choose to skip: continue)
                                else:
                                    count_to_insert = parsed_count
                            except ValueError:
                                logger.warning(f"append_csv_to_db: Invalid cell count value ('{cell_count_value_str}') for sample '{current_sample_id_from_csv}', population '{pop_col_name}'. Storing as NULL.")
                                # count_to_insert remains None
                        # If cell_count_value_str was empty, count_to_insert is already None.
                        
                        # Always attempt to insert, using None for count if it was empty or invalid
                        c.execute('''
                            INSERT OR IGNORE INTO cell_counts (id, sample_id, population, count)
                            VALUES (?, ?, ?, ?)
                        ''', (str(uuid4()), current_sample_id_from_csv, pop_col_name, count_to_insert))
                        
                        # We count additions based on c.rowcount, regardless of whether count was NULL or a number
                        if c.rowcount > 0:
                            cell_counts_added_count += 1
                        # No explicit error count increment here unless c.execute itself fails (caught by outer try-except)
                
                except Exception as e_row:
                    logger.error(f"append_csv_to_db: Error processing row {rows_processed} for sample_id '{current_sample_id_from_csv}': {e_row}. Row data: {row.to_dict()}")
                    rows_with_errors_count += 1
                    continue # to next row in chunk_df
            
            conn.commit()
            logger.info(f"append_csv_to_db: Committed chunk. Total rows processed so far: {rows_processed}")

        summary_details = {
            'file_name': file_name_for_logging,
            'rows_processed': rows_processed,
            'samples_added': samples_added_count,
            'samples_skipped_existing': samples_skipped_existing_count,
            'cell_counts_added': cell_counts_added_count,
            'rows_with_errors': rows_with_errors_count
        }
        log_operation(db_file, 'append_csv_data', details=json.dumps(summary_details)) # Log details as JSON string
        logger.info(f"append_csv_to_db: Finished appending data. Summary: {summary_details}")
        return True, summary_details

    except pd.errors.EmptyDataError:
        logger.warning("append_csv_to_db: The uploaded CSV file is empty.")
        return False, {"error": "Uploaded CSV file is empty."}
    except Exception as e_main:
        logger.error(f"append_csv_to_db: Failed to append CSV data. Error: {e_main}", exc_info=True)
        conn.rollback()
        return False, {"error": str(e_main)}
    finally:
        if conn:
            conn.close()
