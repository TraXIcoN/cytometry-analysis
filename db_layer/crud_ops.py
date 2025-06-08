import sqlite3
import logging
from uuid import uuid4
import pandas as pd # For pd.notna in add_sample

# Assuming admin_manager.py will be in the same directory for log_operation
from .admin_manager import log_operation

logger = logging.getLogger(__name__)

def add_sample(db_file, sample_data):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    try:
        sample_id = sample_data.get('sample_id')
        if not sample_id:
            logger.error("add_sample: 'sample_id' is missing or empty. Sample not added.")
            return False, "'sample_id' is missing or empty."

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
        log_operation(db_file, 'add_sample', sample_id=sample_id, details={'data': sample_data})
        return True, f"Sample {sample_id} added successfully."
    except sqlite3.IntegrityError as e:
        conn.rollback()
        logger.error(f"add_sample: Failed to add sample '{sample_id}' due to integrity error (e.g., duplicate sample_id): {e}")
        return False, f"Failed to add sample '{sample_id}'. It might already exist."
    except Exception as e:
        conn.rollback()
        logger.error(f"add_sample: An unexpected error occurred while adding sample '{sample_id}': {e}")
        return False, f"An unexpected error occurred while adding sample '{sample_id}'."
    finally:
        conn.close()

def remove_sample(db_file, sample_id):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    try:
        # First, delete associated cell counts
        c.execute('DELETE FROM cell_counts WHERE sample_id = ?', (sample_id,))
        logger.info(f"remove_sample: Deleted cell counts for sample_id '{sample_id}'.")
        
        # Then, delete the sample itself
        c.execute('DELETE FROM samples WHERE sample_id = ?', (sample_id,))
        logger.info(f"remove_sample: Deleted sample with sample_id '{sample_id}'.")
        
        if c.rowcount == 0:
            logger.warning(f"remove_sample: No sample found with sample_id '{sample_id}' to delete.")
            conn.rollback() # Rollback if no sample was deleted
            return False, f"No sample found with ID {sample_id}."

        conn.commit()
        logger.info(f"remove_sample: Successfully removed sample '{sample_id}'.")
        log_operation(db_file, 'remove_sample', sample_id=sample_id)
        return True, f"Sample {sample_id} removed successfully."
    except Exception as e:
        conn.rollback()
        logger.error(f"remove_sample: Error removing sample '{sample_id}': {e}")
        return False, f"Error removing sample {sample_id}: {e}"
    finally:
        conn.close()
