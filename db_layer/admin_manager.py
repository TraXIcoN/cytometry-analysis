import sqlite3
import pandas as pd
import logging
import json
from datetime import datetime
import shutil
import os

logger = logging.getLogger(__name__)

def log_operation(db_file, operation_type, sample_id=None, details=None):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(details, dict):
            details_json = json.dumps(details)
        else:
            details_json = str(details) # Fallback if details is not a dict

        c.execute('''
            INSERT INTO operation_log (timestamp, operation_type, sample_id, details)
            VALUES (?, ?, ?, ?)
        ''', (timestamp, operation_type, sample_id, details_json))
        conn.commit()
        logger.info(f"Logged operation: {operation_type} for sample_id: {sample_id}")
    except Exception as e:
        logger.error(f"Error logging operation: {e}")
        # Potentially rollback, but logging failure shouldn't stop main ops
    finally:
        conn.close()

def get_operation_log(db_file, limit=50):
    conn = sqlite3.connect(db_file)
    try:
        query = f"SELECT timestamp, operation_type, sample_id, details FROM operation_log ORDER BY timestamp DESC LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
        
        def try_parse_json(data):
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data # Return as is if not valid JSON
            return data

        if 'details' in df.columns:
            df['details'] = df['details'].apply(try_parse_json)
        return df
    except Exception as e:
        logger.error(f"Error fetching operation log: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    finally:
        conn.close()

def create_db_checkpoint(db_file, checkpoint_dir="checkpoints"):
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir)
        logger.info(f"Created checkpoint directory: {checkpoint_dir}")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    checkpoint_name = f"db_checkpoint_{timestamp}.db"
    checkpoint_path = os.path.join(checkpoint_dir, checkpoint_name)

    try:
        shutil.copy2(db_file, checkpoint_path)
        logger.info(f"Database checkpoint created: {checkpoint_path}")
        log_operation(db_file, 'create_checkpoint', details={'checkpoint_path': checkpoint_path})
        return checkpoint_path
    except Exception as e:
        logger.error(f"Failed to create database checkpoint: {e}")
        return None

def list_db_checkpoints(db_file, checkpoint_dir="checkpoints"):
    if not os.path.exists(checkpoint_dir):
        logger.info(f"Checkpoint directory '{checkpoint_dir}' does not exist. No checkpoints to list.")
        return []
    
    try:
        checkpoint_files = []
        for f_name in os.listdir(checkpoint_dir):
            if f_name.startswith('db_checkpoint_') and f_name.endswith('.db'):
                full_path = os.path.join(checkpoint_dir, f_name)
                checkpoint_files.append(full_path)
        
        # Sort by modification time, newest first
        checkpoint_files.sort(key=lambda f_path: os.path.getmtime(f_path), reverse=True)
        return checkpoint_files
    except Exception as e:
        logger.error(f"Error listing database checkpoints: {e}")
        return []

def revert_to_db_checkpoint(current_db_file, checkpoint_db_file):
    if not os.path.exists(checkpoint_db_file):
        logger.error(f"Checkpoint file '{checkpoint_db_file}' not found.")
        return False, "Checkpoint file not found."
    
    try:
        # It's safer to copy the checkpoint over the current DB file
        # than to rename, to avoid data loss if something goes wrong.
        # Ensure current_db_file path is valid and we have write permissions.
        shutil.copy2(checkpoint_db_file, current_db_file)
        logger.info(f"Database successfully reverted from checkpoint '{checkpoint_db_file}' to '{current_db_file}'.")
        log_operation(current_db_file, 'revert_checkpoint', details={'reverted_from': checkpoint_db_file})
        return True, f"Database successfully reverted from {os.path.basename(checkpoint_db_file)}."
    except Exception as e:
        logger.error(f"Failed to revert database from checkpoint '{checkpoint_db_file}': {e}")
        return False, f"Failed to revert database: {e}"
