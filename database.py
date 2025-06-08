import sqlite3
import pandas as pd
import streamlit as st
import logging
from uuid import uuid4

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
    logger.info("init_db: Attempting to commit changes.")
    conn.commit()
    logger.info("init_db: Commit successful.")
    conn.close()
    logger.info(f"init_db: Connection to {db_file} closed.")

def load_csv_to_db(db_file, csv_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    df = pd.read_csv(csv_file)
    for _, row in df.iterrows():
        c.execute('''
            INSERT OR REPLACE INTO samples (
                sample_id, project, subject, condition, age, sex, 
                treatment, response, sample_type, time_from_treatment_start
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['sample'], 
            str(row['project']).strip() if pd.notna(row['project']) else None, 
            str(row['subject']).strip() if pd.notna(row['subject']) else None, 
            str(row['condition']).strip().lower() if pd.notna(row['condition']) else None, 
            row['age'], 
            str(row['sex']).strip() if pd.notna(row['sex']) else None, 
            str(row['treatment']).strip() if pd.notna(row['treatment']) else None, 
            str(row['response']).strip() if pd.notna(row['response']) else None, 
            str(row['sample_type']).strip() if pd.notna(row['sample_type']) else None, 
            row['time_from_treatment_start']
        ))
        for pop in ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']:
            if pop in row and pd.notna(row[pop]):
                c.execute('''
                    INSERT OR REPLACE INTO cell_counts (id, sample_id, population, count)
                    VALUES (?, ?, ?, ?)
                ''', (str(uuid4()), row['sample'], pop, int(row[pop])))
    conn.commit()
    conn.close()

def add_sample(db_file, sample_data):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''
        INSERT INTO samples (
            sample_id, project, subject, condition, age, sex, 
            treatment, response, sample_type, time_from_treatment_start
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        sample_data['sample_id'], sample_data['project'], sample_data['subject'],
        sample_data['condition'], sample_data['age'], sample_data['sex'],
        sample_data['treatment'], sample_data['response'], sample_data['sample_type'],
        sample_data['time_from_treatment_start']
    ))
    if 'cell_counts' in sample_data and isinstance(sample_data['cell_counts'], dict):
        for pop, count in sample_data['cell_counts'].items():
            c.execute('''
                INSERT INTO cell_counts (id, sample_id, population, count)
                VALUES (?, ?, ?, ?)
            ''', (str(uuid4()), sample_data['sample_id'], pop, count))
    conn.commit()
    conn.close()

def remove_sample(db_file, sample_id):
    logger.info(f"Attempting to remove sample_id: {sample_id}")
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('DELETE FROM samples WHERE sample_id = ?', (sample_id,))
    c.execute('DELETE FROM cell_counts WHERE sample_id = ?', (sample_id,))
    conn.commit()
    logger.info(f"Successfully removed sample_id: {sample_id}")
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
            SELECT s.sample_id, c.population, c.count
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
                   c.population, c.count
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
