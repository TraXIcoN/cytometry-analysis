import sqlite3
import pandas as pd
import logging
import streamlit as st

logger = logging.getLogger(__name__)

@st.cache_data
def get_distinct_values(db_file, column_name, table_name='samples'):
    conn = sqlite3.connect(db_file)
    try:
        query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL ORDER BY {column_name}"
        df = pd.read_sql_query(query, conn)
        return df[column_name].tolist()
    finally:
        conn.close()

def get_filtered_data(db_file, selected_project=None,
                      selected_condition=None,
                      selected_treatment=None, selected_response=None):
    conn = sqlite3.connect(db_file)
    try:
        base_query = '''
            SELECT 
                s.sample_id, s.project, s.subject, s.condition, s.age, s.sex,
                s.treatment, s.response, s.sample_type, s.time_from_treatment_start,
                c.population, c.count
            FROM samples s
            LEFT JOIN cell_counts c ON s.sample_id = c.sample_id
        '''
        conditions = []
        params = []

        if selected_project: 
            placeholders = ','.join(['?'] * len(selected_project))
            conditions.append(f"s.project IN ({placeholders})")
            params.extend(selected_project)

        if selected_condition: 
            placeholders = ','.join(['?'] * len(selected_condition))
            conditions.append(f"s.condition IN ({placeholders})")
            params.extend(selected_condition)

        if selected_treatment: 
            placeholders = ','.join(['?'] * len(selected_treatment))
            conditions.append(f"s.treatment IN ({placeholders})")
            params.extend(selected_treatment)

        if selected_response: 
            placeholders = ','.join(['?'] * len(selected_response))
            conditions.append(f"s.response IN ({placeholders})")
            params.extend(selected_response)

        if conditions:
            query = base_query + " WHERE " + " AND ".join(conditions) + " ORDER BY s.sample_id, c.population"
        else:
            query = base_query + " ORDER BY s.sample_id, c.population"
        
        logger.debug(f"Executing query: {query} with params: {params}")
        df = pd.read_sql_query(query, conn, params=params)

        if not df.empty and 'population' in df.columns and 'count' in df.columns:
            sample_cols = ['sample_id', 'project', 'subject', 'condition', 'age', 'sex', 'treatment', 'response', 'sample_type', 'time_from_treatment_start']
            # Filter out any sample_cols not actually present in df, though they should be from the SELECT
            sample_cols_present = [col for col in sample_cols if col in df.columns]
            
            if not sample_cols_present or 'sample_id' not in sample_cols_present:
                logger.error("Critical sample identifier columns missing in get_filtered_data before pivot.")
                return pd.DataFrame()

            df = df.pivot_table(index=sample_cols_present, columns='population', values='count').reset_index()
            df = df.fillna(0)
            
            expected_cell_cols = ['b_cell', 'cd4_t_cell', 'cd8_t_cell', 'monocyte', 'nk_cell']
            for col in expected_cell_cols:
                if col not in df.columns:
                    df[col] = 0.0 # Ensure float type if other counts are float
            logger.info(f"get_filtered_data: Pivoted data, {len(df)} rows.")
            return df
        elif df.empty:
            logger.info("get_filtered_data: No data returned from SQL query.")
            return pd.DataFrame() # Return empty DataFrame if SQL query returned nothing
        else: # Not empty, but missing critical columns for pivot
            logger.warning("get_filtered_data: Data is missing population/count columns for pivot. Returning empty DataFrame.")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error in get_filtered_data: {e}")
        return pd.DataFrame() # Return empty DataFrame on error
    finally:
        conn.close()

def get_all_sample_ids_from_samples_table(db_file):
    """
    Retrieves a sorted list of all unique sample_ids directly from the samples table.
    """
    conn = sqlite3.connect(db_file)
    try:
        query = "SELECT DISTINCT sample_id FROM samples ORDER BY sample_id"
        df = pd.read_sql_query(query, conn)
        if df.empty:
            logger.info("get_all_sample_ids_from_samples_table: No sample_ids found in samples table.")
            return []
        
        all_retrieved_ids = df['sample_id'].tolist()
        valid_ids = []
        none_count = 0
        empty_string_count = 0
        for sid in all_retrieved_ids:
            if sid is None:
                none_count += 1
            elif isinstance(sid, str) and sid.strip() == "":
                empty_string_count += 1
            elif isinstance(sid, str): # Ensure it's a string before adding
                valid_ids.append(sid)
            # else: could be other types if schema is not strictly enforced, but sample_id is TEXT

        if none_count > 0:
            logger.warning(f"get_all_sample_ids_from_samples_table: Found {none_count} NULL sample_ids in 'samples' table.")
        if empty_string_count > 0:
            logger.warning(f"get_all_sample_ids_from_samples_table: Found {empty_string_count} empty string sample_ids in 'samples' table.")
        
        sorted_valid_ids = sorted(valid_ids)
        logger.info(f"get_all_sample_ids_from_samples_table: Found {len(sorted_valid_ids)} valid (non-null, non-empty) unique sample_ids in samples table: {sorted_valid_ids}")
        return sorted_valid_ids
    except Exception as e:
        logger.error(f"Error in get_all_sample_ids_from_samples_table: {e}")
        return [] # Return an empty list on error
    finally:
        conn.close()


@st.cache_data
def get_all_data(db_file):
    conn = sqlite3.connect(db_file)
    try:
        query = '''
            SELECT 
                s.sample_id,
                s.project,
                s.subject,
                s.condition,
                s.age,
                s.sex,
                s.treatment,
                s.response,
                s.sample_type,
                s.time_from_treatment_start,
                c.population,
                c.count
            FROM samples s
            LEFT JOIN cell_counts c ON s.sample_id = c.sample_id
            ORDER BY s.sample_id, c.population;
        '''
        df = pd.read_sql_query(query, conn)
        logger.info(f"get_all_data: Fetched {len(df)} long-format rows from the database.")

        if not df.empty and 'population' in df.columns and 'count' in df.columns:
            sample_cols = ['sample_id', 'project', 'subject', 'condition', 'age', 'sex', 'treatment', 'response', 'sample_type', 'time_from_treatment_start']
            # Filter out any sample_cols not actually present in df
            sample_cols_present = [col for col in sample_cols if col in df.columns]

            if not sample_cols_present or 'sample_id' not in sample_cols_present:
                logger.error("Critical sample identifier columns missing in get_all_data before pivot.")
                return pd.DataFrame()

            # Fill NaN in index columns with a placeholder (e.g., empty string) before pivoting
            # This prevents rows with NaN in index columns from being dropped by pivot_table
            logger.debug(f"get_all_data: Columns to be used as index for pivot: {sample_cols_present}")
            for col in sample_cols_present:
                if df[col].isnull().any():
                    logger.debug(f"get_all_data: Filling NaNs in index column '{col}' with empty string before pivot.")
                    df[col] = df[col].fillna('') 

            df = df.pivot_table(index=sample_cols_present, columns='population', values='count').reset_index()
            
            # Now fillna(0) for the numeric cell count columns that were pivoted (and any other NaNs that might have appeared)
            df = df.fillna(0)
            
            expected_cell_cols = ['b_cell', 'cd4_t_cell', 'cd8_t_cell', 'monocyte', 'nk_cell']
            for col in expected_cell_cols:
                if col not in df.columns:
                    df[col] = 0.0 # Ensure float type
            logger.info(f"get_all_data: Pivoted data, {len(df)} wide-format rows.")
            return df
        elif df.empty:
            logger.info("get_all_data: No data returned from SQL query for pivoting.")
            return pd.DataFrame()
        else: # Not empty, but missing critical columns for pivot
            logger.warning("get_all_data: Data is missing population/count columns for pivot. Returning empty DataFrame.")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error fetching all data: {e}")
        return pd.DataFrame() # Return an empty DataFrame on error
    finally:
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
        query = '''
            SELECT s.sample_id, s.project, s.response, s.sex
            FROM samples s
            WHERE s.condition = 'melanoma' 
              AND s.sample_type = 'PBMC' 
              AND s.time_from_treatment_start = 0
        '''
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()

@st.cache_data
def get_data_for_custom_baseline_query(db_file):
    conn = sqlite3.connect(db_file)
    try:
        query = '''
            SELECT s.sample_id, s.project, s.response, s.sex
            FROM samples s
            WHERE s.condition = 'melanoma' 
              AND s.sample_type = 'PBMC' 
              AND s.time_from_treatment_start = 0
              AND s.treatment = 'tr1'
        '''
        df = pd.read_sql_query(query, conn)
        logger.info(f"get_data_for_custom_baseline_query: Fetched {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error in get_data_for_custom_baseline_query: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
