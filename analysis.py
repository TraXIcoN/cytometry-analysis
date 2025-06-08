import pandas as pd
import scipy.stats as stats
import plotly.express as px
import plotly.graph_objects as go 
import db_layer as database

def calculate_frequency_table(db_file):
    df = database.get_data_for_frequency_table(db_file)
    if df.empty or 'count' not in df.columns or 'sample_id' not in df.columns or 'total_count' not in df.columns:
        return pd.DataFrame(columns=['sample_id', 'population', 'count', 'total_count', 'percentage'])
    
    # Total counts are now pre-calculated in the SQL query
    # Ensure 'total_count' is not zero to avoid division by zero
    df['percentage'] = df.apply(lambda row: round(row['count'] / row['total_count'] * 100, 2) if row['total_count'] > 0 else 0, axis=1)
    
    return df[['sample_id', 'population', 'count', 'total_count', 'percentage']]

def perform_treatment_response_analysis(db_file):
    df = database.get_data_for_treatment_response_analysis(db_file)
    if df.empty:
        return pd.DataFrame(), [], go.Figure()

    if 'count' not in df.columns or 'sample_id' not in df.columns or 'total_count' not in df.columns:
        return df, [], go.Figure() # Return original df if critical columns missing for processing

    # Total counts are now pre-calculated in the SQL query
    df['percentage'] = df.apply(lambda row: round(row['count'] / row['total_count'] * 100, 2) if row['total_count'] > 0 else 0, axis=1)
    
    results = []
    fig = go.Figure() # Default empty figure

    if 'population' in df.columns and 'response' in df.columns and 'percentage' in df.columns:
        for pop in df['population'].unique():
            resp = df[(df['response'] == 'y') & (df['population'] == pop)]['percentage']
            non_resp = df[(df['response'] == 'n') & (df['population'] == pop)]['percentage']
            
            if len(resp) >= 2 and len(non_resp) >= 2:
                t_stat, p_val = stats.ttest_ind(resp, non_resp, nan_policy='omit')
                results.append({
                    'population': pop,
                    't_statistic': round(t_stat, 3),
                    'p_value': round(p_val, 3),
                    'significant': p_val < 0.05
                })
            else:
                 results.append({
                    'population': pop,
                    't_statistic': 'N/A',
                    'p_value': 'N/A',
                    'significant': 'N/A' # Changed from False for clarity
                })
        
        if not df.empty:
             fig = px.box(df, x='population', y='percentage', color='response',
                     title='Cell Population Frequencies: Responders vs Non-Responders')
    
    return df, results, fig

def perform_baseline_analysis(db_file):
    df = database.get_data_for_baseline_analysis(db_file)
    if df.empty:
         return {
            'samples_per_project': {},
            'response_counts': {},
            'sex_counts': {},
            'total_samples': 0
        }

    results = {
        'samples_per_project': df.groupby('project').size().to_dict() if 'project' in df.columns else {},
        'response_counts': df['response'].value_counts().to_dict() if 'response' in df.columns else {},
        'sex_counts': df['sex'].value_counts().to_dict() if 'sex' in df.columns else {},
        'total_samples': len(df)
    }
    return results

def perform_custom_baseline_query_analysis(db_file):
    df = database.get_data_for_custom_baseline_query(db_file)
    if df.empty:
        return {
            'samples_per_project': {},
            'response_counts': {},
            'sex_counts': {},
            'total_samples': 0,
            'data_summary': pd.DataFrame() # Add an empty DataFrame for data summary
        }

    # Ensure we count unique subjects for response and sex, as one subject might have multiple samples
    # However, the request is "How many subjects were responders/non-responders" and "How many subjects were males/females"
    # For this specific query, sample_id is unique per subject at baseline for a given treatment, so len(df) is fine for total.
    # But for sub-categories, we should be careful if data structure changes. Assuming sample_id is granular enough for now.

    results = {
        'samples_per_project': df.groupby('project').size().to_dict() if 'project' in df.columns else {},
        'response_counts': df['response'].value_counts().to_dict() if 'response' in df.columns else {},
        'sex_counts': df['sex'].value_counts().to_dict() if 'sex' in df.columns else {},
        'total_samples': len(df),
        'data_summary': df # Include the raw (or lightly processed) data for display
    }
    return results
