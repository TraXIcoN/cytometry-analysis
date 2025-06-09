import streamlit as st
import logging
import os
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from uuid import uuid4

import db_layer as database
from reporting_tools import analysis
from reporting_tools import utils
from ui_modules.app_helpers import initialize_app, initialize_session_state
from ui_modules.left_column import render_left_column_controls
from ui_modules.right_column_tabs import render_viewer_summary_tab, render_cell_population_plots_tab, render_frequency_table_tab, render_treatment_response_tab, render_baseline_characteristics_tab, render_custom_baseline_query_tab

st.set_page_config(layout="wide", page_title="Cytometry Data Analysis")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True # Ensure our config takes precedence if Streamlit also configures logging
)
logger = logging.getLogger(__name__)


# Initialize database and load initial data if CSV exists
DB_FILE = 'cytometry.db'
CSV_FILE = 'cell-count.csv'

# Initialize application and session state
initialize_app(DB_FILE, CSV_FILE)
initialize_session_state(DB_FILE)

# Streamlit UI
st.title('Cytometry Data Analysis')

# --- Data Fetching for Filters (runs early) ---
projects = database.get_distinct_values(DB_FILE, 'project')
conditions = database.get_distinct_values(DB_FILE, 'condition')
treatments_db = database.get_distinct_values(DB_FILE, 'treatment')
responses_options = ['y', 'n', '']
all_db_samples_ids = database.get_all_data(DB_FILE)['sample_id'].tolist()





# --- Main Layout: Two Columns ---
left_column, right_column = st.columns([1, 3]) # Adjust ratio as needed

with left_column:
    # Call the function to render filters and get selections
    # projects, conditions, treatments_db, responses_options are fetched globally
    selected_filters = render_left_column_controls(DB_FILE, projects, conditions, treatments_db, responses_options)



                # For a more robust clear, you might need to wrap it in a form or use a more complex session state trick.
                # st.session_state.csv_append_uploader = None # This might not always work as expected with st.file_uploader

    # ... (rest of the code remains the same)

# ... (rest of the code remains the same)

selected_project = selected_filters.get('project')
selected_condition = selected_filters.get('condition')
selected_treatment = selected_filters.get('treatment')
selected_response = selected_filters.get('response')

# --- Data Processing based on filters (after left_column is defined) ---
if any([selected_project, selected_condition, selected_treatment, selected_response]):
    ndf = database.get_filtered_data(
        DB_FILE,
        selected_project=selected_project,
        selected_condition=selected_condition,
        selected_treatment=selected_treatment,
        selected_response=selected_response
    )
else:
    ndf = st.session_state.all_samples_df

# --- Right Column: Data Display and Analysis ---
with right_column:
    st.header("Data Viewer & Analysis")
    show_filtered_only = st.checkbox('Show Filtered Data Only', value=True, key='show_filtered_checkbox')

    if show_filtered_only:
        display_df = ndf
    else:
        display_df = database.get_all_data(DB_FILE)

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Viewer & Summary", 
        "Cell Population Plots", 
        "Frequency Table", 
        "Treatment Response", 
        "Baseline Characteristics", 
        "Custom Baseline Query"
    ])

    with tab1:
        render_viewer_summary_tab(display_df, ndf, show_filtered_only)

    with tab2:
        render_cell_population_plots_tab(ndf)

    with tab3:
        render_frequency_table_tab(DB_FILE)

    with tab4:
        render_treatment_response_tab(DB_FILE)

    with tab5:
        render_baseline_characteristics_tab(DB_FILE)

    with tab6:
        render_custom_baseline_query_tab(DB_FILE)