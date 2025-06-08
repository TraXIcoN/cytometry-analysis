import streamlit as st
import logging
import os
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from uuid import uuid4

import database
import analysis
import utils

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
logger.info(f"Checking for database file: {DB_FILE}") # Ensure this file is in the same directory or provide full path

if not os.path.exists(DB_FILE):
    logger.info(f"Database file {DB_FILE} not found. Initializing and loading data.")
    database.init_db(DB_FILE) # Moved here
    database.load_csv_to_db(DB_FILE, CSV_FILE)
else:
    logger.info(f"Database file {DB_FILE} found. Skipping initialization.")

# Initialize all_samples_df in session state
if 'all_samples_df' not in st.session_state:
    st.session_state.all_samples_df = database.get_all_data(DB_FILE)

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
    st.header("Controls")

    with st.expander("🔍 Filter Data", expanded=True):
        selected_project = st.multiselect('Project', projects, default=[], key='filter_project')
        selected_condition = st.multiselect('Condition', conditions, default=[], key='filter_condition')
        selected_treatment = st.multiselect('Treatment', treatments_db, default=[], key='filter_treatment')
        selected_response = st.multiselect('Response', responses_options, default=[],
                                         format_func=lambda x: {'': 'All', 'y': 'Responder', 'n': 'Non-responder'}[x],
                                         key='filter_response')

    with st.expander("➕ Add New Sample", expanded=False):
        with st.form("add_sample_form", clear_on_submit=True):
            st.subheader("Sample Details")
            add_sample_data = {}
            # Row 1: Project & Treatment
            row1_col1, row1_col2 = st.columns(2)
            with row1_col1:
                current_projects_for_add = [''] + database.get_distinct_values(DB_FILE, 'project') + ['New Project']
                add_sample_data['project'] = st.selectbox('Project*', options=current_projects_for_add, key='add_project_select')
                if add_sample_data['project'] == 'New Project':
                    add_sample_data['project'] = st.text_input('Enter New Project Name', key='add_new_project_text')
            with row1_col2:
                current_treatments_for_add = [''] + database.get_distinct_values(DB_FILE, 'treatment') + ['New Treatment']
                add_sample_data['treatment'] = st.selectbox('Treatment', options=current_treatments_for_add, key='add_treatment_select')
                if add_sample_data['treatment'] == 'New Treatment':
                    add_sample_data['treatment'] = st.text_input('Enter New Treatment Name', key='add_new_treatment_text')

            # Row 2: Subject ID & Response
            row2_col1, row2_col2 = st.columns(2)
            with row2_col1:
                add_sample_data['subject'] = st.text_input('Subject ID*', key='add_subject')
            with row2_col2:
                add_sample_data['response'] = st.selectbox('Response', ['', 'y', 'n'],
                                                         format_func=lambda x: {'': 'None', 'y': 'Responder', 'n': 'Non-responder'}[x],
                                                         key='add_response_select')

            # Row 3: Condition & Sample Type
            row3_col1, row3_col2 = st.columns(2)
            with row3_col1:
                add_sample_data['condition'] = st.text_input('Condition*', value='Healthy', key='add_condition')
            with row3_col2:
                add_sample_data['sample_type'] = st.selectbox('Sample Type*', ['PBMC', 'Tumor', 'Blood', 'Tissue'], key='add_sample_type_select')

            # Row 4: Age & Time from Treatment
            row4_col1, row4_col2 = st.columns(2)
            with row4_col1:
                add_sample_data['age'] = st.number_input('Age*', min_value=0, max_value=120, value=30, key='add_age')
            with row4_col2:
                add_sample_data['time_from_treatment_start'] = st.number_input('Time from Treatment Start (days)', min_value=0, value=0, key='add_time_treatment')

            # Row 5: Sex (using first column of a 2-column layout for consistency)
            row5_col1, _ = st.columns(2) # Use _ if second column is intentionally empty
            with row5_col1:
                add_sample_data['sex'] = st.radio('Sex*', ['M', 'F'], horizontal=True, key='add_sex')

            st.subheader('Cell Counts')
            cell_cols_form = st.columns(5)
            cell_types = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
            add_sample_data['cell_counts'] = {}
            for i, cell_type in enumerate(cell_types):
                with cell_cols_form[i]:
                    add_sample_data['cell_counts'][cell_type] = st.number_input(cell_type.replace('_', ' ').title(), min_value=0, value=0, key=f'add_cell_{cell_type}')
            
            submit_add_sample = st.form_submit_button('Add Sample')
            if submit_add_sample:
                required_fields = ['project', 'subject', 'condition', 'age', 'sex', 'sample_type']
                # Handle 'New Project'/'New Treatment' potentially being empty if not filled
                if add_sample_data.get('project') == 'New Project' and not add_sample_data.get('project_new_text_input_value'): # Assuming you'd use a different key for the text input
                     st.error("Please enter a name for the new project.")
                elif add_sample_data.get('treatment') == 'New Treatment' and not add_sample_data.get('treatment_new_text_input_value'):
                     st.error("Please enter a name for the new treatment.")    
                else:
                    missing = [field for field in required_fields if not add_sample_data.get(field) or (isinstance(add_sample_data.get(field), str) and not add_sample_data.get(field).strip())]
                    if missing:
                        st.error(f"Please fill in all required fields: {', '.join(missing)}")
                    else:
                        try:
                            # Strip whitespace from user-defined project/treatment if they are strings
                            project_val = add_sample_data.get('project')
                            if isinstance(project_val, str):
                                add_sample_data['project'] = project_val.strip()

                            treatment_val = add_sample_data.get('treatment')
                            if isinstance(treatment_val, str):
                                add_sample_data['treatment'] = treatment_val.strip()

                            # Convert condition to lowercase
                            condition_val = add_sample_data.get('condition')
                            if isinstance(condition_val, str):
                                add_sample_data['condition'] = condition_val.strip().lower()

                            add_sample_data['sample_id'] = f"sample_{uuid4().hex[:8]}" # Shorter UUID
                            database.add_sample(DB_FILE, add_sample_data)
                            st.success('Sample added successfully!')
                            # Force reload of all_samples_df and update session state
                            database.get_all_data.clear() # Ensure next call to get_all_data is fresh
                            st.session_state.all_samples_df = database.get_all_data(DB_FILE)
                            # Clear other caches
                            database.get_distinct_values.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error adding sample: {str(e)}")

    with st.expander("➖ Remove Sample", expanded=False):
        if 'confirm_removal_sample_id' not in st.session_state:
            st.session_state.confirm_removal_sample_id = None
        if 'confirm_removal_sample_id_target' not in st.session_state:
            st.session_state.confirm_removal_sample_id_target = None

        # Fetch all sample IDs for the selectbox each time to ensure it's up-to-date
        if 'all_samples_df' in st.session_state and not st.session_state.all_samples_df.empty:
            all_sample_ids_for_removal = [''] + sorted(st.session_state.all_samples_df['sample_id'].unique().tolist())
        else:
            all_sample_ids_for_removal = [''] # Fallback if no samples or df not loaded
            st.caption("No samples available for removal or data not loaded.")

        sample_id_to_remove = st.selectbox('Select Sample to Remove', 
                                           options=all_sample_ids_for_removal,
                                           index=0, # Default to blank
                                           format_func=lambda x: x if x else "Select a sample...",
                                           key='remove_sample_select')

        # Reset confirmation if a different sample is selected or if blank is re-selected
        if sample_id_to_remove != st.session_state.get('confirm_removal_sample_id_target'):
            st.session_state.confirm_removal_sample_id = None 
            st.session_state.confirm_removal_sample_id_target = sample_id_to_remove

        if sample_id_to_remove: # Only show button if a sample is selected
            # Show 'Request to Remove' button if this sample isn't the one currently up for confirmation
            if st.session_state.confirm_removal_sample_id != sample_id_to_remove:
                 if st.button(f"Request to Remove: {sample_id_to_remove}", key=f"prepare_remove_{sample_id_to_remove}_btn"):
                    st.session_state.confirm_removal_sample_id = sample_id_to_remove
                    st.session_state.confirm_removal_sample_id_target = sample_id_to_remove # Store which sample is targeted
                    st.rerun()
        
        # If a sample is selected AND it's the one confirmed for removal, show warning and confirm/cancel buttons
        if st.session_state.confirm_removal_sample_id and st.session_state.confirm_removal_sample_id == sample_id_to_remove:
            st.warning(f"Are you sure you want to remove sample '{st.session_state.confirm_removal_sample_id}'? This cannot be undone easily without reverting to a checkpoint.")
            confirm_col1, confirm_col2 = st.columns(2)
            with confirm_col1:
                if st.button(f"Yes, Remove '{st.session_state.confirm_removal_sample_id}'", type="primary", key="confirm_remove_action_btn"):
                    try:
                        if database.remove_sample(DB_FILE, st.session_state.confirm_removal_sample_id):
                            st.success(f"Sample '{st.session_state.confirm_removal_sample_id}' removed.")
                            database.get_all_data.clear()
                            st.session_state.all_samples_df = database.get_all_data(DB_FILE)
                            database.get_distinct_values.clear()
                            st.session_state.confirm_removal_sample_id = None 
                            st.session_state.confirm_removal_sample_id_target = None
                            st.rerun()
                        else:
                            st.error(f"Failed to remove sample '{st.session_state.confirm_removal_sample_id}'.")
                            st.session_state.confirm_removal_sample_id = None 
                            st.session_state.confirm_removal_sample_id_target = None
                    except Exception as e:
                        st.error(f"Error removing sample: {str(e)}")
                        st.session_state.confirm_removal_sample_id = None 
                        st.session_state.confirm_removal_sample_id_target = None
            with confirm_col2:
                if st.button("Cancel Removal", key="cancel_remove_action_btn"):
                    st.session_state.confirm_removal_sample_id = None 
                    st.session_state.confirm_removal_sample_id_target = None # Also reset target on cancel
                    st.rerun()
        elif sample_id_to_remove and st.session_state.confirm_removal_sample_id and st.session_state.confirm_removal_sample_id != sample_id_to_remove:
            # This case: a sample (A) was set for confirmation, but user selected a different one (B) from dropdown.
            # The logic at the top (sample_id_to_remove != st.session_state.get('confirm_removal_sample_id_target'))
            # should have reset confirm_removal_sample_id, so this state might not be typically hit if that works perfectly.
            # However, it's a good place for a specific instruction if needed.
            st.caption(f"To remove '{sample_id_to_remove}', click 'Request to Remove: {sample_id_to_remove}'.")


    with st.expander("⬆️ Append Data from CSV", expanded=False):
        uploaded_csv_file = st.file_uploader("Upload a CSV file to append", type=['csv'], key='csv_append_uploader')
        
        if uploaded_csv_file is not None:
            if st.button("Append Data from CSV", key='append_csv_data_btn'):
                with st.spinner("Processing and appending data..."):
                    try:
                        # The uploaded_csv_file object is directly usable by pd.read_csv
                        success, details = database.append_csv_to_db(DB_FILE, uploaded_csv_file)
                        
                        if success:
                            msg = (f"Data append process finished.\n"
                                   f"- Rows Processed: {details.get('rows_processed', 0)}\n"
                                   f"- New Samples Added: {details.get('samples_added', 0)}\n"
                                   f"- Existing Samples Skipped: {details.get('samples_skipped_existing', 0)}\n"
                                   f"- Cell Counts Added: {details.get('cell_counts_added', 0)}\n"
                                   f"- Rows with Errors/Skipped: {details.get('rows_with_errors', 0)}")
                            st.success(msg)
                            
                            # Clear caches and rerun
                            database.get_all_data.clear()
                            st.session_state.all_samples_df = database.get_all_data(DB_FILE)
                            database.get_distinct_values.clear()
                            # If you have analysis functions that cache results based on all_samples_df, clear them too.
                            # e.g., analysis.calculate_frequency_table.clear() # Assuming such a cache exists
                            st.rerun()
                        else:
                            st.error(f"Failed to append data: {details.get('error', 'Unknown error')}")
                    except Exception as e:
                        logger.error(f"Error during CSV append UI operation: {e}")
                        st.error(f"An unexpected error occurred: {e}")
                # Attempt to clear the uploader after processing by resetting its key or through a form
                # A simple way is to ensure the key is unique or changes, or rely on rerun to refresh its state.
                # For a more robust clear, you might need to wrap it in a form or use a more complex session state trick.
                # st.session_state.csv_append_uploader = None # This might not always work as expected with st.file_uploader

    # --- History & Checkpoints Expander ---
    with st.expander("📜 History & Checkpoints", expanded=False):
        st.subheader("Recent Operations")
        try:
            op_log_df = database.get_operation_log(DB_FILE, limit=10) 
            if op_log_df is not None and not op_log_df.empty:
                op_log_df_display = op_log_df.copy()
                if 'timestamp' in op_log_df_display.columns:
                    try:
                        op_log_df_display['timestamp'] = pd.to_datetime(op_log_df_display['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception as e:
                        logger.warning(f"Could not format timestamp for display: {e}")
                if 'details' in op_log_df_display.columns:
                    op_log_df_display['details'] = op_log_df_display['details'].apply(lambda x: str(x) if isinstance(x, dict) else x)
                st.dataframe(op_log_df_display, height=200, use_container_width=True)
            else:
                st.caption("No operations logged yet.")
        except Exception as e:
            st.error(f"Error loading operation log: {e}")
            logger.error(f"Error loading operation log in UI: {e}")

        st.subheader("Database Checkpoints")
        if st.button("Create New Checkpoint", key='create_checkpoint_btn'):
            checkpoint_path = database.create_db_checkpoint(DB_FILE)
            if checkpoint_path:
                st.success(f"Checkpoint created: {os.path.basename(checkpoint_path)}")
                st.rerun() 
            else:
                st.error("Failed to create checkpoint.")
        
        list_of_checkpoints = database.list_db_checkpoints(DB_FILE)

        if list_of_checkpoints:
            checkpoint_files = sorted([os.path.basename(p) for p in list_of_checkpoints], reverse=True) # Show newest first
            selected_checkpoint_file = st.selectbox("Select Checkpoint to Revert To", 
                                               options=[''] + checkpoint_files, 
                                               format_func=lambda x: x if x else "Select a checkpoint...",
                                               key='select_checkpoint_revert')
            
            if selected_checkpoint_file:
                selected_checkpoint_full_path = next((p for p in list_of_checkpoints if os.path.basename(p) == selected_checkpoint_file), None)

                if selected_checkpoint_full_path:
                    # Initialize session state for revert confirmation
                    if 'confirm_revert_target' not in st.session_state:
                        st.session_state.confirm_revert_target = None
                    if 'show_revert_confirmation' not in st.session_state:
                        st.session_state.show_revert_confirmation = False

                    # If selection changed, reset confirmation
                    if st.session_state.confirm_revert_target != selected_checkpoint_full_path:
                        st.session_state.show_revert_confirmation = False
                        st.session_state.confirm_revert_target = selected_checkpoint_full_path

                    if not st.session_state.show_revert_confirmation:
                        if st.button("Prepare to Revert to this Checkpoint", key='prepare_revert_btn'):
                            st.session_state.show_revert_confirmation = True
                            st.session_state.confirm_revert_target = selected_checkpoint_full_path # Ensure target is set
                            st.rerun()
                    else: # Show confirmation dialog
                        st.warning(f"Are you sure you want to revert to checkpoint '{selected_checkpoint_file}'? This will replace the current database and cannot be undone easily.")
                        revert_confirm_col1, revert_confirm_col2 = st.columns(2)
                        with revert_confirm_col1:
                            if st.button("Yes, Confirm Revert", type="primary", key="confirm_revert_action"):
                                if database.revert_to_db_checkpoint(DB_FILE, selected_checkpoint_full_path):
                                    st.success(f"Successfully reverted to {selected_checkpoint_file}. App is refreshing.")
                                    st.cache_data.clear()
                                    st.cache_resource.clear()
                                    database.get_all_data.clear() 
                                    st.session_state.all_samples_df = database.get_all_data(DB_FILE)
                                    database.get_distinct_values.clear()
                                    # Reset revert confirmation state
                                    st.session_state.show_revert_confirmation = False
                                    st.session_state.confirm_revert_target = None
                                    st.rerun()
                                else:
                                    st.error(f"Failed to revert to {selected_checkpoint_file}.")
                                    st.session_state.show_revert_confirmation = False # Reset on error
                        with revert_confirm_col2:
                            if st.button("Cancel Revert", key="cancel_revert_action"):
                                st.session_state.show_revert_confirmation = False
                                # st.session_state.confirm_revert_target = None # Keep target or clear? Clear for now.
                                st.rerun()
        else:
            st.caption("No checkpoints available yet.")

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

    st.subheader('Sample Data')
    st.dataframe(display_df)

    if show_filtered_only and not display_df.empty:
        st.markdown(utils.get_table_download_link(display_df, f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"),
                    unsafe_allow_html=True)
    elif not show_filtered_only and not display_df.empty:
         st.markdown(utils.get_table_download_link(display_df, f"all_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"),
                    unsafe_allow_html=True)

    st.header('Summary Statistics')
    if not ndf.empty:
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        with summary_col1:
            st.metric("Total Samples (in selection)", len(ndf))
            st.metric("Unique Subjects (in selection)", ndf['subject'].nunique())
        with summary_col2:
            st.metric("Average Age (in selection)", f"{ndf['age'].mean():.1f} years" if not ndf['age'].empty else "N/A")
            st.metric("Gender Distribution (in selection)", f"{ndf['sex'].value_counts().to_dict()}" if not ndf['sex'].empty else "N/A")
        with summary_col3:
            if 'response' in ndf.columns and not ndf['response'].empty:
                response_counts = ndf['response'].value_counts()
                st.metric("Response Rate (in selection)", 
                         f"{response_counts.get('y', 0) / len(ndf) * 100:.1f}%" if len(ndf) > 0 else "N/A")
            else:
                st.metric("Response Rate (in selection)", "N/A")
    else:
        st.info("No data in current selection for summary statistics.")

    st.header('Cell Population Analysis')
    # Use ndf for analysis plots, as it reflects the user's filter selection
    if not ndf.empty:
        cell_cols_plot = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
        plot_type = st.selectbox('Select Plot Type', ['Bar Chart', 'Box Plot', 'Violin Plot', 'Scatter Plot'], key='plot_type_select')
        group_by_options = ['None'] + [col for col in ['project', 'condition', 'treatment', 'response', 'sex', 'sample_type'] if col in ndf.columns and ndf[col].nunique() > 0]
        group_by = st.selectbox('Group By', group_by_options, key='plot_group_by_select')

        plot_df_melted = ndf.melt(
            id_vars=[col for col in ndf.columns if col not in cell_cols_plot],
            value_vars=cell_cols_plot,
            var_name='Cell Type',
            value_name='Count'
        )

        fig = None
        title_suffix = f"by {group_by.capitalize() if group_by != 'None' else 'Cell Type'}"
        if plot_type == 'Bar Chart':
            fig = px.bar(plot_df_melted, x=group_by if group_by != 'None' else 'Cell Type', y='Count', 
                         color='Cell Type' if group_by == 'None' else group_by, 
                         barmode='group' if group_by != 'None' else 'relative',
                         title=f'Cell Counts {title_suffix}', color_discrete_sequence=px.colors.qualitative.Plotly)
        elif plot_type == 'Box Plot':
            fig = px.box(plot_df_melted, x=group_by if group_by != 'None' else 'Cell Type', y='Count', 
                         color=group_by if group_by != 'None' else 'Cell Type', 
                         title=f'Distribution of Cell Counts {title_suffix}', color_discrete_sequence=px.colors.qualitative.Plotly)
        elif plot_type == 'Violin Plot':
            fig = px.violin(plot_df_melted, x=group_by if group_by != 'None' else 'Cell Type', y='Count', 
                           color=group_by if group_by != 'None' else 'Cell Type', box=True, 
                           title=f'Distribution of Cell Counts {title_suffix}', color_discrete_sequence=px.colors.qualitative.Plotly)
        elif plot_type == 'Scatter Plot' and 'age' in ndf.columns:
            fig = px.scatter(plot_df_melted, x='age', y='Count', 
                             color=group_by if group_by != 'None' else 'Cell Type', size='Count', 
                             hover_data=['subject', 'sample_type'], title=f'Cell Counts by Age {title_suffix}', 
                             color_discrete_sequence=px.colors.qualitative.Plotly)
        elif plot_type == 'Scatter Plot':
            st.warning("'age' column not available or selected for scatter plot grouping.")

        if fig:
            fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
                              xaxis_title=group_by.capitalize() if group_by != 'None' else 'Cell Type',
                              yaxis_title='Cell Count', legend_title_text='', height=600)
            st.plotly_chart(fig, use_container_width=True)
        elif plot_type == 'Scatter Plot' and 'age' not in ndf.columns:
            pass # Warning already shown
        else:
            st.info("Select plot type and grouping options.")
    else:
        st.info('No data in current selection for cell population analysis.')

    # Frequency Table Analysis - uses all data by default from analysis.py
    st.header('Frequency Table Analysis (All Data)')
    freq_table = analysis.calculate_frequency_table(DB_FILE)
    st.dataframe(freq_table)
    if not freq_table.empty:
        st.markdown(utils.get_table_download_link(freq_table, "frequency_table_all_data.csv"), unsafe_allow_html=True)

    # Treatment Response Analysis - uses specific filtered data from analysis.py
    st.header('Treatment Response Analysis (Melanoma, TR1, PBMC)')
    treatment_df, treatment_results, treatment_fig = analysis.perform_treatment_response_analysis(DB_FILE)
    if not treatment_df.empty:
        st.subheader('Data')
        st.dataframe(treatment_df)
        st.subheader('Statistical Results')
        st.dataframe(pd.DataFrame(treatment_results))
        st.subheader('Plot')
        st.plotly_chart(treatment_fig, use_container_width=True)
    else:
        st.info('No data available for standard treatment response analysis (melanoma, tr1, PBMC).')

    # Baseline Characteristics Analysis - uses all data by default from analysis.py
    st.header('Baseline Characteristics Analysis (All Data)')
    baseline_results = analysis.perform_baseline_analysis(DB_FILE)
    st.json(baseline_results)