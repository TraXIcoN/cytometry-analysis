import streamlit as st
import pandas as pd
from uuid import uuid4
import db_layer as database
import os
import logging
from ui_modules.app_helpers import initialize_session_state

logger = logging.getLogger(__name__)

def render_left_column_controls(db_file, projects_options, conditions_options, treatments_options, response_filter_options):
    st.header("Controls")
    selected_filters_dict = {}
    with st.expander("🔍 Filter Data", expanded=True):
        selected_filters_dict['project'] = st.multiselect('Project', projects_options, default=[], key='filter_project')
        selected_filters_dict['condition'] = st.multiselect('Condition', conditions_options, default=[], key='filter_condition')
        selected_filters_dict['treatment'] = st.multiselect('Treatment', treatments_options, default=[], key='filter_treatment')
        selected_filters_dict['response'] = st.multiselect('Response', response_filter_options, default=[],
                                         format_func=lambda x: {'': 'All', 'y': 'Responder', 'n': 'Non-responder'}[x],
                                         key='filter_response')
    with st.expander("➕ Add New Sample", expanded=False):
        with st.form("add_sample_form", clear_on_submit=True):
            st.subheader("Sample Details")
            add_sample_data = {}
            # Row 1: Project & Treatment
            row1_col1, row1_col2 = st.columns(2)
            with row1_col1:
                current_projects_for_add = [''] + database.get_distinct_values(db_file, 'project') + ['New Project']
                add_sample_data['project'] = st.selectbox('Project*', options=current_projects_for_add, key='add_project_select')
                if add_sample_data['project'] == 'New Project':
                    add_sample_data['project'] = st.text_input('Enter New Project Name', key='add_new_project_text')
            with row1_col2:
                current_treatments_for_add = [''] + database.get_distinct_values(db_file, 'treatment') + ['New Treatment']
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
            for i, cell_type_val in enumerate(cell_types):
                with cell_cols_form[i]:
                    add_sample_data['cell_counts'][cell_type_val] = st.number_input(cell_type_val.replace('_', ' ').title(), min_value=0, value=0, key=f'add_cell_{cell_type_val}')
            
            submit_add_sample = st.form_submit_button('Add Sample')
            if submit_add_sample:
                required_fields = ['project', 'subject', 'condition', 'age', 'sex', 'sample_type']
                actual_project_name = add_sample_data.get('project')
                if add_sample_data.get('project') == 'New Project':
                    actual_project_name = st.session_state.get('add_new_project_text', '') 
                
                actual_treatment_name = add_sample_data.get('treatment')
                if add_sample_data.get('treatment') == 'New Treatment':
                    actual_treatment_name = st.session_state.get('add_new_treatment_text', '')

                if add_sample_data.get('project') == 'New Project' and not actual_project_name.strip():
                     st.error("Please enter a name for the new project.")
                elif add_sample_data.get('treatment') == 'New Treatment' and not actual_treatment_name.strip():
                     st.error("Please enter a name for the new treatment.")    
                else:
                    add_sample_data_final = add_sample_data.copy()
                    add_sample_data_final['project'] = actual_project_name
                    add_sample_data_final['treatment'] = actual_treatment_name

                    missing = [field for field in required_fields if not add_sample_data_final.get(field) or (isinstance(add_sample_data_final.get(field), str) and not add_sample_data_final.get(field).strip())]
                    if missing:
                        st.error(f"Please fill in all required fields: {', '.join(missing)}")
                    else:
                        try:
                            project_val = add_sample_data_final.get('project')
                            if isinstance(project_val, str):
                                add_sample_data_final['project'] = project_val.strip()

                            treatment_val = add_sample_data_final.get('treatment')
                            if isinstance(treatment_val, str):
                                add_sample_data_final['treatment'] = treatment_val.strip()

                            condition_val = add_sample_data_final.get('condition')
                            if isinstance(condition_val, str):
                                add_sample_data_final['condition'] = condition_val.strip().lower()

                            add_sample_data_final['sample_id'] = f"sample_{uuid4().hex[:8]}"
                            database.add_sample(db_file, add_sample_data_final)
                            st.success('Sample added successfully!')
                            database.get_all_data.clear() 
                            st.session_state.all_samples_df = database.get_all_data(db_file)
                            database.get_distinct_values.clear()
                            initialize_session_state(db_file) 
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error adding sample: {str(e)}")
    with st.expander("➖ Remove Sample", expanded=False):
        if 'confirm_removal_sample_id' not in st.session_state:
            st.session_state.confirm_removal_sample_id = None
        if 'confirm_removal_sample_id_target' not in st.session_state:
            st.session_state.confirm_removal_sample_id_target = None

        if 'all_samples_df' in st.session_state and not st.session_state.all_samples_df.empty:
            all_sample_ids_for_removal = [''] + sorted(st.session_state.all_samples_df['sample_id'].unique().tolist())
        else:
            all_sample_ids_for_removal = [''] 
            st.caption("No samples available for removal or data not loaded.")

        sample_id_to_remove = st.selectbox('Select Sample to Remove', 
                                           options=all_sample_ids_for_removal,
                                           index=0, 
                                           format_func=lambda x: x if x else "Select a sample...",
                                           key='remove_sample_select')

        if sample_id_to_remove != st.session_state.get('confirm_removal_sample_id_target'):
            st.session_state.confirm_removal_sample_id = None 
            st.session_state.confirm_removal_sample_id_target = sample_id_to_remove
        
        if sample_id_to_remove: 
            if st.session_state.confirm_removal_sample_id != sample_id_to_remove:
                 if st.button(f"Request to Remove: {sample_id_to_remove}", key=f"prepare_remove_{sample_id_to_remove}_btn"):
                    st.session_state.confirm_removal_sample_id = sample_id_to_remove
                    st.session_state.confirm_removal_sample_id_target = sample_id_to_remove 
                    st.rerun()
        
        if st.session_state.confirm_removal_sample_id and st.session_state.confirm_removal_sample_id == sample_id_to_remove:
            st.warning(f"Are you sure you want to remove sample '{st.session_state.confirm_removal_sample_id}'? This cannot be undone easily without reverting to a checkpoint.")
            confirm_col1, confirm_col2 = st.columns(2)
            with confirm_col1:
                if st.button(f"Yes, Remove '{st.session_state.confirm_removal_sample_id}'", type="primary", key="confirm_remove_action_btn"):
                    try:
                        if database.remove_sample(db_file, st.session_state.confirm_removal_sample_id):
                            st.success(f"Sample '{st.session_state.confirm_removal_sample_id}' removed.")
                            database.get_all_data.clear()
                            st.session_state.all_samples_df = database.get_all_data(db_file)
                            database.get_distinct_values.clear()
                            initialize_session_state(db_file) 
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
                    st.session_state.confirm_removal_sample_id_target = None 
                    st.rerun()
        elif sample_id_to_remove and st.session_state.confirm_removal_sample_id and st.session_state.confirm_removal_sample_id != sample_id_to_remove:
            st.caption(f"To remove '{sample_id_to_remove}', click 'Request to Remove: {sample_id_to_remove}'.")

    with st.expander("⬆️ Append Data from CSV", expanded=False):
        uploaded_csv_file = st.file_uploader("Upload a CSV file to append", type=['csv'], key='csv_append_uploader')
        
        if uploaded_csv_file is not None:
            if st.button("Append Data from CSV", key='append_csv_data_btn'):
                with st.spinner("Processing and appending data..."):
                    try:
                        success, details = database.append_csv_to_db(db_file, uploaded_csv_file)
                        
                        if success:
                            msg = (f"Data append process finished.\n"
                                   f"Rows processed: {details.get('rows_processed', 'N/A')}\n"
                                   f"Rows successfully appended: {details.get('rows_appended', 'N/A')}\n"
                                   f"Rows with errors: {details.get('rows_with_errors', 'N/A')}\n"
                                   f"Rows with missing required columns: {details.get('rows_missing_required_cols', 'N/A')}\n"
                                   f"Rows with type conversion errors: {details.get('rows_type_conversion_error', 'N/A')}\n"
                                   f"Duplicate rows skipped: {details.get('duplicate_rows_skipped', 'N/A')}")
                            st.success(msg)
                            if details.get('error_details') and details['error_details'].get('errors'):
                                st.warning("Some rows had issues. See error log for details.")
                                logger.error(f"Detailed errors during CSV append: {details['error_details']}")

                            database.get_all_data.clear()
                            st.session_state.all_samples_df = database.get_all_data(db_file)
                            database.get_distinct_values.clear()
                            initialize_session_state(db_file)
                            st.rerun()
                        else:
                            st.error(f"Failed to append data: {details.get('error', 'Unknown error')}")
                            if details.get('error_details') and details['error_details'].get('errors'):
                                logger.error(f"Detailed errors during CSV append: {details['error_details']}")
                    except Exception as e:
                        st.error(f"An unexpected error occurred during CSV append: {str(e)}")
                        logger.exception("Critical error during CSV append UI operation")

    with st.expander("📜 History & Checkpoints", expanded=False):
        st.subheader("Recent Operations")
        try:
            op_log_df = database.get_operation_log(db_file, limit=10)
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
            checkpoint_path = database.create_db_checkpoint(db_file)
            if checkpoint_path:
                st.success(f"Checkpoint created: {os.path.basename(checkpoint_path)}")
                st.rerun()
            else:
                st.error("Failed to create checkpoint.")
        
        list_of_checkpoints = database.list_db_checkpoints(db_file)

        if list_of_checkpoints:
            checkpoint_files = sorted([os.path.basename(p) for p in list_of_checkpoints], reverse=True)
            selected_checkpoint_file = st.selectbox("Select Checkpoint to Revert To", 
                                               options=[''] + checkpoint_files, 
                                               format_func=lambda x: x if x else "Select a checkpoint...",
                                               key='select_checkpoint_revert')
            
            if selected_checkpoint_file:
                if 'show_revert_confirmation' not in st.session_state:
                    st.session_state.show_revert_confirmation = False
                
                if st.button(f"Revert to {selected_checkpoint_file}", key='revert_checkpoint_confirm_request_btn'):
                    st.session_state.show_revert_confirmation = True
                    st.session_state.selected_checkpoint_to_revert = selected_checkpoint_file
                    st.rerun()

                if st.session_state.show_revert_confirmation and \
                   st.session_state.get('selected_checkpoint_to_revert') == selected_checkpoint_file:
                    st.warning(f"Are you sure you want to revert the database to checkpoint '{selected_checkpoint_file}'? "
                               "This will overwrite current data. This action cannot be easily undone.")
                    revert_cols = st.columns(2)
                    with revert_cols[0]:
                        if st.button("Yes, Revert Database", type="primary", key='revert_db_confirmed_btn'):
                            full_checkpoint_path = os.path.join(os.path.dirname(db_file), "checkpoints", selected_checkpoint_file)
                            if database.revert_db_to_checkpoint(db_file, full_checkpoint_path):
                                st.success(f"Database reverted to {selected_checkpoint_file}.")
                                database.get_all_data.clear()
                                database.get_distinct_values.clear()
                                initialize_session_state(db_file)
                                st.session_state.show_revert_confirmation = False
                                st.session_state.selected_checkpoint_to_revert = None
                                st.rerun()
                            else:
                                st.error("Failed to revert database.")
                                st.session_state.show_revert_confirmation = False
                                st.session_state.selected_checkpoint_to_revert = None
                    with revert_cols[1]:
                        if st.button("Cancel Revert", key='cancel_revert_db_btn'):
                            st.session_state.show_revert_confirmation = False
                            st.session_state.selected_checkpoint_to_revert = None
                            st.rerun()
        else:
            st.caption("No checkpoints available.")
            
    return selected_filters_dict
