import streamlit as st
import os
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from uuid import uuid4

import database
import analysis
import utils

# Initialize database and load initial data if CSV exists
DB_FILE = 'cytometry.db'
CSV_FILE = 'cell-count.csv' # Ensure this file is in the same directory or provide full path

database.init_db(DB_FILE)
if os.path.exists(CSV_FILE):
    database.load_csv_to_db(DB_FILE, CSV_FILE)

# Streamlit UI
st.set_page_config(layout="wide") # Use wide layout
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
                            add_sample_data['sample_id'] = f"sample_{uuid4().hex[:8]}" # Shorter UUID
                            database.add_sample(DB_FILE, add_sample_data)
                            st.success('Sample added successfully!')
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Error adding sample: {str(e)}")

    with st.expander("➖ Remove Sample", expanded=False):
        if 'confirm_removal_sample_id' not in st.session_state:
            st.session_state.confirm_removal_sample_id = None

        # Fetch all sample IDs for the selectbox each time to ensure it's up-to-date
        all_sample_ids_for_removal = [''] + database.get_all_data(DB_FILE)['sample_id'].tolist()
        sample_id_to_remove = st.selectbox('Select Sample to Remove', 
                                           options=all_sample_ids_for_removal,
                                           format_func=lambda x: x if x else 'Select a sample',
                                           key='remove_sample_selectbox')

        if sample_id_to_remove and sample_id_to_remove != st.session_state.confirm_removal_sample_id:
             # If a new sample is selected, reset confirmation state for previous sample (if any)
            st.session_state.confirm_removal_sample_id = None 

        if sample_id_to_remove:
            if st.button(f"Request to Remove Sample: {sample_id_to_remove}", key=f"request_remove_btn_{sample_id_to_remove}"):
                st.session_state.confirm_removal_sample_id = sample_id_to_remove
                st.experimental_rerun() # Rerun to show confirmation buttons
        
        if st.session_state.confirm_removal_sample_id and st.session_state.confirm_removal_sample_id == sample_id_to_remove:
            st.warning(f"Are you sure you want to remove sample {st.session_state.confirm_removal_sample_id}? This action cannot be undone.")
            confirm_col1, confirm_col2 = st.columns(2)
            with confirm_col1:
                if st.button("Yes, Confirm Removal", key="confirm_remove_action_btn"):
                    try:
                        database.remove_sample(DB_FILE, st.session_state.confirm_removal_sample_id)
                        st.success(f'Sample {st.session_state.confirm_removal_sample_id} removed successfully!')
                        st.session_state.confirm_removal_sample_id = None # Reset confirmation
                        # To refresh the selectbox, we might need to clear its specific state if Streamlit caches it aggressively
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Error removing sample: {str(e)}")
                        st.session_state.confirm_removal_sample_id = None # Reset on error
            with confirm_col2:
                if st.button("Cancel Removal", key="cancel_remove_action_btn"):
                    st.session_state.confirm_removal_sample_id = None # Reset confirmation
                    st.experimental_rerun()

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
    ndf = database.get_all_data(DB_FILE)

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