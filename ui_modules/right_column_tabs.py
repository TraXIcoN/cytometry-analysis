import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
from reporting_tools import utils
from reporting_tools import analysis
from reporting_tools import reporting # Added for PDF reporting
import plotly.express as px # Ensure px is available for potential plot generation for PDF

def render_viewer_summary_tab(display_df, ndf, show_filtered_only):
    st.subheader('Sample Data')
    st.dataframe(display_df)

    if show_filtered_only and not display_df.empty:
        col1, col2 = st.columns(2, gap="small")
        with col1:
            csv_bytes = utils.df_to_csv_bytes(display_df)
            st.download_button(
                label="Download CSV (Filtered)",
                data=csv_bytes,
                file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="csv_filtered_viewer"
            )
        with col2:
            excel_bytes = utils.df_to_excel_bytes(display_df)
            st.download_button(
                label="Download Excel (Filtered)",
                data=excel_bytes,
                file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_filtered_viewer"
            )
    elif not show_filtered_only and not display_df.empty:
        col1, col2 = st.columns(2, gap="small")
        with col1:
            csv_bytes = utils.df_to_csv_bytes(display_df)
            st.download_button(
                label="Download CSV (All)",
                data=csv_bytes,
                file_name=f"all_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="csv_all_viewer"
            )
        with col2:
            excel_bytes = utils.df_to_excel_bytes(display_df)
            st.download_button(
                label="Download Excel (All)",
                data=excel_bytes,
                file_name=f"all_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="excel_all_viewer"
            )

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

    # PDF Report Download Button
    st.markdown("---") # Add a separator
    st.subheader("Download Report")
    if st.button("Generate PDF Report"): 
        with st.spinner("Generating PDF report..."):
            # Prepare data for the report
            report_stats = {}
            if not ndf.empty:
                report_stats["Total Samples (in selection)"] = len(ndf)
                report_stats["Unique Subjects (in selection)"] = ndf['subject'].nunique()
                report_stats["Average Age (in selection)"] = f"{ndf['age'].mean():.1f} years" if not ndf['age'].empty else "N/A"
                report_stats["Gender Distribution (in selection)"] = f"{ndf['sex'].value_counts().to_dict()}" if not ndf['sex'].empty else "N/A"
                if 'response' in ndf.columns and not ndf['response'].empty:
                    response_counts = ndf['response'].value_counts()
                    report_stats["Response Rate (in selection)"] = f"{response_counts.get('y', 0) / len(ndf) * 100:.1f}%" if len(ndf) > 0 else "N/A"
                else:
                    report_stats["Response Rate (in selection)"] = "N/A"
            else:
                report_stats["Summary"] = "No data in current selection for summary statistics."

            # For plots, let's try to generate a simple one for demonstration
            # This could be expanded to include plots from other tabs
            report_plots = []
            if not display_df.empty and 'age' in display_df.columns and display_df['age'].nunique() > 1:
                try:
                    fig_age_dist = px.histogram(display_df, x='age', title='Age Distribution (in displayed data)')
                    report_plots.append(fig_age_dist)
                except Exception as e:
                    st.error(f"Error generating plot for PDF: {e}")
            
            pdf_bytes = reporting.generate_pdf_report(
                summary_df=display_df, 
                plots=report_plots, 
                stats=report_stats
            )
            st.download_button(
                label="Download PDF Report",
                data=pdf_bytes,
                file_name=f"cytometry_summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                mime="application/pdf"
            )
            st.success("PDF report generated! Click the button above to download.")

def render_cell_population_plots_tab(ndf):
    st.header('Cell Population Analysis')
    if not ndf.empty:
        cell_cols_plot = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']
        plot_type = st.selectbox('Select Plot Type', ['Bar Chart', 'Box Plot', 'Violin Plot', 'Scatter Plot'], key='plot_type_select_tab2')
        group_by_options = ['None'] + [col for col in ['project', 'condition', 'treatment', 'response', 'sex', 'sample_type'] if col in ndf.columns and ndf[col].nunique() > 0]
        group_by = st.selectbox('Group By', group_by_options, key='plot_group_by_select_tab2')

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
            pass 
        else:
            st.info("Select plot type and grouping options.")
    else:
        st.info('No data in current selection for cell population analysis.')

def render_frequency_table_tab(db_file):
    st.header('Frequency Table Analysis (All Data)')
    freq_table_df = analysis.calculate_frequency_table(db_file)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader('Population Distribution')
        pie_fig = px.pie(freq_table_df, names='population', values='count', title='Population Distribution')
        st.plotly_chart(pie_fig, use_container_width=True)
    with col2:
        if not freq_table_df.empty:
            st.dataframe(freq_table_df)
            csv_bytes = utils.df_to_csv_bytes(freq_table_df)
            st.download_button(
                label="Download CSV",
                data=csv_bytes,
                file_name=f"frequency_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="freq_csv"
            )
            excel_bytes = utils.df_to_excel_bytes(freq_table_df)
            st.download_button(
                label="Download Excel",
                data=excel_bytes,
                file_name=f"frequency_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="freq_excel"
            )
    if freq_table_df.empty:
        st.info("No data available for frequency table.")

def render_treatment_response_tab(db_file):
    st.header('Treatment Response Analysis (Melanoma, TR1, PBMC)')
    treatment_df, treatment_results, treatment_fig = analysis.perform_treatment_response_analysis(db_file)
    if not treatment_df.empty:
        st.subheader('Data')
        st.dataframe(treatment_df)
        st.subheader('Statistical Results')
        st.dataframe(pd.DataFrame(treatment_results))
        st.subheader('Plot')
        st.plotly_chart(treatment_fig, use_container_width=True)

        st.subheader('Summary Report')
        if treatment_results:
            significant_findings = []
            non_significant_findings = []
            not_calculable = []
            for res in treatment_results:
                if res['significant'] == True:
                    significant_findings.append(f"- **{res['population']}**: Shows a statistically significant difference in relative frequencies between responders and non-responders (p-value: {res['p_value']}).")
                elif res['significant'] == False:
                    non_significant_findings.append(f"- **{res['population']}**: Does not show a statistically significant difference (p-value: {res['p_value']}).")
                else: 
                    not_calculable.append(f"- **{res['population']}**: Significance could not be determined (insufficient data for t-test).")
            
            if significant_findings:
                st.markdown("**Significant Differences Observed:**")
                for finding in significant_findings:
                    st.markdown(finding)
            else:
                st.markdown("No cell populations showed a statistically significant difference in this dataset.")
            
            if non_significant_findings:
                st.markdown("\n**No Significant Differences Observed:**")
                for finding in non_significant_findings:
                    st.markdown(finding)

            if not_calculable:
                st.markdown("\n**Could Not Calculate Significance:**")
                for finding in not_calculable:
                    st.markdown(finding)
        else:
            st.markdown("Statistical analysis results are not available to generate a report.")
    else:
        st.info('No data available for standard treatment response analysis (melanoma, tr1, PBMC).')

def render_baseline_characteristics_tab(db_file):
    st.header('Baseline Characteristics Analysis (All Data)')
    baseline_results = analysis.perform_baseline_analysis(db_file)
    if baseline_results and baseline_results['total_samples'] > 0:
        st.write(f"Total Samples (All Data, Baseline): {baseline_results['total_samples']}")
        st.write("Samples per project:", baseline_results['samples_per_project'])
        st.write("Response counts:", baseline_results['response_counts'])
        st.write("Sex counts:", baseline_results['sex_counts'])
    else:
        st.info("No data for general baseline characteristics analysis.")

def render_custom_baseline_query_tab(db_file):
    st.header('Custom Baseline Query: Melanoma PBMC, TR1, Baseline')
    custom_baseline_data = analysis.perform_custom_baseline_query_analysis(db_file)
    if custom_baseline_data and custom_baseline_data['total_samples'] > 0:
        st.write(f"Total Samples Found: {custom_baseline_data['total_samples']}")
        
        st.subheader("Samples per Project")
        if custom_baseline_data['samples_per_project']:
            st.table(pd.DataFrame(list(custom_baseline_data['samples_per_project'].items()), columns=['Project', 'Number of Samples']))
        else:
            st.write("No samples found for any project.")

        st.subheader("Responder/Non-Responder Counts")
        if custom_baseline_data['response_counts']:
            st.table(pd.DataFrame(list(custom_baseline_data['response_counts'].items()), columns=['Response', 'Number of Subjects']))
        else:
            st.write("No response data available.")

        st.subheader("Male/Female Counts")
        if custom_baseline_data['sex_counts']:
            st.table(pd.DataFrame(list(custom_baseline_data['sex_counts'].items()), columns=['Sex', 'Number of Subjects']))
        else:
            st.write("No sex data available.")
        
        st.subheader("Filtered Data")
        st.dataframe(custom_baseline_data['data_summary'])
        if not custom_baseline_data['data_summary'].empty:
            col1, col2 = st.columns(2, gap="small")
            with col1:
                csv_bytes = utils.df_to_csv_bytes(custom_baseline_data['data_summary'])
                st.download_button(
                    label="Download CSV",
                    data=csv_bytes,
                    file_name=f"custom_baseline_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="cbq_csv"
                )
            with col2:
                excel_bytes = utils.df_to_excel_bytes(custom_baseline_data['data_summary'])
                st.download_button(
                    label="Download Excel",
                    data=excel_bytes,
                    file_name=f"custom_baseline_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="cbq_excel"
                )

    else:
        st.info('No data found for the custom baseline query (Melanoma, PBMC, TR1, Baseline).')
