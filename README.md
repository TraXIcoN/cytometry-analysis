# Cytometry Data Analysis Dashboard

This application provides an interactive dashboard for analyzing cytometry data from clinical trials. It allows users to explore immune cell population frequencies, compare treatment responses, and visualize data patterns.

## Features

- Interactive data loading and exploration from `cell-count.csv`.
- Modular codebase with clear separation of concerns (`app.py`, `database.py`, `analysis.py`, `utils.py`).
- Relative frequency calculations for immune cell populations.
- Response comparison analysis with statistical significance testing.
- Baseline statistics for treatment groups.
- Modern, user-friendly interface using Streamlit with a two-column layout (controls on the left, data display on the right).
- Dynamic data updates: adding or removing samples immediately refreshes the view.
- Conditional CSV download for analysis results.

## Project Structure

The application is organized into several Python modules to promote maintainability and scalability:

- `app.py`: The main Streamlit application file. Handles UI elements, user interactions, and orchestrates calls to other modules.
- `database.py`: Manages all interactions with the SQLite database, including initialization, data loading (from `cell-count.csv`), and CRUD operations for samples and cell counts.
- `analysis.py`: Contains functions for data processing and statistical analysis, such as calculating frequency tables, comparing treatment responses, and performing baseline analysis.
- `utils.py`: Provides utility functions, like generating download links for CSV files.
- `requirements.txt`: Lists all Python dependencies required to run the application.
- `cell-count.csv`: The initial dataset used by the application (must be provided by the user at first run if the database is not already populated).
- `README.md`: This file, providing information about the project.

## Database Schema Design

The database is designed to be scalable and maintainable:

1. **Samples Table**

   - Stores sample metadata
   - Normalized structure for easy expansion
   - Supports multiple sample types and treatments

2. **Cell Counts Table**

   - Normalized storage of cell population counts
   - Flexible for adding new cell populations
   - Efficient querying through proper indexing

3. **Analysis Results Table**
   - Stores pre-calculated analysis results
   - Optimizes performance for repeated queries
   - Maintains data consistency

## Installation

1. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the application:

```bash
streamlit run app.py
```

## Usage

The application interface is divided into two main columns:
- The **left column** houses controls for:
    - Uploading the `cell-count.csv` file (if not already loaded for the session and the database is empty).
    - Filtering data based on available metadata.
    - Adding new samples or removing existing ones (data views refresh immediately).
    - Selecting the type of analysis to perform.
- The **right column** displays:
    - Data tables (e.g., raw counts, relative frequencies).
    - Interactive visualizations (e.g., plots for relative frequencies, response comparisons).
    - Statistical summaries and results.

**Initial Setup:**
On the first run, or if the application's database file (`cytometry_data.db`) is not found, the application will prompt you to upload the `cell-count.csv` file. This file is used to initialize and populate the database.

**Analysis Types:**

Users can select from various analysis options, which are then displayed in the right column:
- **Relative Frequencies**: View and plot the relative frequencies of different immune cell populations across samples.
- **Response Comparison**: Compare cell population statistics between predefined responder and non-responder groups, including statistical significance tests.
- **Baseline Statistics**: View descriptive statistics for samples categorized under a 'Baseline' timepoint or similar designation.

A 'Download CSV' button appears contextually when tabular data (like analysis results or filtered datasets) is displayed, allowing users to export it.


## Data Processing

The application automatically:

- Loads and validates data
- Calculates relative frequencies
- Performs statistical analysis
- Generates visualizations
- Maintains data consistency

## Scalability Considerations

The database schema is designed to:

- Handle multiple projects and sample types
- Support thousands of samples
- Allow for new cell populations
- Maintain data integrity through foreign key constraints
- Optimize query performance with proper indexing

## Future Enhancements

- Add more statistical tests
- Implement data validation rules
- Add export functionality for analysis results
- Implement user authentication
- Add more visualization options
