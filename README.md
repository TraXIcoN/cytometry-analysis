# Cytometry Data Analysis Dashboard

This application provides an interactive dashboard for analyzing cytometry data from clinical trials, enabling users to explore immune cell population frequencies, compare treatment responses, and visualize data patterns using Streamlit.

## Features

### Distributed Locking and Robust Initialization

This app implements a robust distributed cache and database initialization system to ensure correct operation across multiple devices and deployments:

- **Distributed Locking:** Only one instance at a time can initialize the database and cache from the CSV, preventing race conditions and partial/empty cache issues.
- **Cache Validation:** The cache is only used if it contains a valid, non-empty DataFrame with the expected schema (including `sample_id`) and the database is populated. Otherwise, the cache is invalidated and reloaded from the CSV.
- **Idempotent Startup:** All app instances check both the cache and database population before proceeding, guaranteeing consistency and preventing `KeyError` or missing data issues, even in distributed or multi-user scenarios.

This ensures that the application is robust to restarts, redeployments, and multi-device usage, and that no instance ever gets stuck with a stale or incomplete cache or database.


- Interactive data loading and exploration from `cell-count.csv`.
- Modular codebase with clear separation of concerns across multiple packages (`app.py`, `db_layer`, `reporting_tools`, `ui_modules`).
- Relative frequency calculations for immune cell populations.
- Response comparison analysis with statistical significance testing.
- Baseline statistics for treatment groups.
- Modern, user-friendly interface with a two-column Streamlit layout (controls on the left, data display on the right).
- Dynamic data updates: adding or removing samples refreshes views instantly.
- Conditional CSV download for analysis results.
- PDF report generation with key findings and visualizations.
- Caching of frequent queries and computations to enhance performance.
- Checkpoint feature: Users can revert to previous database snapshots stored in a `checkpoints` folder.
- Manual sample addition: Users can add new samples directly via the UI, with input as CSV rows.
- Multi-format output: Reports available in CSV, Excel, and PDF formats, plus downloadable plot images.

## Project Structure

The application is organized into several Python modules and packages to ensure maintainability and scalability:

- `app.py`: Main Streamlit application file, managing UI interactions and orchestrating module calls.
- `db_layer`: Package for database interactions:
  - `__init__.py`: Package initialization.
  - `admin_manager.py`: Manages database checkpoints and logging.
  - `crud_ops.py`: Handles CRUD operations for database management.
  - `data_loader.py`: Manages loading and appending data to the database.
  - `query_executor.py`: Executes data retrieval queries.
  - `schema_manager.py`: Initializes and manages the database schema.
- `reporting_tools`: Package for analysis and reporting:
  - `__init__.py`: Package initialization.
  - `analysis.py`: Functions for data processing and statistical analysis.
  - `reporting.py`: Generates PDF reports from data.
  - `utils.py`: Utility functions, including CSV/Excel download link generation and plot image export.
- `ui_modules`: Package for UI components:
  - `__init__.py`: Package initialization.
  - `app_helpers.py`: Initializes application and session state.
  - `left_column.py`: Renders controls in the left column.
  - `right_column_tabs.py`: Manages tabs in the right column.
- `requirements.txt`: Lists all Python dependencies.
- `cell-count.csv`: Initial dataset for database population (required on first run if `cytometry_data.db` is absent).
- `cytometry_data.db`: SQLite database file (generated on first run).
- `checkpoints`: Folder storing database snapshots for checkpoint reversion.
- `README.md`: This file, providing project details.
- `.gitignore`: Excludes temporary files and caches.
- `MANIFEST.in`: Configures package distribution.
- `setup.py`: Facilitates package installation.

## Database Schema Design

The database is designed for scalability and maintainability:

1. **Samples Table**

   - Stores sample metadata (e.g., `sample_id`, `project`, `condition`, `age`, `sex`, `treatment`, `response`, `sample_type`, `time_from_treatment_start`).
   - Normalized structure for easy expansion.
   - Supports multiple sample types and treatments.

2. **Cell Counts Table**

   - Stores cell population counts (e.g., `b_cell`, `cd8_t_cell`, etc.) with a unique `id`, `sample_id`, `population`, and `count`.
   - Flexible for adding new cell populations.
   - Efficient querying with proper indexing.

3. **Operation Log Table**
   - Logs database operations (e.g., sample additions, deletions) with timestamps for auditing and debugging.

## Rationale for Design and Scalability

The database schema and codebase are designed with scalability and flexibility in mind:

- **Hundreds of Projects**: The `Samples Table` stores project-specific metadata, enabling efficient management and querying across multiple projects.
- **Thousands of Samples**: The normalized structure and indexing on key fields (e.g., `sample_id`, `time_from_treatment_start`) ensure quick data retrieval and scalability.
- **Various Types of Analytics**: Separating `Samples` and `Cell Counts` tables supports diverse analyses (e.g., frequency calculations, response comparisons) without performance degradation. The `Operation Log` enhances traceability for large-scale operations.
- **Code Modularity**: The package-based structure allows independent updates to database, analysis, or UI components, supporting long-term maintenance and expansion.
- **Caching**: Frequent queries (e.g., frequency tables, baseline stats) are cached in-memory to reduce database load and improve response times, especially beneficial as data volume grows.
- **Checkpoint System**: The `checkpoints` folder enables database reversion, ensuring data recovery and experimentation flexibility.

This design ensures the application remains performant and adaptable as the dataset expands to include more projects, samples, or analytical needs.

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

The dashboard features a two-column layout:

- **Left Column**: Contains controls for:
  - Uploading `cell-count.csv` (required on first run if the database is empty).
  - Filtering data by metadata (e.g., condition, treatment).
  - Adding or removing samples (views update dynamically; new samples can be added manually via CSV row input).
  - Reverting to a previous checkpoint from the `checkpoints` folder.
  - Selecting analysis types (e.g., frequencies, response comparison).
- **Right Column**: Displays:
  - Data tables (e.g., raw counts, relative frequencies).
  - Interactive visualizations (e.g., boxplots for response comparisons).
  - Statistical summaries and results.
  - Options to download tables as CSV/Excel, reports as PDF, and plots as images.

**Initial Setup:**
On first run or if `cytometry_data.db` is missing, upload `cell-count.csv` to initialize and populate the database.

**Analysis Types:**

- **Relative Frequencies**: Displays and plots cell population percentages.
- **Response Comparison**: Compares responder vs. non-responder statistics with significance tests.
- **Baseline Statistics**: Shows aggregates for baseline samples (e.g., `time_from_treatment_start = 0`).

## Data Processing

The application automatically:

- Loads and validates data from `cell-count.csv`.
- Calculates relative frequencies with precision.
- Performs statistical analysis (e.g., t-tests).
- Generates interactive visualizations.
- Maintains data consistency with dynamic updates.
- Utilizes caching for frequently accessed results to enhance performance.
- Manages checkpoints for database reversion.

## Scalability Considerations

The schema supports:

- Multiple projects and sample types.
- Thousands of samples with indexed queries.
- New cell populations via flexible table design.
- Data integrity with foreign key constraints.
- **Caching Scalability**: In-memory caching improves performance for frequent queries. For larger datasets or concurrent users, consider integrating a distributed cache (e.g., Redis) to scale horizontally.
- **Checkpoint Scalability**: The `checkpoints` folder works for small datasets but may require a versioned storage system (e.g., S3) for large-scale or cloud deployments.
- **Database Transition**: The current SQLite setup is suitable for thousands of samples. For millions, migrate to PostgreSQL with connection pooling and sharding, leveraging the existing schema with minor adjustments.

## Future Enhancements

- Add advanced visualizations (e.g., heatmaps, PCA plots).
- Implement user authentication and role-based access.
- Enhance PDF reports with customizable templates.
- Integrate predictive analytics (e.g., response prediction models).
- Develop a custom query builder for flexible data filtering.
- Add collaboration features, such as saving and sharing analysis configurations via URL or JSON files for sharing with colleagues (e.g., Yah D’yada), and a comment/annotation system for sample observations.
- Implement comprehensive testing and reliability measures, including unit tests, integration tests, and a CI/CD pipeline.

## GitHub Pages Link

[Include link to your GitHub Pages deployment here once available.]
