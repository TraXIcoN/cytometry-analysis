import sqlite3
import logging

logger = logging.getLogger(__name__)

# Expected columns for the samples table (excluding primary key if auto-generated)
EXPECTED_SAMPLE_COLUMNS = [
    'sample_id', 'project', 'subject', 'condition', 'age', 'sex',
    'treatment', 'response', 'sample_type', 'time_from_treatment_start'
]
# Column name in CSV that maps to 'sample_id' in the database
CSV_SAMPLE_ID_COLUMN = 'sample' 

# Expected cell population columns
EXPECTED_CELL_POPULATIONS = ['b_cell', 'cd8_t_cell', 'cd4_t_cell', 'nk_cell', 'monocyte']

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

    # Add Indexes
    logger.info("init_db: Creating indexes...")
    c.execute('CREATE INDEX IF NOT EXISTS idx_samples_sample_id ON samples (sample_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_samples_time_from_treatment_start ON samples (time_from_treatment_start)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_cell_counts_sample_id ON cell_counts (sample_id)')
    logger.info("init_db: Indexes created.")

    # Create operation_log table
    c.execute('''
        CREATE TABLE IF NOT EXISTS operation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            operation_type TEXT NOT NULL,
            sample_id TEXT,
            details TEXT
        )
    ''')
    logger.info("init_db: CREATE TABLE IF NOT EXISTS operation_log statement executed.")

    logger.info("init_db: Attempting to commit changes.")
    conn.commit()
    logger.info("init_db: Commit successful.")
    conn.close()
    logger.info(f"init_db: Connection to {db_file} closed.")
