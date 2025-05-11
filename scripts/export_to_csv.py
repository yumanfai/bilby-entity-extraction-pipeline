import sqlite3
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_to_csv(db_path='output/extracted_entities.db', csv_path='output/extracted_entities.csv'):
    """Export the extracted_entities table from SQLite to a CSV file."""
    try:
        # Check if database exists
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found at {db_path}")

        # Connect to SQLite database
        conn = sqlite3.connect(db_path)

        # Query all data from extracted_entities table
        df = pd.read_sql_query('SELECT * FROM extracted_entities', conn)

        # Ensure output directory exists
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)

        # Export to CSV
        df.to_csv(csv_path, index=False)
        logger.info(f"Exported {len(df)} records to {csv_path}")

        # Close connection
        conn.close()

    except Exception as e:
        logger.error(f"Error exporting to CSV: {str(e)}")
        raise

if __name__ == "__main__":
    export_to_csv()