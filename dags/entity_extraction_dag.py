from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import json
import logging
import os
from datetime import datetime, timedelta
import docker
import tempfile
import ast

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'entity_extraction_pipeline',
    default_args=default_args,
    description='Entity extraction and matching pipeline',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def load_documents(**kwargs):
        """Load documents from CSV and push to XCom."""
        try:
            documents_path = 'data/documents.csv'
            if not os.path.exists(documents_path):
                raise FileNotFoundError(f"Documents file not found at {documents_path}")
            df = pd.read_csv(documents_path)
            required_columns = ['uuid', 'body_en']
            if not all(col in df.columns for col in required_columns):
                raise ValueError(f"Missing required columns in documents.csv: {required_columns}")
            logger.info(f"Loaded {len(df)} documents")
            kwargs['ti'].xcom_push(key='documents', value=df.to_dict(orient='records'))
        except Exception as e:
            logger.error(f"Error loading documents: {str(e)}")
            raise

    def extract_entities(**kwargs):
        """Run Docker container to extract entities and push results to XCom."""
        try:
            documents = kwargs['ti'].xcom_pull(key='documents', task_ids='load_documents')
            client = docker.from_env()
            extracted_entities = []

            logger.info(f"Starting entity extraction for {len(documents)} documents")
            for i, doc in enumerate(documents, 1):
                doc_id = doc['uuid']
                text = doc['body_en']
                logger.info(f"Processing document {i}/{len(documents)} (ID: {doc_id})")
                logger.debug(f"Document text length: {len(text)} characters")

                # Create temporary files for input and output
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as tmp_in, \
                     tempfile.NamedTemporaryFile(mode='r', delete=False, suffix='.json') as tmp_out:
                    tmp_in.write(text)
                    tmp_in.flush()  # Ensure input is written to disk
                    tmp_path_in = tmp_in.name
                    tmp_path_out = tmp_out.name
                    logger.debug(f"Temporary input file: {tmp_path_in}, output file: {tmp_path_out}")

                    try:
                        logger.info(f"Running Docker container for document {doc_id}")
                        container = client.containers.run(
                            image='entity-extraction:latest',
                            command=f'python /app/extract_entities.py /app/input.txt /app/output.json',
                            volumes={
                                os.path.abspath(tmp_path_in): {'bind': '/app/input.txt', 'mode': 'ro'},
                                os.path.abspath(tmp_path_out): {'bind': '/app/output.json', 'mode': 'rw'}
                            },
                            detach=True
                        )
                        container.wait()
                        container.remove()
                        logger.info(f"Container completed for document {doc_id}")

                        # Read output JSON file
                        if os.path.exists(tmp_path_out) and os.path.getsize(tmp_path_out) > 0:
                            with open(tmp_path_out, 'r', encoding='utf-8') as f:
                                entities = json.load(f)
                            logger.debug(f"Extracted {len(entities)} entities for document {doc_id}: {entities}")
                        else:
                            logger.warning(f"Output file {tmp_path_out} is empty or missing for document {doc_id}")
                            entities = []

                        for entity in entities:
                            entity['document_id'] = doc_id
                        extracted_entities.extend(entities)
                        logger.info(f"Successfully extracted {len(entities)} entities from document {doc_id}")
                    except Exception as e:
                        logger.error(f"Error processing document {doc_id}: {str(e)}")
                        raise
                    finally:
                        os.unlink(tmp_path_in)
                        os.unlink(tmp_path_out)
                        logger.debug(f"Cleaned up temporary files for document {doc_id}")

            logger.info(f"Completed entity extraction. Total entities extracted: {len(extracted_entities)}")
            if extracted_entities:
                logger.debug("Sample of extracted entities:")
                for entity in extracted_entities[:5]:  # Show first 5 entities
                    logger.debug(f"Entity: {entity['text']} (type: {entity['label']})")
            kwargs['ti'].xcom_push(key='extracted_entities', value=extracted_entities)
        except Exception as e:
            logger.error(f"Error in entity extraction: {str(e)}")
            raise

    def match_entities(**kwargs):
        """Match extracted entities against aliases table and push results to XCom."""
        try:
            extracted_entities = kwargs['ti'].xcom_pull(key='extracted_entities', task_ids='extract_entities')
            logger.info(f"Number of extracted entities to match: {len(extracted_entities)}")
            
            aliases_path = 'data/entity_aliases.csv'
            if not os.path.exists(aliases_path):
                raise FileNotFoundError(f"Aliases file not found at {aliases_path}")

            aliases_df = pd.read_csv(aliases_path)
            logger.info(f"Number of alias entries loaded: {len(aliases_df)}")
            
            aliases_df['aliases'] = aliases_df['aliases'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip() else [])

            matched_entities = []
            for entity in extracted_entities:
                entity_text = entity['text']
                entity_type = entity['label']
                matched = False
                matched_entity_id = None
                matched_entity_name = None

                # Debug: Log the entity being processed
                logger.debug(f"Processing entity: {entity_text} (type: {entity_type})")

                for _, row in aliases_df[aliases_df['entity_type'] == entity_type].iterrows():
                    if entity_text == row['name'] or entity_text in row['aliases']:
                        matched = True
                        # to be replaced with entity_id from actual SoT entities database
                        matched_entity_id = row['name']
                        matched_entity_name = row['name']
                        logger.info(f"Matched entity '{entity_text}' with '{matched_entity_name}'")
                        break

                matched_entities.append({
                    'document_id': entity['document_id'],
                    'entity_type': entity_type,
                    'entity_text': entity_text,
                    'start_pos': entity['start'],
                    'end_pos': entity['end'],
                    'score': entity['score'],
                    'is_matched': matched,
                    'matched_entity_id': matched_entity_id,
                    'matched_entity_name': matched_entity_name
                })

            logger.info(f"Matched {sum(e['is_matched'] for e in matched_entities)} entities out of {len(matched_entities)}")
            kwargs['ti'].xcom_push(key='matched_entities', value=matched_entities)
        except Exception as e:
            logger.error(f"Error in entity matching: {str(e)}")
            raise

    def store_to_db(**kwargs):
        """Store matched entities in SQLite database."""
        try:
            matched_entities = kwargs['ti'].xcom_pull(key='matched_entities', task_ids='match_entities')
            db_path = 'output/extracted_entities.db'
            os.makedirs(os.path.dirname(db_path), exist_ok=True)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS extracted_entities (
                    document_id TEXT,
                    entity_type TEXT,
                    entity_text TEXT,
                    start_pos INTEGER,
                    end_pos INTEGER,
                    score REAL,
                    is_matched BOOLEAN,
                    matched_entity_id TEXT,
                    matched_entity_name TEXT
                )
            ''')

            # Insert data
            for entity in matched_entities:
                cursor.execute('''
                    INSERT INTO extracted_entities (
                        document_id, entity_type, entity_text, start_pos, end_pos,
                        score, is_matched, matched_entity_id, matched_entity_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    entity['document_id'],
                    entity['entity_type'],
                    entity['entity_text'],
                    entity['start_pos'],
                    entity['end_pos'],
                    entity['score'],
                    entity['is_matched'],
                    entity['matched_entity_id'],
                    entity['matched_entity_name']
                ))

            conn.commit()
            logger.info(f"Stored {len(matched_entities)} entities in database")
            conn.close()
        except Exception as e:
            logger.error(f"Error storing to database: {str(e)}")
            raise

    def build_docker_image(**kwargs):
        """Build the Docker image using the Docker SDK."""
        try:
            client = docker.from_env()
            logger.info("Building Docker image...")
            image, logs = client.images.build(
                path=".",
                tag="entity-extraction:latest",
                rm=True
            )
            for log in logs:
                if 'stream' in log:
                    logger.info(log['stream'].strip())
                if 'error' in log:
                    logger.error(log['error'].strip())
            logger.info("Docker image built successfully")
        except Exception as e:
            logger.error(f"Error building Docker image: {str(e)}")
            raise

    # Define tasks
    build_docker = PythonOperator(
        task_id='build_docker',
        python_callable=build_docker_image,
        dag=dag,
    )

    load_task = PythonOperator(
        task_id='load_documents',
        python_callable=load_documents,
    )

    extract_task = PythonOperator(
        task_id='extract_entities',
        python_callable=extract_entities,
        provide_context=True,
    )

    match_task = PythonOperator(
        task_id='match_entities',
        python_callable=match_entities,
    )

    store_task = PythonOperator(
        task_id='store_to_db',
        python_callable=store_to_db,
    )

    # Set task dependencies
    build_docker >> load_task >> extract_task >> match_task >> store_task
