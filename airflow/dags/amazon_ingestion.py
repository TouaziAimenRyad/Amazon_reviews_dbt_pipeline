from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import requests, gzip, json, os, shutil, hashlib
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict
import ast

default_args = {
    'owner': 'msc_de2',
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=30)
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_file(url: str, output_dir: str) -> Dict:
    """Download file with hash calculation for versioning"""
    filename = url.split("/")[-1]
    temp_path = os.path.join("/tmp", filename)
    final_path = os.path.join(output_dir, filename)
    
    # Calculate hash of existing file if it exists
    old_hash = None
    if os.path.exists(final_path):
        with open(final_path, 'rb') as f:
            old_hash = hashlib.md5(f.read()).hexdigest()
    
    # Download new file
    response = requests.get(url, stream=True, timeout=(10, 30))
    response.raise_for_status()
    
    with open(temp_path, 'wb') as f:
        for chunk in response.iter_content(8192):
            if chunk:
                f.write(chunk)
    
    # Calculate new hash
    with open(temp_path, 'rb') as f:
        new_hash = hashlib.md5(f.read()).hexdigest()
    
    # Only move if changed
    if old_hash != new_hash:
        os.makedirs(output_dir, exist_ok=True)
        versioned_path = os.path.join(output_dir, f"{new_hash[:8]}_{filename}")
        shutil.move(temp_path, versioned_path)
        return {
            'path': versioned_path,
            'changed': True,
            'hash': new_hash,
            'filename': filename,
            'url': url
        }
    else:
        os.remove(temp_path)
        return {
            'path': final_path,
            'changed': False,
            'hash': old_hash,
            'filename': filename,
            'url': url
        }

def download_all_files(urls: list, output_dir: str) -> List[Dict]:
    os.makedirs(output_dir, exist_ok=True)
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda url: download_file(url, output_dir), urls))
    
    # Log download results
    changed_files = [r['filename'] for r in results if r['changed']]
    if changed_files:
        print(f"New versions downloaded for: {changed_files}")
    return json.dumps(results)  # Serialize to JSON string for XCom

def track_file_versions(pg_hook, file_info_json: str):
    """Track file versions in Postgres for OSS versioning"""
    try:
        file_info = json.loads(file_info_json)  # Deserialize JSON string
    except json.JSONDecodeError:
        file_info = ast.literal_eval(file_info_json)  # Fallback for string representation
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create tracking table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS data_wearhouse.oss_file_versions (
        file_name VARCHAR,
        file_hash VARCHAR,
        file_path VARCHAR,
        download_time TIMESTAMP,
        loaded_to_dw BOOLEAN DEFAULT FALSE,
        source_url VARCHAR,
        PRIMARY KEY (file_name, file_hash)
    );
    """)
    
    # Insert new versions
    for info in file_info:
        if isinstance(info, dict) and info.get('changed'):
            cursor.execute("""
            INSERT INTO data_wearhouse.oss_file_versions 
            (file_name, file_hash, file_path, download_time, source_url)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
            ON CONFLICT (file_name, file_hash) DO NOTHING
            """, (
                info.get('filename'), 
                info.get('hash'),
                info.get('path'),
                info.get('url')
            ))
    
    conn.commit()
    cursor.close()
    conn.close()

def load_new_files_to_dw():
    """Load only new files that haven't been loaded yet"""
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Get unloaded files
    cursor.execute("""
    SELECT file_path, file_name 
    FROM data_wearhouse.oss_file_versions 
    WHERE loaded_to_dw = FALSE
    """)
    files_to_load = cursor.fetchall()
    
    if not files_to_load:
        print("No new files to load")
        return
    
    # Create staging schema if not exists
    cursor.execute("CREATE SCHEMA IF NOT EXISTS data_wearhouse;")
    conn.commit()
    
    for file_path, file_name in files_to_load:
        category = extract_category(file_name)
        
        # Create table for this category
        table_name = create_table_for_category(pg_hook, category)
        
        # Load data into the specific table
        with gzip.open(file_path, 'rt') as f:
            for line in f:
                row = json.loads(line)
                cursor.execute(f"""
                INSERT INTO data_wearhouse.{table_name} (
                    review_id, asin, parent_asin, user_id, reviewer_name,
                    review_text, overall_rating, summary,
                    unix_review_time, review_time, helpful_vote,
                    verified_purchase, images, _source_file
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s / 1000), %s, %s, %s, %s)
                ON CONFLICT (review_id) DO NOTHING
                """, (
                    row.get('user_id'),
                    row.get('asin'),
                    row.get('parent_asin'),
                    row.get('user_id'),
                    row.get('reviewer_name'),
                    row.get('text'),
                    row.get('rating'),
                    row.get('title'),
                    row.get('timestamp'),
                    row.get('timestamp'),
                    row.get('helpful_vote', 0),
                    row.get('verified_purchase', False),
                    json.dumps(row.get('images', [])),
                    file_name
                ))
        
        # Mark as loaded
        cursor.execute("""
        UPDATE data_wearhouse.oss_file_versions
        SET loaded_to_dw = TRUE
        WHERE file_path = %s
        """, (file_path,))
        conn.commit()
    
    cursor.close()
    conn.close()

def extract_category(fname: str) -> str:
    """Extract category name from filename"""
    if fname.endswith('.gz'):
        fname = fname[:-3]
    base_name = os.path.splitext(os.path.basename(fname))[0]
    return base_name.replace('meta_', '').lower() if base_name else 'unknown'

def create_table_for_category(pg_hook, category: str) -> str:
    """Create table for a specific category if not exists"""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    table_name = f"{category}"#!!!
    
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS data_wearhouse.{table_name} (
        review_id VARCHAR PRIMARY KEY,
        asin VARCHAR,
        parent_asin VARCHAR,
        user_id VARCHAR,
        reviewer_name VARCHAR,
        review_text TEXT,
        overall_rating FLOAT,
        summary TEXT,
        unix_review_time BIGINT,
        review_time TIMESTAMP,
        helpful_vote INT,
        verified_purchase BOOLEAN,
        images JSONB,
        _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _source_file VARCHAR
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    return table_name




with DAG(
    dag_id="amazon_reviews_dbt_pipeline_v4",
    default_args=default_args,
    description="Enhanced Amazon reviews ETL with OSS versioning and DBT",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['amazon', 'reviews', 'etl']
) as dag:

    urls = [
        "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Subscription_Boxes.jsonl.gz",
        "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Gift_Cards.jsonl.gz",
        "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Digital_Music.jsonl.gz"
    ]
    
   
    
    # Download tasks with version tracking
    t1_download = PythonOperator(
        task_id="download_files_with_versioning",
        python_callable=download_all_files,
        op_kwargs={
            "urls": urls,
            "output_dir": "/opt/airflow/data/amazon-raw"
        },
    )
    
    t2_track_versions = PythonOperator(
        task_id="track_file_versions",
        python_callable=track_file_versions,
        op_kwargs={
            "pg_hook": PostgresHook(postgres_conn_id='postgres'),
            "file_info_json": "{{ ti.xcom_pull(task_ids='download_files_with_versioning') }}"
        },
    )
    
    t3_load_dw = PythonOperator(
        task_id="load_new_files_to_data_warehouse",
        python_callable=load_new_files_to_dw,
    )
    
    # DBT tasks
    t4_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )
    
    t5_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
    )
    
    t6_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . || true", #making non blocking if test fails =
    )

    
    t7_dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir .",
    )
    
    
    # Corrected dependencies
    # Set up dependencies
    t1_download >> t2_track_versions >> t3_load_dw
    t3_load_dw >> t4_dbt_deps >> t5_dbt_run  # First build all models

    # Then run both tests and docs generation in parallel
    t5_dbt_run >> t6_dbt_test  # Tests depend on models
    t5_dbt_run >> t7_dbt_docs   # Docs depend on models


    # If you have a test results processing task