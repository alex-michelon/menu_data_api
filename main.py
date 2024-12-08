from flask import Flask, jsonify, request
from functools import wraps
import os
import sqlalchemy
import logging
import sys
from google.cloud import secretmanager

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def access_secret_version(secret_id):
    try:
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable not set")
            
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Error accessing secret {secret_id}: {e}")
        return None

# Initialize database connection
def init_db_connection():
    try:
        db_user = os.environ.get('DB_USER')
        db_pass = os.environ.get('DB_PASSWORD')
        db_name = os.environ.get('DB_NAME')
        db_connection = os.environ.get('DB_CONNECTION_NAME')
        
        logger.info(f"Database configuration: user={db_user}, name={db_name}, connection={db_connection}")
        
        if not all([db_user, db_pass, db_name, db_connection]):
            logger.error("Missing database configuration")
            return None

        pool = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername="postgresql+pg8000",
                username=db_user,
                password=db_pass,
                database=db_name,
                query={"unix_sock": f"/cloudsql/{db_connection}/.s.PGSQL.5432"},
            ),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800,
        )
        
        with pool.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        logger.info("Database connection successful")
        return pool
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

try:
    logger.info("Initializing application...")
    db = init_db_connection()
    API_KEY = os.environ.get('API_KEY')
    if not API_KEY:
        logger.error("API_KEY not found in environment variables")
    else:
        logger.info("API_KEY successfully loaded")
except Exception as e:
