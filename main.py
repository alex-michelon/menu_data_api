from flask import Flask, jsonify, request
from flask_cors import CORS
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
CORS(app, resources={r"/*": {"origins": "*"}})

@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-API-Key"
    return response

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
    logger.error(f"Initialization error: {e}")
    db = None
    API_KEY = None

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        provided_key = request.headers.get('X-API-Key')
        if not provided_key:
            provided_key = request.args.get('api_key')
        
        logger.info(f"API request received with key: {provided_key[:4]}...")
        logger.info(f"Stored API key starts with: {API_KEY[:4] if API_KEY else 'None'}...")
        
        if not provided_key or provided_key != API_KEY:
            logger.error("API key validation failed")
            return jsonify({'error': 'Invalid or missing API key'}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/api/objects', methods=['GET'])
@require_api_key
def get_objects():
    if not db:
        logger.error("Database connection not available")
        return jsonify({'error': 'Database connection not available'}), 503
    
    try:
        date = request.args.get('date')
        meal_time = request.args.get('meal_time')
        line_type = request.args.get('line_type')
        
        query = 'SELECT * FROM daily_meals'
        params = {}

        if date or meal_time or line_type:
            query += ' WHERE '
        
        if date:
            query += 'date = :date'
            params['date'] = date
        
        if meal_time:
            if date:
                query += ' AND '
            query += 'meal_time = :meal_time'
            params['meal_time'] = meal_time

        if line_type:
            if date or meal_time:
                query += ' AND '
            query += 'line_type = :line_type'
            params['line_type'] = line_type

        with db.connect() as conn:
            result = conn.execute(sqlalchemy.text(query), params)
            results = [dict(row._mapping) for row in result]
            logger.info(f"Retrieved {len(results)} records")
            return jsonify(results)
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    status = {
        'database': bool(db),
        'api_key': bool(API_KEY),
        'environment': {
            'DB_USER': bool(os.environ.get('DB_USER')),
            'DB_NAME': bool(os.environ.get('DB_NAME')),
            'DB_CONNECTION_NAME': bool(os.environ.get('DB_CONNECTION_NAME')),
            'API_KEY': bool(os.environ.get('API_KEY'))
        }
    }
    
    if all(status['environment'].values()) and db and API_KEY:
        return jsonify({'status': 'healthy', 'details': status}), 200
    return jsonify({'status': 'unhealthy', 'details': status}), 503

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
