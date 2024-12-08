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
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def access_secret_version(secret_id, project_id=None):
    try:
        if not project_id:
            project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.error(f"Failed to access secret {secret_id}: {str(e)}")
        return None

def init_db_connection():
    try:
        db_user = access_secret_version('db-user')
        db_pass = access_secret_version('db-password')
        db_name = access_secret_version('db-name')
        db_connection = access_secret_version('db-connection-name')
        
        if not all([db_user, db_pass, db_name, db_connection]):
            raise ValueError("Missing database configuration secrets")

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
        return pool
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return None

try:
    db = init_db_connection()
    API_KEY = access_secret_version('api-key')
    if not API_KEY:
        logger.error("Failed to retrieve API key")
except Exception as e:
    logger.error(f"Initialization error: {str(e)}")
    db = None

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        provided_key = request.headers.get('X-API-Key')
        if not provided_key:
            provided_key = request.args.get('api_key')
        
        if not provided_key or provided_key != API_KEY:
            return jsonify({'error': 'Invalid or missing API key'}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/api/objects', methods=['GET'])
@require_api_key
def get_objects():
    if not db:
        return jsonify({'error': 'Database connection not available'}), 503
    
    try:
        date = request.args.get('date')
        
        query = 'SELECT * FROM daily_meals'
        params = {}
        
        if date:
            query += ' WHERE date = :date'
            params = {'date': date}

        with db.connect() as conn:
            result = conn.execute(sqlalchemy.text(query), params)
            results = [dict(row._mapping) for row in result]
            return jsonify(results)
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/health', methods=['GET'])
def health_check():
    if not db:
        return jsonify({'status': 'unhealthy', 'error': 'Database connection not available'}), 503
    return jsonify({'status': 'healthy'}), 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
