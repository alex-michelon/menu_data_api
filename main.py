from flask import Flask, jsonify, request
from functools import wraps
import os
import sqlalchemy
import logging
from google.cloud import secretmanager

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

def access_secret_version(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.environ.get('PROJECT_ID')}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

try:
    API_KEY = access_secret_version('api-key')
    DB_USER = access_secret_version('db-user')
    DB_PASSWORD = access_secret_version('db-password')
    DB_NAME = access_secret_version('db-name')
    DB_CONNECTION_NAME = access_secret_version('db-connection-name')
except Exception as e:
    logging.error(f"Error accessing secrets: {str(e)}")
    raise e

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

def init_db_connection():
    try:
        pool = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername="postgresql+pg8000",
                username=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                query={"unix_sock": f"/cloudsql/{DB_CONNECTION_NAME}/.s.PGSQL.5432"},
            ),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800,
        )
        return pool
    except Exception as e:
        logging.error(f"Error initializing database connection: {str(e)}")
        raise e

db = init_db_connection()

@app.route('/api/objects', methods=['GET'])
@require_api_key
def get_objects():
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
        logging.error(f"Database error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
