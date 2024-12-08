from flask import Flask, jsonify, request
from functools import wraps
import os
import sqlalchemy
import pymysql
from google.cloud import secretmanager

app = Flask(__name__)

API_KEY = os.environ.get('API_KEY')

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

def init_connection_pool():
    db_config = {
        'pool_size': 5,
        'pool_timeout': 30,
        'pool_recycle': 1800,
    }
    return sqlalchemy.create_engine(
        'mysql+pymysql://{user}:{password}@/{database}?unix_socket=/cloudsql/{connection_name}'.format(
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASS'),
            database=os.environ.get('DB_NAME'),
            connection_name=os.environ.get('CLOUD_SQL_CONNECTION_NAME')
        ),
        **db_config
    )

db = init_connection_pool()

@app.route('/api/objects', methods=['GET'])
@require_api_key  # Add this decorator to protect the endpoint
def get_objects():
    try:
        date = request.args.get('date')
        
        query = 'SELECT * FROM daily_meals'
        params = []
        
        if date:
            query += ' WHERE date = %s'
            params = [date]

        with db.connect() as conn:
            results = conn.execute(query, params).fetchall()
            return jsonify([dict(row) for row in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
