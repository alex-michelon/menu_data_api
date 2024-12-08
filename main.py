from flask import Flask, jsonify
import os
import sqlalchemy
import pymysql

app = Flask(__name__)

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
def get_objects():
    try:
        with db.connect() as conn:
            results = conn.execute('SELECT * FROM your_table').fetchall()
            return jsonify([dict(row) for row in results])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
