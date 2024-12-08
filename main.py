from flask import Flask, jsonify
import pymysql

app = Flask(__name__)

def get_db_connection():
    connection = pymysql.connect(
        host='YOUR_CLOUD_SQL_IP',
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        database='YOUR_DATABASE'
    )
    return connection

@app.route('/api/objects', methods=['GET'])
def get_objects():
    connection = get_db_connection()
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM your_table')
        results = cursor.fetchall()
    connection.close()
    return jsonify(results)

if __name__ == '__main__':
    app.run()
