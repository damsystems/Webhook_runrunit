import flask
from flask import request, jsonify
from datetime import datetime
import logging
import sqlite3
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

DB_FILE = '/tmp/task_events.db' 
REQUIRED_FIELDS = ['assignee_id', 'happened_at', 'task_id']
WEBHOOK_SECRET = 'segredodowebhook'

def initialize_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            assignee_id TEXT,
            happened_at TEXT,
            action TEXT,
            task_id TEXT,
            recorded_at TEXT,
            tab_token TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Banco de dados inicializado")

@app.before_first_request
def before_first_request():
    initialize_db()

@app.route('/webhook', methods=['POST'])
def webhook():
    if not request.is_json:
        logger.warning("Received non-JSON request")
        return jsonify({"error": "Request must be JSON"}), 400
    
    data = request.json
    logger.info(f"Received webhook data: {data}")

    missing_fields = [field for field in REQUIRED_FIELDS if field not in data]
    if missing_fields:
        logger.warning(f"Missing required fields: {missing_fields}")
        return jsonify({"error": f"Missing fields: {', '.join(missing_fields)}"}), 400
    
    new_record = {
        'assignee_id': data['assignee_id'],
        'happened_at': data['happened_at'],
        'action': 'play' if 'play' in request.path else 'pause',
        'task_id': str(data['task_id']), 
        'recorded_at': datetime.now().isoformat(),
        'tab_token': data.get('tab_token', '')
    }

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            new_record['assignee_id'],
            new_record['happened_at'],
            new_record['action'],
            new_record['task_id'],
            new_record['recorded_at'],
            new_record['tab_token']
        ))
        conn.commit()
        logger.info(f"Evento registrado no SQLite: {new_record}")
        return jsonify({"status": "success"}), 200
    except sqlite3.Error as e:
        logger.error(f"Erro no SQLite: {str(e)}")
        return jsonify({"error": f"Erro no banco de dados: {str(e)}"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    initialize_db()
    app.run(host='0.0.0.0', port=5000)

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
