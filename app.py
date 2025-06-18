import flask
from flask import request, jsonify, send_file
from datetime import datetime
import logging
import sqlite3
import pandas as pd
import io

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

DB_FILE = '/tmp/task_events.db'  
REQUIRED_FIELDS = ['assignee_id', 'happened_at', 'task_id']
WEBHOOK_SECRET = 'segredodowebhook'

def initialize_db():
    try:
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
        logger.info("Banco de dados inicializado com sucesso")
    except sqlite3.Error as e:
        logger.error(f"Erro ao inicializar banco de dados: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

with app.app_context():
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

@app.route('/download-db')
def download_file():
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql('SELECT * FROM events', conn)
         
        excel_file = io.BytesIO()
        df.to_excel(excel_file, index=False, sheet_name='Eventos')
        excel_file.seek(0)

        conn.close()

        return send_file(
             excel_file,
             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
             as_attachment=True,
             download_name='task_events.xlsx'
        )
    except Exception as e:
         logger.error(f"Erro na exportação para Excel: {str(e)}")
         return jsonify({"error": str(e)}), 500

@app.route('/')
def health_check():
    return jsonify({"status": "online", "database": "initialized"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)