import flask
from flask import request, jsonify
from datetime import datetime
import logging
import sqlite3


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

DB_FILE = 'task_events.db'
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
           'action': 'play' if  'play' in request.path else 'pause',
           'task_id': data['task_id'],
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
         conn.close()
         logger.info(f"Evento registrado no SQLite: {new_record}")
         return jsonify({"status": "success"}), 200
     except Exception as e:
           logger.error(f"Erro no SQLite: {str(e)}")
           return jsonify({"error": "Falha ao registrar evento"}), 500
         

if __name__ == '__main__':
      app.run(host='0.0.0.0', port=5000)

if __name__ != '__main__':
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)




            

           
          
    
    

  
     
     

     
     
   
     