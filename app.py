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
REQUIRED_FIELDS = ['happened_at']
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
    
    try:
        task_id = str(data['data']['task']['id'])
        assignee_id = data['data']['task']['assignees'][0]['id']
        happened_at = data['happened_at']
        event_type = data['event']
    except (KeyError, IndexError) as e:
         logger.error(f"Estrutura de dados inválida: {str(e)}")
         return jsonify({"error": "Estrutura de dados inválida"}), 400
    
    new_record = {
        'assignee_id': assignee_id,
        'happened_at': happened_at,
        'action': 'pause' if 'pause' in event_type else 'play',
        'task_id': task_id,
        'recorded_at': datetime.now().isoformat(),
        'tab_token': data.get('data', {}).get('task', {}).get('url', '').split('/')[-1]
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

@app.route('/download-filtered-db')
def download_filtered_file():
    try:
        conn = sqlite3.connect(DB_FILE)
        
        df = pd.read_sql('SELECT * FROM events', conn)
        
        df['happened_at'] = pd.to_datetime(df['happened_at']).dt.tz_localize(None)
        df['date'] = df['happened_at'].dt.date
        df['time'] = df['happened_at'].dt.time
        df['hour'] = df['happened_at'].dt.hour
        
        morning_mask = (df['hour'] >= 6) & (df['hour'] < 12)
        afternoon_mask = (df['hour'] >= 13) & (df['hour'] < 22)
        
        morning_data = df[morning_mask].copy()
        afternoon_data = df[afternoon_mask].copy()
        
        def process_period_data(period_df, period_name=None):
            if period_df.empty:
                return pd.DataFrame()
            
            period_df = period_df.sort_values(['assignee_id', 'happened_at'])
            
            first_plays = period_df[period_df['action'] == 'play'].groupby('assignee_id').first().reset_index()
            
            last_pauses = period_df[period_df['action'] == 'pause'].groupby('assignee_id').last().reset_index()
            
            combined = pd.concat([first_plays, last_pauses]).sort_values(['assignee_id', 'happened_at'])
            
            combined['date'] = combined['happened_at'].dt.strftime('%Y-%m-%d')
            combined['time'] = combined['happened_at'].dt.strftime('%H:%M:%S')
            
            result = combined[[
                'assignee_id', 'date', 'time', 'action', 
                'task_id', 'recorded_at', 'tab_token'
            ]]
            
            return result
        
        morning_processed = process_period_data(morning_data)
        afternoon_processed = process_period_data(afternoon_data)
        
        excel_file = io.BytesIO()
        
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            if not morning_processed.empty:
                morning_processed.to_excel(
                    writer, 
                    index=False, 
                    sheet_name='Manhã (6h-12h)'
                )
            
            if not afternoon_processed.empty:
                afternoon_processed.to_excel(
                    writer, 
                    index=False, 
                    sheet_name='Tarde (13h-22h)'
                )
            
            df.to_excel(
                writer,
                index=False,
                sheet_name='Dados Completos'
            )
        
        excel_file.seek(0)
        conn.close()

        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='filtered_task_events.xlsx'
        )
    except Exception as e:
        logger.error(f"Erro na exportação filtrada para Excel: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/')
def health_check():
    return jsonify({"status": "online", "database": "initialized"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)