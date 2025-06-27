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
        
        df = pd.read_sql('SELECT * FROM events ORDER BY happened_at', conn)
        
        df['happened_at'] = pd.to_datetime(df['happened_at']).dt.tz_localize(None)
        df['date'] = df['happened_at'].dt.date
        
        excel_file = io.BytesIO()
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
 
            df_export = df.copy()
            
            df_export = df_export.drop(['recorded_at', 'task_id', 'tab_token'], axis=1)
            
            df_export = df_export.rename(columns={
                'date': 'Data',
                'assignee_id': 'Nome',
                'happened_at': 'Hora do Evento',
                'action': 'Ação'
            })
            
            df_export.to_excel(writer, index=False, sheet_name='Eventos')
            
            turnos_df = pd.DataFrame(columns=['Nome', 'Data', 'Turno 1', 'Turno 2', 'Turno 3'])
            
            grouped = df.groupby(['assignee_id', 'date'])
            
            for (user_id, date), group in grouped:

                group = group.sort_values('happened_at')
                
                turno1, turno2, turno3 = "", "", ""
                
                mask = (group['happened_at'].dt.hour >= 6) & (group['happened_at'].dt.hour < 12)
                turno_group = group[mask]
                if not turno_group.empty:
                    plays = turno_group[turno_group['action'] == 'play']
                    pauses = turno_group[turno_group['action'] == 'pause']
                    if not plays.empty and not pauses.empty:
                        first_play = plays.iloc[0]['happened_at']
                        last_pause = pauses.iloc[-1]['happened_at']
                        turno1 = f"{first_play.strftime('%H:%M')} - {last_pause.strftime('%H:%M')}"
                
                mask = (group['happened_at'].dt.hour >= 13) & (group['happened_at'].dt.hour < 22)
                turno_group = group[mask]
                if not turno_group.empty:
                    plays = turno_group[turno_group['action'] == 'play']
                    pauses = turno_group[turno_group['action'] == 'pause']
                    if not plays.empty and not pauses.empty:
                        first_play = plays.iloc[0]['happened_at']
                        last_pause = pauses.iloc[-1]['happened_at']
                        turno2 = f"{first_play.strftime('%H:%M')} - {last_pause.strftime('%H:%M')}"
                
                mask = ~((group['happened_at'].dt.hour >= 6) & (group['happened_at'].dt.hour < 12)) & \
                       ~((group['happened_at'].dt.hour >= 13) & (group['happened_at'].dt.hour < 22))
                turno_group = group[mask]
                if not turno_group.empty:
                    plays = turno_group[turno_group['action'] == 'play']
                    pauses = turno_group[turno_group['action'] == 'pause']
                    if not plays.empty and not pauses.empty:
                        first_play = plays.iloc[0]['happened_at']
                        last_pause = pauses.iloc[-1]['happened_at']
                        turno3 = f"{first_play.strftime('%H:%M')} - {last_pause.strftime('%H:%M')}"
                
                turnos_df = turnos_df.append({
                    'Nome': user_id,
                    'Data': date.strftime('%Y-%m-%d'),
                    'Turno 1': turno1,
                    'Turno 2': turno2,
                    'Turno 3': turno3
                }, ignore_index=True)
            
            turnos_df.to_excel(writer, index=False, sheet_name='Turnos')
        
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