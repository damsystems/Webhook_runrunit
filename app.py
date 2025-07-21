import flask
from flask import request, jsonify, send_file
from datetime import datetime
import logging
import pandas as pd
import io
import os
import psycopg2
from psycopg2 import sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
REQUIRED_FIELDS = ['happened_at']
WEBHOOK_SECRET = 'segredodowebhook'

def initialize_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                assignee_id TEXT NOT NULL,
                happened_at TIMESTAMP NOT NULL,
                action TEXT NOT NULL,
                task_id TEXT NOT NULL,
                recorded_at TIMESTAMP NOT NULL,
                tab_token TEXT
            )
        ''')
        conn.commit()
        logger.info("Banco de dados PostgreSQL inicializado com sucesso")
    except Exception as e:
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
        logger.warning("Requisição não é JSON")
        return jsonify({"error": "Request must be JSON"}), 400
    
    data = request.json
    logger.info(f"Payload recebido (raw): {json.dumps(data, indent=2)}")

    REQUIRED_FIELDS = ['happened_at', 'event', 'performer']
    missing_fields = [field for field in REQUIRED_FIELDS if field not in data]
    if missing_fields:
        logger.error(f"Campos obrigatórios faltando: {missing_fields}")
        return jsonify({"error": f"Missing fields: {', '.join(missing_fields)}"}), 400

    try:
        task_id = str(data['data']['task']['id'])
        assignee_id = data['performer']['id']
        happened_at = data['happened_at']
        event_type = data['event']

        action = None
        play_events = ['task_assignment:play', 'task_assignment:start', 'task_play']
        pause_events = ['task_assignment:pause', 'task_assignment:stop', 'task_pause']

        if any(e in event_type for e in play_events):
            action = 'play'
            logger.debug(f"Evento PLAY detectado: {event_type}")
        elif any(e in event_type for e in pause_events):
            action = 'pause'
            logger.debug(f"Evento PAUSE detectado: {event_type}")
        else:
            logger.warning(f"Tipo de evento não reconhecido: {event_type}")
            return jsonify({"error": "Unsupported event type"}), 400

        new_record = {
            'assignee_id': assignee_id,
            'happened_at': happened_at,
            'action': action,
            'task_id': task_id,
            'recorded_at': datetime.now().isoformat(),
            'tab_token': data.get('data', {}).get('task', {}).get('url', '').split('/')[-1]
        }

        logger.info(f"Registrando novo evento: {new_record}")

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO events 
            (assignee_id, happened_at, action, task_id, recorded_at, tab_token)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            new_record['assignee_id'],
            new_record['happened_at'],
            new_record['action'],
            new_record['task_id'],
            new_record['recorded_at'],
            new_record['tab_token']
        ))
        conn.commit()
        
        logger.info(f"Evento registrado com sucesso no PostgreSQL (ID: {cursor.lastrowid})")
        return jsonify({"status": "success", "record": new_record}), 200

    except KeyError as e:
        logger.error(f"Erro de estrutura no payload: {str(e)}")
        return jsonify({"error": f"Invalid payload structure: {str(e)}"}), 400
    except psycopg2.Error as e:
        logger.error(f"Erro no PostgreSQL: {str(e)}")
        conn.rollback()
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/download-db')
def download_file():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        df = pd.read_sql('''
            SELECT 
                assignee_id as "Nome",
                happened_at as "Hora do Evento",
                action as "Ação",
                task_id,
                recorded_at,
                tab_token
            FROM events 
            ORDER BY happened_at
        ''', conn)
        
        turnos_data = []
        df['happened_at'] = pd.to_datetime(df['Hora do Evento'])
        df['date'] = df['happened_at'].dt.date
        
        grouped = df.groupby(['Nome', 'date'])
        
        for (user_id, date), group in grouped:
            group = group.sort_values('happened_at')
            
            turno1_entrada, turno1_saida = "", ""
            turno2_entrada, turno2_saida = "", ""
            turno3_entrada, turno3_saida = "", ""
            
            turnos_data.append({
                'Nome': user_id,
                'Data': date.strftime('%Y-%m-%d'),
                'Turno 1 Entrada': turno1_entrada,
                'Turno 1 Saída': turno1_saida,
                'Turno 2 Entrada': turno2_entrada,
                'Turno 2 Saída': turno2_saida,
                'Turno 3 Entrada': turno3_entrada,
                'Turno 3 Saída': turno3_saida
            })
        
        # Cria o Excel
        excel_file = io.BytesIO()
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
       
            df.to_excel(writer, index=False, sheet_name='Eventos')
            
            turnos_df = pd.DataFrame(turnos_data)
            turnos_df.to_excel(writer, index=False, sheet_name='Turnos')
        
        excel_file.seek(0)
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='task_events.xlsx'
        )
    except Exception as e:
        logger.error(f"Erro na exportação para Excel: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()

@app.route('/')
def health_check():
    return jsonify({"status": "online", "database": "PostgreSQL"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)