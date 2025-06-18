import flask
from flask import request, jsonify
import pandas as pd
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = flask.Flask(__name__)

EXCEL_FILE = 'task_events.xlsx'
REQUIRED_FIELDS = ['assignee_id', 'happened_at', 'task_id']
WEBHOOK_SECRET = 'segredodowebhook'

def initialize_excel_file():
     if not os.path.exists(EXCEL_FILE):
          df = pd.DataFrame(columns=[
            'assignee_id', 
            'happened_at', 
            'action', 
            'task_id', 
            'recorded_at',
            'tab_token'
          ])
          df.to_excel(EXCEL_FILE, index=False)
          logger.info("Created new Excel file")

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
     
     action = 'play' if 'play' in request.path else 'pause'
     new_record = {
           'assignee_id': data['assignee_id'],
           'happened_at': data['happened_at'],
           'action': action,
           'task_id': data['task_id'],
           'recorded_at': datetime.now().isoformat(),
           'tab_token': data.get('tab_token', '')
     }

     try:
         df = pd.read_excel(EXCEL_FILE)
         df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)
         df.to_excel(EXCEL_FILE, index=False)
         logger.info(f"Successfully logged event for {data['assignee_id']}")
     except Exception as e:
         logger.error(f"Failed to update Excel file: {str(e)}")
         return jsonify({"error": "Internal server error"}), 500
     
     return jsonify({"status": "success"}), 200

if __name__ == '__main__':
      initialize_excel_file()
      app.run(host='0.0.0.0', port=5000)




            

           
          
    
    

  
     
     

     
     
   
     