# lambda_transform.py
import json
from base64 import b64decode, b64encode
from datetime import datetime

def transform_record(record):
    data = json.loads(b64decode(record['data']).decode('utf-8'))
    # Example transform: ensure types and timestamp
    data['quantity'] = int(data.get('quantity', 0))
    data['price'] = float(data.get('price', 0))
    data['total_price'] = data['quantity'] * data['price']
    # Add ingestion_ts
    data['ingestion_ts'] = datetime.utcnow().isoformat()
    return b64encode((json.dumps(data) + "\n").encode('utf-8')).decode('utf-8')

def handler(event, context):
    output = {'records': []}
    for rec in event['records']:
        try:
            transformed = transform_record(rec)
            output['records'].append({
                'recordId': rec['recordId'],
                'result': 'Ok',
                'data': transformed
            })
        except Exception as e:
            output['records'].append({
                'recordId': rec['recordId'],
                'result': 'ProcessingFailed',
                'data': rec['data']
            })
    return output
