import os, json, uuid, threading, traceback
from datetime import datetime, timezone
from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename

import ticks_to_r2

BASE = os.path.dirname(os.path.abspath(__file__))
UPLOADS = os.path.join(BASE, 'uploads')
HISTORY_FILE = os.path.join(BASE, 'history.json')
os.makedirs(UPLOADS, exist_ok=True)

app = Flask(__name__)

jobs = {}                          # job id -> live job info (lives in memory)
history_lock = threading.Lock()
run_lock = threading.Lock()        # only one conversion runs at a time

def now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

# ---------- history ----------
def load_history():
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

def add_history(entry):
    with history_lock:
        items = load_history()
        items.insert(0, entry)     # newest first
        with open(HISTORY_FILE, 'w') as f:
            json.dump(items, f, indent=2)

def delete_history(entry_id):
    with history_lock:
        items = [h for h in load_history() if h.get('id') != entry_id]
        with open(HISTORY_FILE, 'w') as f:
            json.dump(items, f, indent=2)

# ---------- the job ----------
def run_job(job, path, symbol, mode, delete_after):
    def progress(msg):
        job['log'].append({'time': now(), 'message': msg})

    with run_lock:
        job['status'] = 'running'
        try:
            result = ticks_to_r2.process_file(path, symbol, mode, progress)
            job['status'] = 'done'
            job['result'] = result
            add_history({'id': job['id'], 'status': 'done',
                         'started_at': job['started_at'], **result})
        except Exception as e:
            traceback.print_exc()
            job['status'] = 'failed'
            job['error'] = str(e)
            progress(f'Failed: {e}')
            add_history({'id': job['id'], 'status': 'failed',
                         'started_at': job['started_at'],
                         'file': os.path.basename(path), 'symbol': symbol,
                         'mode': mode, 'error': str(e), 'finished_at': now()})
        finally:
            if delete_after and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

# ---------- routes ----------
@app.route('/')
def index():
    return send_from_directory(BASE, 'index.html')

@app.route('/api/run', methods=['POST'])
def start_run():
    symbol = (request.form.get('symbol') or ticks_to_r2.DEFAULT_SYMBOL).strip().upper()
    mode = request.form.get('mode') or ticks_to_r2.DEFAULT_MODE
    if not symbol.isalnum():
        return jsonify(error='Symbol can only have letters and numbers.'), 400
    if mode not in ('overwrite', 'skip'):
        return jsonify(error='Mode must be overwrite or skip.'), 400

    upload = request.files.get('file')
    pasted = (request.form.get('path') or '').strip().strip('"')

    if upload and upload.filename:
        name = secure_filename(upload.filename)
        path = os.path.join(UPLOADS, f'{uuid.uuid4().hex[:8]}_{name}')
        upload.save(path)
        delete_after = True        # remove the big copy when done
    elif pasted:
        if not os.path.isfile(pasted):
            return jsonify(error=f'File not found: {pasted}'), 400
        path = pasted
        delete_after = False       # never delete a file the user pointed to
    else:
        return jsonify(error='Choose a file or paste a file path.'), 400

    if not path.lower().endswith('.csv'):
        return jsonify(error='Only .csv tick files are supported.'), 400

    job = {'id': uuid.uuid4().hex[:10], 'status': 'queued', 'log': [],
           'started_at': now(), 'result': None, 'error': None}
    jobs[job['id']] = job
    threading.Thread(target=run_job, args=(job, path, symbol, mode, delete_after),
                     daemon=True).start()
    return jsonify(id=job['id'])

@app.route('/api/jobs/<job_id>')
def job_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error='Unknown job.'), 404
    since = int(request.args.get('since', 0))
    return jsonify(status=job['status'], log=job['log'][since:],
                   next=len(job['log']), result=job['result'], error=job['error'])

@app.route('/api/history')
def history():
    return jsonify(load_history())

@app.route('/api/history/<entry_id>', methods=['DELETE'])
def remove_history(entry_id):
    delete_history(entry_id)
    return jsonify(ok=True)

if __name__ == '__main__':
    print('Open http://localhost:5000 in your browser')
    app.run(host='127.0.0.1', port=5000, threaded=True)