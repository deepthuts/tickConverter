import os, json, uuid, shutil, tempfile, threading, traceback
from datetime import datetime, timezone
from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename

import ticks_to_r2

BASE = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(BASE, 'history.json')

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
def run_job(job, path, mode, cleanup_dir):
    def progress(msg):
        job['log'].append({'time': now(), 'message': msg})

    name = os.path.basename(path)
    with run_lock:
        try:
            if job['stop'].is_set():
                raise ticks_to_r2.Stopped({'file': name, 'symbol': None, 'mode': mode,
                    'ticks': 0, 'candles': 0, 'days_uploaded': 0, 'days_skipped': 0,
                    'first_day': None, 'last_day': None, 'seconds': 0, 'finished_at': now()})
            job['status'] = 'running'
            result = ticks_to_r2.process_file(path, mode, progress, job['stop'].is_set)
            job['status'] = 'done'
            job['result'] = result
            add_history({'id': job['id'], 'status': 'done',
                         'started_at': job['started_at'], **result})
        except ticks_to_r2.Stopped as e:
            partial = e.args[0]
            progress(f"Stopped. {partial['days_uploaded']} day(s) were already sent to R2 and stay there.")
            job['status'] = 'stopped'
            job['result'] = partial
            add_history({'id': job['id'], 'status': 'stopped',
                         'started_at': job['started_at'], **partial})
        except Exception as e:
            traceback.print_exc()
            job['status'] = 'failed'
            job['error'] = str(e)
            progress(f'Failed: {e}')
            add_history({'id': job['id'], 'status': 'failed',
                         'started_at': job['started_at'], 'file': name,
                         'symbol': None, 'mode': mode, 'error': str(e),
                         'finished_at': now()})
        finally:
            if cleanup_dir:
                shutil.rmtree(cleanup_dir, ignore_errors=True)

# ---------- routes ----------
@app.route('/')
def index():
    return send_from_directory(BASE, 'index.html')

@app.route('/api/run', methods=['POST'])
def start_run():
    mode = request.form.get('mode') or ticks_to_r2.DEFAULT_MODE
    if mode not in ('overwrite', 'skip'):
        return jsonify(error='Mode must be overwrite or skip.'), 400

    job_id = uuid.uuid4().hex[:10]
    upload = request.files.get('file')
    pasted = (request.form.get('path') or '').strip().strip('"')
    cleanup_dir = None

    if upload and upload.filename:
        # save to a temporary folder, removed when the run ends.
        # The original file name stays, because the symbol can come from it.
        cleanup_dir = tempfile.mkdtemp(prefix='tickconv_')
        path = os.path.join(cleanup_dir, secure_filename(upload.filename))
        try:
            upload.save(path)
        except Exception:
            shutil.rmtree(cleanup_dir, ignore_errors=True)
            return jsonify(error='The upload was cancelled or failed.'), 400
    elif pasted:
        if not os.path.isfile(pasted):
            return jsonify(error=f'File not found: {pasted}'), 400
        path = pasted              # never deleted, since the user pointed to it
    else:
        return jsonify(error='Choose a file or paste a file path.'), 400

    if not path.lower().endswith('.csv'):
        if cleanup_dir:
            shutil.rmtree(cleanup_dir, ignore_errors=True)
        return jsonify(error='Only .csv tick files are supported.'), 400

    job = {'id': job_id, 'status': 'queued', 'log': [], 'stop': threading.Event(),
           'started_at': now(), 'result': None, 'error': None}
    jobs[job_id] = job
    threading.Thread(target=run_job, args=(job, path, mode, cleanup_dir),
                     daemon=True).start()
    return jsonify(id=job_id)

@app.route('/api/jobs/<job_id>')
def job_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error='Unknown job.'), 404
    since = int(request.args.get('since', 0))
    return jsonify(status=job['status'], log=job['log'][since:],
                   next=len(job['log']), result=job['result'], error=job['error'])

@app.route('/api/jobs/<job_id>/stop', methods=['POST'])
def stop_job(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error='Unknown job.'), 404
    if job['status'] in ('queued', 'running'):
        job['stop'].set()
        job['log'].append({'time': now(), 'message': 'Stop requested. Finishing the current step...'})
    return jsonify(ok=True)

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