import os, sys, json, time
import pandas as pd
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # fine if you set the variables in Windows instead

# ---------- settings (read from .env) ----------
BUCKET = os.environ.get('R2_BUCKET', 'your-bucket-name')
OUT_ROOT = os.environ.get('OUT_ROOT', 'candles_30s')   # local copies of the daily JSON files
DEFAULT_SYMBOL = os.environ.get('SYMBOL', 'EURUSD')
DEFAULT_MODE = os.environ.get('MODE', 'overwrite')     # 'overwrite' or 'skip'
CHUNK_ROWS = 2_000_000
# -----------------------------------------------

_s3 = None

def get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client('s3',
            endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ['R2_KEY'],
            aws_secret_access_key=os.environ['R2_SECRET'])
    return _s3

def exists_in_r2(key):
    try:
        get_s3().head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise

def build_candles(path, progress=print):
    parts = []
    total_ticks = 0
    for i, ch in enumerate(pd.read_csv(path, chunksize=CHUNK_ROWS), start=1):
        ch.columns = [c.strip().lower() for c in ch.columns]
        ch['time'] = pd.to_datetime(ch['time'], utc=True)
        ch['bid'] = pd.to_numeric(ch['bid'], errors='coerce')
        ch['ask'] = pd.to_numeric(ch['ask'], errors='coerce')
        ch = ch.dropna(subset=['bid', 'ask'])
        ch['spr'] = ch['ask'] - ch['bid']
        total_ticks += len(ch)
        g = ch.set_index('time').resample('30s')
        c = g['bid'].ohlc()
        c['volume'] = g['bid'].count()      # tick volume = number of ticks
        c['spr_sum'] = g['spr'].sum()
        parts.append(c)
        progress(f'Read chunk {i} ({total_ticks:,} ticks so far)')

    progress('Merging chunks and building candles')
    df = pd.concat(parts)
    # one candle can split across two chunks, so merge them
    df = df.groupby(level=0).agg(
        open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'),
        volume=('volume', 'sum'), spr_sum=('spr_sum', 'sum'))
    df = df[df['volume'] > 0]
    df['spread'] = df['spr_sum'] / df['volume']   # average spread
    return df.drop(columns='spr_sum'), total_ticks

def save_days(df, symbol, mode, progress=print):
    out_dir = os.path.join(OUT_ROOT, symbol)
    os.makedirs(out_dir, exist_ok=True)

    days_done, days_skipped, candles = [], [], 0
    groups = list(df.groupby(df.index.date))

    for n, (day, d) in enumerate(groups, start=1):
        day = str(day)
        key = f'ohlc/30s/{symbol}/{day}.json'

        if mode == 'skip' and exists_in_r2(key):
            days_skipped.append(day)
            progress(f'Skipped {day} (already in R2)  [{n}/{len(groups)}]')
            continue

        rows = [{
            'timestamp': t.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'open': round(o, 6), 'high': round(h, 6),
            'low': round(l, 6), 'close': round(c, 6),
            'volume': int(v), 'spread': round(s, 6),
        } for t, o, h, l, c, v, s in
            d[['open', 'high', 'low', 'close', 'volume', 'spread']].itertuples()]

        payload = {
            'symbol': symbol,
            'interval_seconds': 30,
            'date': day,
            'generated_at_utc': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'rows': rows,
        }
        body = json.dumps(payload, separators=(',', ':'))

        with open(os.path.join(out_dir, f'{day}.json'), 'w') as f:
            f.write(body)

        get_s3().put_object(
            Bucket=BUCKET, Key=key,
            Body=body.encode(), ContentType='application/json',
            CacheControl='public, max-age=3600')

        days_done.append(day)
        candles += len(rows)
        progress(f'Uploaded {day}: {len(rows):,} candles  [{n}/{len(groups)}]')

    return days_done, days_skipped, candles

def process_file(path, symbol=None, mode=None, progress=print):
    """Turn one tick CSV into daily 30s candle files and send them to R2.
    Returns a summary dict. Raises an error if something fails."""
    symbol = (symbol or DEFAULT_SYMBOL).upper()
    mode = mode or DEFAULT_MODE
    started = time.time()
    progress(f'Starting {os.path.basename(path)} ({symbol}, mode: {mode})')

    df, total_ticks = build_candles(path, progress)
    progress(f'Built {len(df):,} candles. Sending days to R2')
    days_done, days_skipped, candles = save_days(df, symbol, mode, progress)

    seconds = round(time.time() - started, 1)
    progress(f'Finished in {seconds}s')
    return {
        'file': os.path.basename(path),
        'symbol': symbol,
        'mode': mode,
        'ticks': total_ticks,
        'candles': candles,
        'days_uploaded': len(days_done),
        'days_skipped': len(days_skipped),
        'first_day': days_done[0] if days_done else None,
        'last_day': days_done[-1] if days_done else None,
        'seconds': seconds,
        'finished_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }

# Run it straight from the terminal if you like:
#   python ticks_to_r2.py C:\ticks\EURUSD_2025.csv EURUSD
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python ticks_to_r2.py <tick_file.csv> [SYMBOL]')
        sys.exit(1)
    result = process_file(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
    print(json.dumps(result, indent=2))