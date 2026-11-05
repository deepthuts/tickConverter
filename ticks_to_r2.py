import os, sys, re, json, time
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
DEFAULT_MODE = os.environ.get('MODE', 'overwrite')     # 'overwrite' or 'skip'
CHUNK_ROWS = 2_000_000
SYMBOL_COLS = ('symbol', 'instrument', 'pair', 'ticker')
# -----------------------------------------------

_s3 = None

class Stopped(Exception):
    """Raised when the user presses Stop. args[0] holds a summary of the work done so far."""

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

def clean_symbol(value):
    return re.sub(r'[^A-Za-z0-9]', '', str(value)).upper()

def symbol_from_filename(path):
    name = os.path.splitext(os.path.basename(path))[0]
    m = re.match(r'[A-Za-z0-9]{3,}', name)
    return clean_symbol(m.group(0)) if m else None

def build_candles(path, progress=print, should_stop=lambda: False):
    parts, symbols, total_ticks = [], set(), 0
    for i, ch in enumerate(pd.read_csv(path, chunksize=CHUNK_ROWS), start=1):
        if should_stop():
            raise Stopped()
        ch.columns = [c.strip().lower() for c in ch.columns]

        # look for a symbol column in the CSV
        for col in SYMBOL_COLS:
            if col in ch.columns:
                symbols.update(s for s in (clean_symbol(v) for v in ch[col].dropna().unique()) if s)
                break

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
    return df.drop(columns='spr_sum'), total_ticks, symbols

def save_days(df, symbol, mode, progress, should_stop, stats):
    groups = list(df.groupby(df.index.date))

    for n, (day, d) in enumerate(groups, start=1):
        if should_stop():
            raise Stopped()
        day = str(day)
        key = f'ohlc/30s/{symbol}/{day}.json'

        if mode == 'skip' and exists_in_r2(key):
            stats['skipped'].append(day)
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

        get_s3().put_object(
            Bucket=BUCKET, Key=key,
            Body=body.encode(), ContentType='application/json',
            CacheControl='public, max-age=3600')

        stats['done'].append(day)
        stats['candles'] += len(rows)
        progress(f'Uploaded {day}: {len(rows):,} candles  [{n}/{len(groups)}]')

def _summary(path, ctx, mode, stats, started):
    done = stats['done']
    return {
        'file': os.path.basename(path),
        'symbol': ctx['symbol'],
        'mode': mode,
        'ticks': ctx['ticks'],
        'candles': stats['candles'],
        'days_uploaded': len(done),
        'days_skipped': len(stats['skipped']),
        'first_day': done[0] if done else None,
        'last_day': done[-1] if done else None,
        'seconds': round(time.time() - started, 1),
        'finished_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }

def process_file(path, mode=None, progress=print, should_stop=lambda: False):
    """Turn one tick CSV into daily 30s candle files and send them to R2.
    The symbol comes from the CSV. Returns a summary dict.
    Raises Stopped if the user pressed Stop, and another error if something fails."""
    mode = mode or DEFAULT_MODE
    started = time.time()
    stats = {'done': [], 'skipped': [], 'candles': 0}
    ctx = {'symbol': None, 'ticks': 0}
    try:
        progress(f'Starting {os.path.basename(path)} (mode: {mode})')
        df, ctx['ticks'], symbols = build_candles(path, progress, should_stop)

        # work out the symbol before anything goes to R2
        if len(symbols) > 1:
            raise ValueError('This file holds more than one symbol (' + ', '.join(sorted(symbols)) +
                             '). Split it so each file has one symbol.')
        if symbols:
            ctx['symbol'], source = next(iter(symbols)), 'the CSV symbol column'
        else:
            ctx['symbol'], source = symbol_from_filename(path), 'the file name (no symbol column in the CSV)'
        if not ctx['symbol']:
            raise ValueError('Could not find a symbol. Add a symbol column to the CSV or start the file name with the symbol.')
        progress(f"Symbol: {ctx['symbol']} (from {source})")

        progress(f'Built {len(df):,} candles. Sending days to R2')
        save_days(df, ctx['symbol'], mode, progress, should_stop, stats)
        progress(f'Finished in {round(time.time() - started, 1)}s')
    except Stopped:
        raise Stopped(_summary(path, ctx, mode, stats, started))
    return _summary(path, ctx, mode, stats, started)

# Run it straight from the terminal if you like:
#   python ticks_to_r2.py C:\ticks\EURUSD_2025.csv
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python ticks_to_r2.py <tick_file.csv>')
        sys.exit(1)
    print(json.dumps(process_file(sys.argv[1]), indent=2))