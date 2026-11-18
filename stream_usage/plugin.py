"""Persistent sampled traffic accounting for Dispatcharr streams."""
import datetime as dt
import json
import os
import sqlite3
import time
import uuid
from collections import defaultdict
from zoneinfo import ZoneInfo

from celery import shared_task

KEY = 'stream_usage'
TASK = 'stream_usage.poll'
DATA_DIR = '/data/plugin_data/stream_usage'
DB = os.path.join(DATA_DIR, 'usage.sqlite3')
REDIS_KEY = 'stream_usage:prometheus:latest'
LOCK_KEY = 'stream_usage:poll:lock'
LOCK_SECONDS = 600   # a poll that loses the lock rolls back instead of committing (see _owns)
FIELDS = [
    {'id': 'poll_enabled', 'label': 'Enable polling', 'type': 'boolean', 'default': True},
    {'id': 'poll_seconds', 'label': 'Poll seconds', 'type': 'number', 'default': 60},
    {'id': 'timezone', 'label': 'Timezone', 'type': 'string', 'default': '',
     'help_text': 'IANA name, e.g. Europe/Berlin. Blank uses Dispatcharr\'s System Settings time zone, else UTC.'},
    {'id': 'hourly_days', 'label': 'Hourly retention days', 'type': 'number', 'default': 62},
    {'id': 'daily_days', 'label': 'Daily retention days', 'type': 'number', 'default': 400},
    {'id': 'top_n', 'label': 'Top users and channels', 'type': 'number', 'default': 5},
    {'id': 'publish_exporter', 'label': 'Publish for Exporter', 'type': 'boolean', 'default': False},
]
ACTIONS = [
    {'id': 'show_usage', 'label': 'Show usage', 'button_label': 'Show'},
    {'id': 'usage', 'label': 'Usage API', 'button_label': 'Get JSON'},
    {'id': 'status', 'label': 'Status', 'button_label': 'Status'},
    {'id': 'apply_schedule', 'label': 'Apply schedule', 'button_label': 'Apply'},
    {'id': 'reset', 'label': 'Reset counters', 'button_label': 'Reset', 'button_color': 'red',
     'confirm': {'required': True, 'title': 'Reset usage?', 'message': 'Delete all collected stream usage history?'}},
    {'id': 'reload_dispatcharr', 'label': 'Reload Dispatcharr', 'button_label': 'Reload', 'button_color': 'orange',
     'confirm': {'required': True, 'title': 'Reload Dispatcharr?', 'message': 'Running plugin jobs stop and streams reconnect.'}},
]


def _text(value):
    return value.decode('utf-8', 'replace') if isinstance(value, bytes) else str(value)


def _field(data, name, default=''):
    return _text(data.get(name, data.get(name.encode(), default)))


def _integer(value):
    try:
        return max(0, int(value))
    except (ValueError, TypeError):
        return 0


def _size(n):
    for unit, scale in (('TB', 1e12), ('GB', 1e9), ('MB', 1e6), ('KB', 1e3)):
        if n >= scale:
            return f'{n / scale:.1f} {unit}'
    return f'{n} B'


def _tz(settings):
    name = settings.get('timezone')
    if not name:
        # Dispatcharr keeps its System Settings zone in CoreSettings; Django's TIME_ZONE is always UTC on 0.34.
        try:
            from core.models import CoreSettings
            name = CoreSettings.get_system_time_zone()
        except Exception:
            try:
                from django.conf import settings as django_settings
                name = django_settings.TIME_ZONE
            except Exception:
                name = 'UTC'
    try:
        return ZoneInfo(name or 'UTC')
    except Exception:
        return ZoneInfo('UTC')


def _connect():
    os.makedirs(DATA_DIR, exist_ok=True)
    con = sqlite3.connect(DB, timeout=30, isolation_level=None)
    con.execute('PRAGMA journal_mode=WAL')
    con.executescript('''
    CREATE TABLE IF NOT EXISTS snapshots (key TEXT PRIMARY KEY, value INTEGER NOT NULL, seen INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS samples (ts INTEGER, direction TEXT, user TEXT, channel TEXT, amount INTEGER);
    CREATE INDEX IF NOT EXISTS samples_ts ON samples(ts);
    CREATE TABLE IF NOT EXISTS hourly (hour INTEGER, direction TEXT, user TEXT, channel TEXT, amount INTEGER,
       PRIMARY KEY(hour,direction,user,channel));
    CREATE TABLE IF NOT EXISTS daily (day TEXT, direction TEXT, user TEXT, channel TEXT, amount INTEGER,
       PRIMARY KEY(day,direction,user,channel));
    CREATE TABLE IF NOT EXISTS lifetime (direction TEXT, user TEXT, channel TEXT, amount INTEGER,
       PRIMARY KEY(direction,user,channel));
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS viewers (key TEXT PRIMARY KEY, users TEXT NOT NULL);
    ''')
    return con


def _settings():
    from apps.plugins.models import PluginConfig
    cfg = PluginConfig.objects.filter(key=KEY).first()
    result = {f['id']: f['default'] for f in FIELDS}
    if cfg and cfg.settings:
        result.update(cfg.settings)
    return result, bool(cfg and cfg.enabled)


def _redis():
    from core.utils import RedisClient
    return RedisClient.get_client()


def _lock(redis):
    token = uuid.uuid4().hex
    if not redis.set(LOCK_KEY, token, nx=True, ex=LOCK_SECONDS):
        return None
    return token


def _owns(redis, token):
    return _text(redis.get(LOCK_KEY) or b'') == token


def _unlock(redis, token):
    redis.eval("if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) end return 0", 1, LOCK_KEY, token)


def _scan(redis):
    """One SCAN pass; read only live client, channel and VOD hashes."""
    found = {}
    timeshift = {}
    timeshift_users = {}
    for raw in redis.scan_iter(match='*', count=1000):
        key = _text(raw)
        parts = key.split(':')
        if ((len(parts) == 5 and parts[0] in ('live', 'ts_proxy') and parts[1] == 'channel' and parts[3] == 'clients')
                or (len(parts) == 4 and parts[0] in ('live', 'ts_proxy') and parts[1] == 'channel' and parts[3] == 'metadata')
                or (parts[0] == 'timeshift' and len(parts) in (4, 5) and parts[1] == 'channel'
                    and parts[3] in ('metadata', 'clients'))
                or key.startswith('vod_persistent_connection:')):
            data = redis.hgetall(raw) or {}
            if not data:
                continue
            if parts[0] == 'timeshift':
                if len(parts) == 4:
                    timeshift[key] = data
                elif len(parts) == 5:
                    timeshift_users.setdefault(parts[2], set()).add(_field(data, 'user_id', '0'))
            elif key.startswith('vod_persistent_connection:'):
                found[key] = (_integer(_field(data, 'bytes_sent', 0)), 'out', _field(data, 'user_id', '0'),
                              _field(data, 'content_name', 'VOD'), _field(data, 'created_at', '0'))
            elif parts[3] == 'clients':
                found[key] = (_integer(_field(data, 'bytes_sent', 0)), 'out', _field(data, 'user_id', '0'), parts[2],
                              _field(data, 'connected_at', '0'))
            else:
                found[key] = (_integer(_field(data, 'total_bytes', 0)), 'in', '', parts[2],
                              _field(data, 'init_time', '0'))
    for key, data in timeshift.items():
        session = key.split(':')[2]
        channel = _field(data, 'channel_uuid') or _field(data, 'channel_name', 'Catch-up')
        # Catch-up has one byte counter per channel, so bytes go to a user only when one user is watching.
        viewers = timeshift_users.get(session, set())
        found[key] = (_integer(_field(data, 'total_bytes', 0)), 'out', ','.join(sorted(viewers)) or 'shared',
                      channel, _field(data, 'init_time', '0'))
    return found


def _names(found):
    ids = {_integer(v[2]) for v in found.values() if v[2] and v[2] != 'shared'}
    uuids = set()
    for key, value in found.items():
        if key.startswith(('live:channel:', 'ts_proxy:channel:', 'timeshift:channel:')):
            try:
                uuids.add(str(uuid.UUID(value[3])))
            except (ValueError, TypeError, AttributeError):
                pass
    users, channels = {'shared': 'shared'}, {}
    try:
        from apps.accounts.models import User
        users.update({str(x['id']): x['username'] for x in User.objects.filter(id__in=ids).values('id', 'username')})
    except Exception:
        pass
    try:
        from apps.channels.models import Channel
        channels = {str(x['uuid']): x['name'] for x in Channel.objects.filter(uuid__in=uuids).values('uuid', 'name')}
    except Exception:
        pass
    return users, channels


def _add(con, ts, direction, user, channel, amount, tz, sample=True):
    if amount <= 0:
        return
    hour = ts // 3600 * 3600
    day = dt.datetime.fromtimestamp(ts, tz).date().isoformat()
    if sample:
        con.execute('INSERT INTO samples VALUES (?,?,?,?,?)', (ts, direction, '', '', amount))
    for table, cols, vals in (
        ('hourly', 'hour,direction,user,channel', (hour, direction, user, channel)),
        ('daily', 'day,direction,user,channel', (day, direction, user, channel)),
        ('lifetime', 'direction,user,channel', (direction, user, channel))):
        marks = ','.join('?' for _ in vals)
        con.execute(f'INSERT INTO {table} ({cols},amount) VALUES ({marks},?) ON CONFLICT DO UPDATE SET amount=amount+excluded.amount', (*vals, amount))


def _apply(con, found, ts, settings, still_owner=lambda: True):
    users, channels = _names(found)
    tz = _tz(settings)
    poll_totals = defaultdict(int)
    with con:
        # Snapshots are read and written in one write transaction, so overlapping polls run one after the other.
        con.execute('BEGIN IMMEDIATE')
        previous = {row[0]: row[1] for row in con.execute('SELECT key,value FROM snapshots')}
        seen_by = dict(con.execute('SELECT key,users FROM viewers'))
        # First poll after install or reset: baseline running sessions, count only traffic seen from now on.
        first = con.execute("SELECT 1 FROM meta WHERE key='last_poll'").fetchone() is None
        for key, (value, direction, uid, channel, started) in found.items():
            # Start time distinguishes a new session reusing the same Redis key, so within one key the counter only
            # grows; a lower value is a stale read from an overlapping poll and adds nothing.
            snap_key = key + '@' + started
            old = previous.get(snap_key)
            delta = 0 if first else value if old is None else max(0, value - old)
            if key.startswith('timeshift:'):
                # One counter per catch-up channel: bytes go to a user only if that same single user was there last poll.
                con.execute('INSERT INTO viewers VALUES (?,?) ON CONFLICT(key) DO UPDATE SET users=excluded.users', (snap_key, uid))
                uid = uid if ',' not in uid and seen_by.get(snap_key, uid) == uid else 'shared'
                if old is None and not first:
                    uid = 'shared'
                value = max(value, old or 0)
            _add(con, ts, direction, users.get(uid, 'anonymous') if direction == 'out' else '',
                 channels.get(channel, channel), delta, tz, sample=False)
            poll_totals[direction] += delta
            con.execute('INSERT INTO snapshots VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value),seen=excluded.seen',
                        (snap_key, value, ts))
        for direction, amount in poll_totals.items():
            if amount:
                con.execute('INSERT INTO samples VALUES (?,?,?,?,?)', (ts, direction, '', '', amount))
        # Keep vanished sessions a day so a session that reappears is not counted from zero.
        con.execute('DELETE FROM snapshots WHERE seen < ?', (ts - 86400,))
        con.execute('DELETE FROM viewers WHERE key NOT IN (SELECT key FROM snapshots)')
        con.execute('DELETE FROM samples WHERE ts < ?', (ts - max(1, _integer(settings.get('hourly_days'))) * 86400,))
        con.execute('DELETE FROM hourly WHERE hour < ?', (ts - max(1, _integer(settings.get('hourly_days'))) * 86400,))
        cutoff = (dt.datetime.fromtimestamp(ts, tz).date() - dt.timedelta(days=max(1, _integer(settings.get('daily_days'))))).isoformat()
        con.execute('DELETE FROM daily WHERE day < ?', (cutoff,))
        con.execute("INSERT INTO meta VALUES ('last_poll',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(ts),))
        if not still_owner():
            raise RuntimeError('poll lock lost; rolled back')
        con.execute("INSERT INTO meta VALUES ('sessions',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(sum(1 for item in found.values() if item[1] == 'out')),))


def _periods(ts, tz):
    local = dt.datetime.fromtimestamp(ts, tz)
    midnight = dt.datetime.combine(local.date(), dt.time(), tz)
    month = midnight.replace(day=1)
    return {'hour': ts-3600, 'today': int(midnight.timestamp()), 'week': ts-7*86400,
            'month': int(month.timestamp())}


def _usage(con, settings, params=None, now=None):
    params = params or {}
    now = int(time.time() if now is None else now)
    tz = _tz(settings)
    starts = _periods(now, tz)
    periods = {}
    for name, start in starts.items():
        # Daily archive supports calendar periods beyond hourly retention.
        if name == 'month' and start < now - max(1, _integer(settings.get('hourly_days'))) * 86400:
            rows = con.execute('SELECT direction,SUM(amount) FROM daily WHERE day>=? GROUP BY direction',
                               (dt.datetime.fromtimestamp(start, tz).date().isoformat(),))
        else:
            rows = con.execute('SELECT direction,SUM(amount) FROM samples WHERE ts>=? GROUP BY direction', (start,))
        values = dict(rows)
        periods[name] = {'out': values.get('out', 0), 'in': values.get('in', 0)}
    top = max(1, min(100, _integer(params.get('top', settings.get('top_n', 5)))))
    month_start = starts['month']
    by = {}
    for field in ('user', 'channel'):
        rows = con.execute(f'''SELECT {field}, direction, SUM(amount) FROM hourly WHERE hour>=? AND {field}!=''
                             GROUP BY {field},direction''', (month_start,))
        grouped = defaultdict(lambda: {'out': 0, 'in': 0})
        for label, direction, amount in rows:
            grouped[label][direction] = amount
        by[field] = [{'name': name, **counts} for name, counts in
                     sorted(grouped.items(), key=lambda x: (-sum(x[1].values()), x[0]))[:top]]
    result = {'v': 1, 'generated': now, 'tz': str(tz), 'periods': periods,
              'by_user': by['user'], 'by_channel': by['channel']}
    if 'hours' in params:
        hours = max(1, min(1488, _integer(params['hours'])))
        first = now // 3600 * 3600 - (hours-1)*3600
        buckets = defaultdict(lambda: {'out': 0, 'in': 0})
        for hour, direction, amount in con.execute('SELECT hour,direction,SUM(amount) FROM hourly WHERE hour>=? GROUP BY hour,direction', (first,)):
            buckets[hour][direction] = amount
        result['series'] = [{'hour': h, **buckets[h]} for h in range(first, first+hours*3600, 3600)]
    return result


def _bounded_lifetime(con, column, label, direction, top):
    rows = list(con.execute(f"SELECT {column},SUM(amount) FROM lifetime WHERE direction=? AND {column}!='' GROUP BY {column} ORDER BY SUM(amount) DESC, {column}",
                            (direction,)))
    pin_key = f'pinned_{column}_{direction}'
    other_key = f'other_{column}_{direction}'
    old = con.execute('SELECT value FROM meta WHERE key=?', (pin_key,)).fetchone()
    old_other = con.execute('SELECT value FROM meta WHERE key=?', (other_key,)).fetchone()
    pinned = json.loads(old[0]) if old else []
    # New labels can be admitted while other has never carried bytes. Once it
    # has, membership is fixed so its counter cannot decrease.
    if not old or not old_other or _integer(old_other[0]) == 0:
        pinned += [name for name, _ in rows if name not in pinned and name != 'other'][:max(0, top - len(pinned))]
    amounts = dict(rows)
    result = [{label: name, 'bytes': amounts.get(name, 0)} for name in pinned]
    other = sum(amount for name, amount in rows if name not in pinned)
    if other:
        result.append({label: 'other', 'bytes': other})
    con.execute('INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                (pin_key, json.dumps(pinned)))
    con.execute('INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                (other_key, str(other)))
    return result


def _publish(redis, con, settings, ts):
    if not settings.get('publish_exporter'):
        redis.delete(REDIS_KEY)
        return
    usage = _usage(con, settings, now=ts)
    totals = dict(con.execute('SELECT direction,SUM(amount) FROM lifetime GROUP BY direction'))
    top = max(1, min(100, _integer(settings.get('top_n', 5))))
    users, channels = {}, {}
    for direction in ('out', 'in'):
        users[direction] = _bounded_lifetime(con, 'user', 'username', direction, top)
        channels[direction] = _bounded_lifetime(con, 'channel', 'channel', direction, top)
    con.commit()
    payload = {'v': 1, 'generated': ts, 'last_poll': ts, 'totals': {'out': totals.get('out', 0), 'in': totals.get('in', 0)},
               'periods': usage['periods'], 'users': users, 'channels': channels}
    redis.set(REDIS_KEY, json.dumps(payload, separators=(',', ':')))


def poll(settings=None, redis=None, now=None):
    settings = settings or _settings()[0]
    redis = redis or _redis()
    token = _lock(redis)
    if not token:
        return 'locked'
    try:
        ts = int(time.time() if now is None else now)
        t0 = time.time()
        found = _scan(redis)
        scan_ms = int((time.time() - t0) * 1000)
        con = _connect()
        try:
            _apply(con, found, ts, settings, lambda: _owns(redis, token))
            _publish(redis, con, settings, ts)
            for name, ms in (('scan_ms', scan_ms), ('poll_ms', int((time.time() - t0) * 1000))):
                con.execute("INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (name, str(ms)))
        finally:
            con.close()
        return len(found)
    finally:
        _unlock(redis, token)


@shared_task(name=TASK)
def scheduled_poll():
    settings, enabled = _settings()
    if enabled and settings.get('poll_enabled'):
        return poll(settings)
    return 'disabled'


def _schedule(settings):
    from django_celery_beat.models import IntervalSchedule, PeriodicTask
    seconds = max(15, min(3600, _integer(settings.get('poll_seconds', 60))))
    interval, _ = IntervalSchedule.objects.get_or_create(every=seconds, period=IntervalSchedule.SECONDS)
    PeriodicTask.objects.update_or_create(name='Stream Usage poll', defaults={
        # dvr queue: on Dispatcharr 0.33 the default worker rejects plugin tasks as unregistered (fixed in 0.34, #1460).
        'task': TASK, 'interval': interval, 'crontab': None, 'enabled': bool(settings.get('poll_enabled')), 'queue': 'dvr'})
    return f'Poll every {seconds} seconds on dvr queue.'


class Plugin:
    name = 'Stream Usage'
    version = '0.2.2'
    description = 'Track sampled streaming traffic by user and channel.'
    author = 'funkasaur'
    license = 'MIT'
    help_url = 'https://github.com/funkasaur/dispatcharr-stream-usage'
    repo_url = 'https://github.com/funkasaur/dispatcharr-stream-usage'
    manifest_version = 2
    min_dispatcharr_version = 'v0.34.0'
    # background_tasks: Celery beat poll; proxy_internals: proxy session hashes in Redis; user_data: usernames;
    # filesystem_write: SQLite under /data/plugin_data; subprocess: pgrep for the Reload Dispatcharr action.
    capabilities = ['background_tasks', 'proxy_internals', 'user_data', 'filesystem_write', 'subprocess']
    fields = FIELDS
    actions = ACTIONS

    def run(self, action, params, context):
        settings = {f['id']: f['default'] for f in FIELDS}
        settings.update(context.get('settings') or {})
        if action == 'apply_schedule':
            return {'status': 'ok', 'message': _schedule(settings)}
        if action == 'reload_dispatcharr':
            import signal
            import subprocess
            import threading
            pid = (subprocess.run(['pgrep', '-o', 'uwsgi'], capture_output=True, text=True).stdout or '').strip()
            if not pid:
                return {'status': 'warning', 'message': 'No uWSGI master found; restart the container instead.'}
            def reload_later():
                time.sleep(2)
                os.kill(int(pid), signal.SIGHUP)
            threading.Thread(target=reload_later, daemon=True).start()
            return {'status': 'ok', 'message': 'Dispatcharr reload queued.'}
        if action == 'reset':
            redis = _redis()
            token = _lock(redis)
            if not token:
                return {'status': 'warning', 'message': 'Poll in progress; retry.'}
            try:
                con = _connect()
                with con:
                    con.execute('BEGIN IMMEDIATE')   # all tables or none (connection is in autocommit mode)
                    for table in ('snapshots', 'samples', 'hourly', 'daily', 'lifetime', 'meta', 'viewers'):
                        con.execute(f'DELETE FROM {table}')
                con.close()
                redis.delete(REDIS_KEY)
            finally:
                _unlock(redis, token)
            return {'status': 'ok', 'message': 'Usage counters reset.'}
        con = _connect()
        try:
            if action == 'status':
                meta = dict(con.execute('SELECT key,value FROM meta'))
                return {'status': 'ok', 'message': f"Last poll: {meta.get('last_poll', 'never')}; sessions: {meta.get('sessions', '0')}; poll {meta.get('poll_ms', '?')} ms, scan {meta.get('scan_ms', '?')} ms; database: {os.path.getsize(DB)} bytes"}
            if action in ('usage', 'show_usage'):
                result = _usage(con, settings, params)
                if action == 'usage':
                    return result
                labels = {'hour': 'Last hour', 'today': 'Today', 'week': 'Last 7 days', 'month': 'This month'}
                lines = [f"{labels[name]}: out {_size(v['out'])} / in {_size(v['in'])}" for name, v in result['periods'].items()]
                for category, key in (('Users this month', 'by_user'), ('Channels this month', 'by_channel')):
                    lines.append(category + ': ' + (', '.join(f"{x['name']} {_size(x['out'] + x['in'])}" for x in result[key][:5]) or 'none'))
                return {'status': 'ok', 'message': '\n'.join(lines)}
        finally:
            con.close()
        return {'status': 'error', 'message': f'Unknown action: {action}'}
