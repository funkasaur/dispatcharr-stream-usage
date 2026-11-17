"""Stream Usage accounting tests; no Django or Redis server required."""
import datetime as dt
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

celery = types.ModuleType('celery')
celery.shared_task = lambda *a, **kw: (lambda fn: fn)
sys.modules.setdefault('celery', celery)
path = pathlib.Path(__file__).resolve().parents[1] / 'stream_usage/plugin.py'
spec = importlib.util.spec_from_file_location('stream_usage_test', path)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class Redis:
    def __init__(self):
        self.data = {}
        self.hashes = {}
        self.locked = False

    def set(self, key, value, nx=False, ex=None):
        if nx and (self.locked or key in self.data):
            return False
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        self.data.pop(key, None)

    def eval(self, script, count, key, token):
        if self.data.get(key) == token:
            self.delete(key)

    def scan_iter(self, match='*', count=1000):
        return iter(self.hashes)

    def hgetall(self, key):
        return self.hashes.get(key, {})


class StreamUsageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.patch = patch.object(mod, 'DATA_DIR', self.temp.name)
        self.patch.start()
        self.dbpatch = patch.object(mod, 'DB', os.path.join(self.temp.name, 'usage.sqlite3'))
        self.dbpatch.start()
        self.redis = Redis()
        self.settings = {f['id']: f['default'] for f in mod.FIELDS}
        self.settings['publish_exporter'] = True
        self.settings['timezone'] = 'America/Chicago'   # a DST zone for the calendar tests
        self.ts = 1794564000
        mod.poll(self.settings, self.redis, 1794000000)   # install baseline: later polls count from here

    def tearDown(self):
        self.dbpatch.stop()
        self.patch.stop()
        self.temp.cleanup()

    def _client(self, amount, started='100'):
        self.redis.hashes['live:channel:abc:clients:1'] = {'bytes_sent': str(amount), 'user_id': '1', 'connected_at': started}

    def _usage(self, now=None):
        con = mod._connect()
        try:
            return mod._usage(con, self.settings, {'hours': 2}, now=now or self.ts)
        finally:
            con.close()

    def test_new_double_reset_vanished_and_restart(self):
        self._client(100)
        self.assertEqual(mod.poll(self.settings, self.redis, self.ts), 1)
        self.assertEqual(self._usage()['periods']['hour']['out'], 100)
        mod.poll(self.settings, self.redis, self.ts + 10)
        self.assertEqual(self._usage(now=self.ts+10)['periods']['hour']['out'], 100)
        self._client(150)
        mod.poll(self.settings, self.redis, self.ts + 20)
        self.assertEqual(self._usage(now=self.ts+20)['periods']['hour']['out'], 150)
        self.redis.hashes.clear()
        mod.poll(self.settings, self.redis, self.ts + 30)
        self._client(40, '200')
        mod.poll(self.settings, self.redis, self.ts + 40)
        self.assertEqual(self._usage(now=self.ts+40)['periods']['hour']['out'], 190)
        self._client(10, '200')   # lower value, same session start: a stale read, adds nothing
        mod.poll(self.settings, self.redis, self.ts + 50)
        self.assertEqual(self._usage(now=self.ts+50)['periods']['hour']['out'], 190)

    def test_payload_api_and_lock(self):
        self._client(80)
        self.settings['top_n'] = 1
        self.assertEqual(mod.poll(self.settings, self.redis, self.ts), 1)
        result = self._usage()
        self.assertEqual(result['v'], 1)
        self.assertEqual(set(result['periods']), {'hour', 'today', 'week', 'month'})
        self.assertEqual(len(result['series']), 2)
        payload = json.loads(self.redis.get(mod.REDIS_KEY))
        self.assertEqual(payload['totals']['out'], 80)
        self.assertEqual(payload['users']['out'][0]['username'], 'anonymous')
        self.assertEqual(payload['channels']['out'][0]['bytes'], 80)
        self.redis.locked = True
        self.assertEqual(mod.poll(self.settings, self.redis, self.ts+60), 'locked')
        self.assertEqual(self._usage(now=self.ts+60)['periods']['hour']['out'], 80)

    def test_exporter_other_counter_never_decreases(self):
        self.settings['top_n'] = 1
        con = mod._connect()
        try:
            mod._add(con, self.ts, 'out', 'alpha', 'One', 100, mod._tz(self.settings))
            con.commit()
            mod._publish(self.redis, con, self.settings, self.ts)
            mod._add(con, self.ts+1, 'out', 'beta', 'Two', 50, mod._tz(self.settings))
            con.commit()
            mod._publish(self.redis, con, self.settings, self.ts+1)
            first = json.loads(self.redis.get(mod.REDIS_KEY))['users']['out']
            mod._add(con, self.ts+2, 'out', 'beta', 'Two', 100, mod._tz(self.settings))
            con.commit()
            mod._publish(self.redis, con, self.settings, self.ts+2)
            second = json.loads(self.redis.get(mod.REDIS_KEY))['users']['out']
            self.assertEqual(first, [{'username': 'alpha', 'bytes': 100}, {'username': 'other', 'bytes': 50}])
            self.assertEqual(second, [{'username': 'alpha', 'bytes': 100}, {'username': 'other', 'bytes': 150}])
        finally:
            con.close()

    def test_timeshift_session_outbound(self):
        self.redis.hashes['timeshift:channel:12_session:metadata'] = {
            'total_bytes': '70', 'init_time': '100', 'channel_name': 'Archive'}
        self.redis.hashes['timeshift:channel:12_session:clients:viewer'] = {'user_id': '1'}
        mod.poll(self.settings, self.redis, self.ts)
        payload = json.loads(self.redis.get(mod.REDIS_KEY))
        self.assertEqual(payload['totals']['out'], 70)
        self.assertEqual(payload['channels']['out'][0]['channel'], 'Archive')

    def test_stale_overlapping_poll_adds_nothing(self):
        self._client(100)
        mod.poll(self.settings, self.redis, self.ts)
        self._client(300)
        newer = mod._scan(self.redis)
        self._client(200)
        older = mod._scan(self.redis)
        con = mod._connect()
        try:
            mod._apply(con, newer, self.ts + 60, self.settings)   # the later read commits first
            mod._apply(con, older, self.ts + 61, self.settings)   # then the overlapping poll's earlier read
        finally:
            con.close()
        self.assertEqual(self._usage(now=self.ts + 61)['periods']['hour']['out'], 300)

    def test_catch_up_viewer_change_is_shared(self):
        meta, client = 'timeshift:channel:12_s:metadata', 'timeshift:channel:12_s:clients:'
        self.redis.hashes[meta] = {'total_bytes': '10', 'init_time': '100'}
        self.redis.hashes[client + 'a'] = {'user_id': '1'}
        mod.poll(self.settings, self.redis, self.ts)
        self.redis.hashes[meta] = {'total_bytes': '40', 'init_time': '100'}
        mod.poll(self.settings, self.redis, self.ts + 60)                  # same single viewer: user 1
        del self.redis.hashes[client + 'a']
        self.redis.hashes[client + 'b'] = {'user_id': '2'}
        self.redis.hashes[meta] = {'total_bytes': '90', 'init_time': '100'}
        mod.poll(self.settings, self.redis, self.ts + 120)                 # viewer changed: shared
        con = mod._connect()
        try:
            rows = dict(con.execute("SELECT user,SUM(amount) FROM hourly WHERE direction='out' GROUP BY user"))
        finally:
            con.close()
        self.assertEqual(rows, {'shared': 10 + 50, 'anonymous': 30})   # user id 1 has no name without Django

    def test_first_poll_baselines_running_sessions(self):
        self.temp2 = tempfile.TemporaryDirectory()
        with patch.object(mod, 'DATA_DIR', self.temp2.name), patch.object(mod, 'DB', os.path.join(self.temp2.name, 'u.sqlite3')):
            self._client(5000)
            mod.poll(self.settings, self.redis, self.ts)
            self.assertEqual(json.loads(self.redis.get(mod.REDIS_KEY))['totals']['out'], 0)
            self._client(5200)
            mod.poll(self.settings, self.redis, self.ts + 60)
            self.assertEqual(json.loads(self.redis.get(mod.REDIS_KEY))['totals']['out'], 200)
        self.temp2.cleanup()

    def test_poll_that_lost_its_lock_rolls_back(self):
        self._client(100)
        real = mod._apply
        def steal(con, found, ts, settings, still_owner):
            self.redis.data[mod.LOCK_KEY] = 'someone-else'
            return real(con, found, ts, settings, still_owner)
        with patch.object(mod, '_apply', steal):
            with self.assertRaises(RuntimeError):
                mod.poll(self.settings, self.redis, self.ts)
        self.redis.data.pop(mod.LOCK_KEY)
        mod.poll(self.settings, self.redis, self.ts + 60)
        self.assertEqual(self._usage(now=self.ts + 60)['periods']['hour']['out'], 100)

    def test_catch_up_with_two_viewers_is_shared(self):
        self.redis.hashes['timeshift:channel:12_session:metadata'] = {'total_bytes': '70', 'init_time': '100'}
        self.redis.hashes['timeshift:channel:12_session:clients:a'] = {'user_id': '1'}
        self.redis.hashes['timeshift:channel:12_session:clients:b'] = {'user_id': '2'}
        mod.poll(self.settings, self.redis, self.ts)
        self.assertEqual(json.loads(self.redis.get(mod.REDIS_KEY))['users']['out'][0]['username'], 'shared')

    def test_reset_is_all_or_nothing(self):
        self._client(100)
        mod.poll(self.settings, self.redis, self.ts)
        tables = ('snapshots', 'samples', 'hourly', 'daily', 'lifetime', 'meta', 'viewers')
        def counts():
            con = mod._connect()
            try:
                return {t: con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0] for t in tables}
            finally:
                con.close()
        before = counts()
        real = mod._connect
        class Failing:
            def __init__(self):
                self.con = real()
            def __getattr__(self, name):
                return getattr(self.con, name)
            def __enter__(self):
                return self.con.__enter__()
            def __exit__(self, *exc):
                return self.con.__exit__(*exc)
            def execute(self, sql, *a):
                if sql == 'DELETE FROM hourly':
                    raise RuntimeError('disk error')
                return self.con.execute(sql, *a)
        with patch.object(mod, '_redis', return_value=self.redis), patch.object(mod, '_connect', Failing):
            with self.assertRaises(RuntimeError):
                mod.Plugin().run('reset', {}, {'settings': self.settings})
        self.redis.data.pop(mod.LOCK_KEY, None)
        self.assertEqual(counts(), before)
        with patch.object(mod, '_redis', return_value=self.redis):
            mod.Plugin().run('reset', {}, {'settings': self.settings})
        self.assertEqual(set(counts().values()), {0})

    def test_calendar_period_queries(self):
        tz = ZoneInfo('America/Chicago')
        before = int(dt.datetime(2026, 10, 31, 23, 58, tzinfo=tz).timestamp())
        after = int(dt.datetime(2026, 11, 1, 0, 2, tzinfo=tz).timestamp())
        self._client(100)
        mod.poll(self.settings, self.redis, before)
        self._client(150)
        mod.poll(self.settings, self.redis, after)
        result = self._usage(now=after)
        self.assertEqual(result['periods']['hour']['out'], 150)
        self.assertEqual(result['periods']['today']['out'], 50)
        self.assertEqual(result['periods']['month']['out'], 50)
        self.assertEqual(result['periods']['week']['out'], 150)

    def test_blank_timezone_falls_back(self):
        self.assertEqual(str(mod._tz({'timezone': 'Not/AZone'})), 'UTC')
        self.assertIn(str(mod._tz({'timezone': ''})), ('UTC',))   # no Django in tests -> UTC

    def test_calendar_and_dst_boundaries(self):
        tz = ZoneInfo('America/Chicago')
        now = int(dt.datetime(2026, 11, 1, 2, 30, tzinfo=tz).timestamp())
        starts = mod._periods(now, tz)
        self.assertEqual(dt.datetime.fromtimestamp(starts['today'], tz).hour, 0)
        self.assertEqual(now - starts['hour'], 3600)
        self.assertEqual(dt.datetime.fromtimestamp(starts['month'], tz).day, 1)
        # The fall-back day is 25 hours; today starts at local midnight.
        self.assertEqual(now - starts['today'], 3*3600 + 30*60)
        dec = int(dt.datetime(2026, 12, 1, 0, 1, tzinfo=tz).timestamp())
        self.assertEqual(dt.datetime.fromtimestamp(mod._periods(dec, tz)['month'], tz).month, 12)


if __name__ == '__main__':
    unittest.main()
