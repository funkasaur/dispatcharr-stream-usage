<h1 align="center">Stream Usage for Dispatcharr</h1>

<p align="center">Track streaming traffic by user and channel in Dispatcharr.</p>

<p align="center">
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/github/license/funkasaur/dispatcharr-stream-usage"></a>
  <a href="https://github.com/funkasaur/dispatcharr-stream-usage/commits/"><img alt="Last commit" src="https://img.shields.io/github/last-commit/funkasaur/dispatcharr-stream-usage"></a>
  <a href="https://github.com/funkasaur/dispatcharr-stream-usage/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/funkasaur/dispatcharr-stream-usage"></a>
</p>

## Overview

A [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) plugin that records how much streaming traffic your
server moves: bytes sent to viewers (out) and bytes pulled from providers (in), by user and by channel, per hour,
day, week and month. History is kept in a small SQLite database, so it survives restarts and container updates.

Useful when a VPS plan has a bandwidth cap, or to see who and what uses the most traffic.

## Features

- Samples upstream and downstream byte counters.
- Keeps aggregate history in SQLite and exposes Prometheus metrics.

## Repository structure

```text
dispatcharr-stream-usage/
├── stream_usage/  # Plugin source, manifest and logo
├── build.py       # Versioned plugin ZIP builder
└── tests/         # Offline unit tests
```

## What it counts

Every poll (60 s by default) the plugin reads Dispatcharr's live byte counters from Redis and adds what changed
since the previous poll:

| Source | Direction | Attributed to |
|---|---|---|
| Live proxy clients | out | the Dispatcharr user (or `anonymous`) and the channel |
| Live proxy channel buffers | in | the channel |
| VOD connections | out | the user and the title |
| Catch-up (timeshift) sessions | out | the user if exactly one user watched, else `shared` |

The numbers are sampled, not packet-exact: traffic of a session that starts and ends between two polls is missed,
and the first poll after install or reset only sets a baseline. In testing, totals agreed with the host's own
network counters (vnStat) and Dispatcharr's proxy byte counter within about 4%. Host traffic that doesn't go through
the proxy (M3U/EPG downloads, Redirect stream profiles, backups) is not counted.

## Install

1. Install **Stream Usage** from the Dispatcharr plugin registry, or download `stream_usage-<version>.zip` from the
   [latest release](https://github.com/funkasaur/dispatcharr-stream-usage/releases) and import it under
   Dispatcharr → Plugins (build it yourself with `python3 build.py`, which writes `dist/stream_usage-<version>.zip`).
2. Approve the requested capabilities and enable **Stream Usage**.
3. Press **Apply schedule** (and again after changing *Poll seconds* or *Enable polling*).

Requires Dispatcharr 0.34.0 or later (0.1.0 also ran on 0.33). Requested capabilities:

| Capability | Why |
|---|---|
| Run background tasks | The poll runs on Dispatcharr's Celery beat schedule. |
| Use live proxy internals | Reads the proxy's per-session byte counters from Redis. |
| Access user account data | Maps session user IDs to usernames. |
| Write outside plugin storage | Keeps its SQLite database in `/data/plugin_data/stream_usage`. |
| Run host processes | Only the **Reload Dispatcharr** action, which finds the uwsgi process to reload. |

## Settings

| Setting | Default | Meaning |
|---|---|---|
| Enable polling | on | Turns the scheduled poll on or off (press Apply schedule after changing). |
| Poll seconds | 60 | Poll interval, 15-3600 s. |
| Timezone | blank | IANA zone for "today" and "this month". Blank = Dispatcharr's System Settings time zone, else UTC. |
| Hourly retention days | 62 | How long per-hour rows (and the hour/today/week periods) are kept. |
| Daily retention days | 400 | How long per-day rows are kept. |
| Top users and channels | 5 | How many users/channels the summaries and the Exporter payload list (max 100). |
| Publish for Exporter | off | Writes a summary to Redis for [Dispatcharr Exporter](https://github.com/swvn-dispatch/dispatcharr-exporter) (see below). |

## Actions

- **Show usage**: last hour, today, last 7 days and this month, plus the top users and channels this month.
- **Usage API**: the same as JSON. Pass `{"hours": N}` (up to 1488) for an hourly series.
- **Status**: last poll time, live sessions, poll duration and database size.
- **Apply schedule**: creates or updates the periodic poll task.
- **Reset counters**: deletes all collected history.
- **Reload Dispatcharr**: sends SIGHUP to the uWSGI master so a newly imported version is picked up by the workers.

Reading the JSON from a script:

```
curl -s -X POST -H "Authorization: ApiKey <admin key>" -H "Content-Type: application/json" \
  -d '{"action":"usage","params":{"hours":24}}' https://<your dispatcharr>/api/plugins/plugins/stream_usage/run/
```

## Prometheus

With *Publish for Exporter* on, each poll stores a compact JSON summary in the Redis key
`stream_usage:prometheus:latest`. An Exporter build with the Stream Usage integration turns it into:

- `dispatcharr_usage_bytes_total{direction}`
- `dispatcharr_usage_user_bytes_total{username,direction}`
- `dispatcharr_usage_channel_bytes_total{channel,direction}`
- `dispatcharr_usage_period_bytes{period,direction}`
- `dispatcharr_usage_last_poll_timestamp_seconds`

User and channel series are capped at *Top users and channels*; the rest is summed into `other`, and once `other`
carries bytes the listed names stay fixed so every counter only goes up.

## Notes

- Data lives in `/data/plugin_data/stream_usage/usage.sqlite3` inside the Dispatcharr container.
- The poll task runs on the `dvr` Celery queue. On Dispatcharr 0.33 the default worker rejects plugin tasks as
  unregistered ([#1460](https://github.com/Dispatcharr/Dispatcharr/issues/1460)); the `dvr` worker works on both
  0.33 and 0.34.
- Usernames and channel names appear in the plugin output and the Exporter metrics. Keep `/metrics` private.

## Development

```
python3 -m unittest discover -s tests   # no Django or Redis needed
python3 build.py
```

## License

MIT
