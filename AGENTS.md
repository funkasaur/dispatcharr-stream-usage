# Agent instructions for dispatcharr-stream-usage

## Scope

Samples Dispatcharr traffic counters and aggregates streaming usage in SQLite.

## Repository layout

- `stream_usage/plugin.py` contains the runtime plugin and its metadata.
- `stream_usage/plugin.json` is the generated Dispatcharr manifest.
- `stream_usage/logo.png` is bundled with the plugin.
- `build.py` regenerates the manifest and packages a versioned ZIP.
- `tests/test_stream_usage.py` contains the offline unit tests.

## Build and test

- From the root, run `python3 -m unittest discover -s tests`.
- Run `python3 build.py` to write `dist/stream_usage-<version>.zip`.
- The offline tests use the repository's stubs rather than a running Dispatcharr stack.
- The builder gets plugin version, declared capabilities, settings and actions from the
  `Plugin` class.
- Review generated manifest changes alongside edits to plugin metadata.

## Runtime conventions

- Measure changes in sampled Redis counters. Handle resets when deriving deltas.
- Preserve inbound versus outbound attribution and the catch-up sharing rules.
- Use the Dispatcharr System Settings time zone when the plugin setting is blank.
- Keep aggregate history across container restarts and keep metric behavior aligned with the
  README.

## Install and check

- The README documents the plugin registry and release ZIP import in Dispatcharr's Plugins
  UI.
- For a local build, import the versioned ZIP after running the test suite.
- Use the settings and actions documented in the README when verifying a deployment.
- Follow the documented reload steps so the workers use the imported version.
