---
name: package-dispatcharr-plugin
description: Test and package the Dispatcharr plugin for a release or local import.
---

# Test and package the Dispatcharr plugin for a release or local import

1. From the repo root, run `python3 -m unittest discover -s tests`.
2. Run `python3 build.py`. It generates `stream_usage/plugin.json` from the plugin class and writes `dist/stream_usage-<version>.zip`.
3. Review the manifest diff. Check that the archive contains the generated manifest, source and logo beneath `stream_usage/`.
4. For a deployment, import the archive in Dispatcharr → Plugins and follow the README's reload and verification steps.
