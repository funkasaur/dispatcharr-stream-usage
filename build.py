"""Build dist/stream_usage.zip for Dispatcharr's plugin import.

plugin.json is regenerated from plugin.py (name, version, fields, actions), so the manifest never drifts from the code.
Usage: python3 build.py
"""
import importlib.util
import json
import pathlib
import sys
import types
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent
KEY = "stream_usage"

celery = types.ModuleType("celery")
celery.shared_task = lambda *a, **k: (lambda f: f)
sys.modules.setdefault("celery", celery)

spec = importlib.util.spec_from_file_location("_build", ROOT / KEY / "plugin.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
p = mod.Plugin
manifest = {"name": p.name, "version": p.version, "description": p.description, "author": p.author,
            "help_url": p.help_url, "fields": p.fields, "actions": p.actions}
(ROOT / KEY / "plugin.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
out = ROOT / "dist" / f"{KEY}.zip"
out.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
    for name in ("plugin.py", "plugin.json"):
        z.write(ROOT / KEY / name, f"{KEY}/{name}")
print(f"{out.relative_to(ROOT)}  {p.name} {p.version}")
