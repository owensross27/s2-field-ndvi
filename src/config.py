"""Sole reader of config.yml. Everything else imports from here."""
import os
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
_CFG_PATH = REPO_ROOT / "config.yml"

with open(_CFG_PATH) as f:
    CFG = yaml.safe_load(f)

STAC = CFG["stac"]
ICEBERG = CFG["iceberg"]
RASTER = CFG["raster"]
QUALITY = CFG["quality"]
FIELDS = CFG["fields"]
TIME = CFG["time"]
EVENT = CFG["event"]

DATA_DIR = REPO_ROOT / "data"


def scope(name: str | None = None) -> dict:
    """Resolve a scope tier (demo|mvp|state|history) to tiles/seasons/dates."""
    name = name or os.environ.get("SCOPE", "demo")
    if name not in CFG["scopes"]:
        raise ValueError(f"unknown scope {name!r}; one of {list(CFG['scopes'])}")
    return {"name": name, **CFG["scopes"][name]}


def utm_zone_for_tile(mgrs_tile: str) -> int:
    """MGRS tile like '15TVG' -> UTM zone 15 (Iowa spans 14/15/16 north)."""
    return int(mgrs_tile[:2])


def epsg_for_tile(mgrs_tile: str) -> int:
    return 32600 + utm_zone_for_tile(mgrs_tile)
