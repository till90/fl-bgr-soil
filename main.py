#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FieldLense – fl-bgr-soil (WMS)
UI-Strategie (neu):
- Leaflet: Overlays pro Dataset (BUEK/BOART/NFKWE/PHYSGRU/optional SQR), standardmäßig AUS
- AOI zeichnen -> Auto-Analyse (alle Datasets) -> strukturierter Report + Export (JSON/CSV/MD)
- Keine Source-/Layer-Dropdowns mehr

API:
- GET  /api/overlays        -> Overlay-Metadaten (wms_url, default_layer, legend_url)
- POST /api/analyze         -> {feature: GeoJSON Feature, quality?: fast|standard|detailed}
- GET  /api/getmap.png      -> src=<id>&bbox=<minx,miny,maxx,maxy> (&w=&h=)

Hinweis:
- Für AOI-Sampling wird shapely benötigt.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from functools import lru_cache
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests
from flask import Flask, Response, jsonify, render_template_string, request, send_file
from pyproj import CRS, Geod, Transformer

try:
    from shapely.geometry import Point, shape
except Exception:
    shape = None
    Point = None


# ------------------------------------------------------------
# 0) CONFIG
# ------------------------------------------------------------

APP_TITLE = os.getenv("APP_TITLE", "FieldLense – fl-bgr-soil (WMS)")

LANDING_URL = os.getenv("LANDING_URL", "https://data-tales.dev/")
COOKBOOK_URL = os.getenv("COOKBOOK_URL", "https://data-tales.dev/cookbook/")

MAP_CRS = os.getenv("MAP_CRS", "EPSG:3857")
GEOJSON_CRS = os.getenv("GEOJSON_CRS", "EPSG:4326")

MAX_AOI_KM2 = float(os.getenv("MAX_AOI_KM2", "25"))

# Multi-Source Call-Budget (Samples werden ggf. reduziert)
MAX_FEATURE_CALLS = int(os.getenv("MAX_FEATURE_CALLS", "60"))
MAX_SAMPLES = int(os.getenv("MAX_SAMPLES", "80"))

# GetFeatureInfo – BBOX um Punkt (in Meter, EPSG:3857)
FI_HALF_SIZE_M = float(os.getenv("FI_HALF_SIZE_M", "750"))
FI_IMG_PX = int(os.getenv("FI_IMG_PX", "101"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "14"))
HTTP_UA = os.getenv("HTTP_UA", "FieldLense/fl-bgr-soil (+https://data-tales.dev)")

ENABLE_SQR = os.getenv("ENABLE_SQR", "0").strip().lower() in ("1", "true", "yes", "on")

QUALITY_TO_SAMPLES = {
    "fast": 15,
    "standard": 25,
    "detailed": 50,
}

TECH_FIELD_RX = re.compile(
    r"(shape(_)?(area|len(gth)?)|objectid|fid|gid|id$|globalid|perimeter|"
    r"created|updated|editor|timestamp|st_area|st_length|geom|geometry)",
    re.IGNORECASE,
)

# BOART Textur-Gruppen
BOART_CODE_TO_LABEL_DE = {
    "ss": "Reinsande (ss)",
    "ls": "Lehmsande (ls)",
    "us": "Schluffsande (us)",
    "sl": "Sandlehme (sl)",
    "ll": "Normallehme (ll)",
    "tl": "Tonlehme (tl)",
    "lu": "Lehmschluffe (lu)",
    "tu": "Tonschluffe (tu)",
    "ut": "Schlufftone (ut)",
    "mo": "Moore (mo)",
    "watt": "Watt",
    "siedlung": "Siedlung",
    "abbau": "Abbauflächen",
    "gewaesser": "Gewässer",
    "gewässer": "Gewässer",
    "sonstige": "Sonstige Flächen",
}


# ------------------------------------------------------------
# 1) SOURCES
# ------------------------------------------------------------

SOURCES: Dict[str, Dict[str, Any]] = {
    "buek1000en": {
        "id": "buek1000en",
        "title": "BUEK1000EN – Kartiereinheiten / Leitprofil (1:1.000.000)",
        "wms_url": "https://services.bgr.de/wms/boden/buek1000en/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|buek|mapping|unit|soil)",
        "card_type": "buek_profile",
        "value_type": "categorical",
        "unit": "",
        "bin_step": None,
        "preferred_fields": ["name", "title", "soil", "unit", "leit", "profil", "beschreibung", "bezeichnung"],
    },
    "boart1000ob": {
        "id": "boart1000ob",
        "title": "BOART1000OB – Bodenart (Textur) Oberboden",
        "wms_url": "https://services.bgr.de/wms/boden/boart1000ob/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|boart|bodenart|texture|oberboden|topsoil)",
        "card_type": "texture_topsoil",
        "value_type": "categorical",
        "unit": "",
        "bin_step": None,
        "preferred_fields": [
            "bodart_gr", "bodartgr", "bodart", "bodenart", "textur", "bodenart_gr",
            "bodenartengruppe", "bodenartgruppe", "bodart_txt", "bodart_bez",
        ],
    },
    "nfkwe1000": {
        "id": "nfkwe1000",
        "title": "NFKWE1000 – Nutzbare Feldkapazität (eff. Wurzelraum)",
        "wms_url": "https://services.bgr.de/wms/boden/nfkwe1000/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|nfk|feldkap|water|wurzel|root|available)",
        "card_type": "plant_available_water",
        "value_type": "numeric",
        "unit": "mm",
        "bin_step": 5.0,
        "preferred_fields": ["nfkwe", "nfk", "value", "pixel", "raster", "mm"],
    },
    "physgru1000": {
        "id": "physgru1000",
        "title": "PHYSGRU1000 – Gründigkeit / Bodentiefe / Durchwurzelung",
        "wms_url": "https://services.bgr.de/wms/boden/physgru1000/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|phys|gruend|depth|root|durchwur)",
        "card_type": "soil_depth",
        "value_type": "numeric",
        "unit": "dm",
        "bin_step": 1.0,
        "preferred_fields": ["physgru", "gruend", "gründ", "depth", "value", "pixel", "dm"],
    },
}

if ENABLE_SQR:
    SOURCES["sqr1000"] = {
        "id": "sqr1000",
        "title": "SQR1000 – Soil Quality Rating (Standortgüte-Index)",
        "wms_url": "https://services.bgr.de/wms/boden/sqr1000/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|sqr|quality|rating|bodenwert)",
        "card_type": "soil_quality",
        "value_type": "numeric",
        "unit": "",
        "bin_step": 1.0,
        "preferred_fields": ["sqr", "value", "rating", "quality", "bodenwert", "index"],
    }

RUN_ORDER = ["buek1000en", "boart1000ob", "nfkwe1000", "physgru1000"] + (["sqr1000"] if ENABLE_SQR else [])


# ------------------------------------------------------------
# 2) HTTP / URL HELPERS
# ------------------------------------------------------------

def _http_get(url: str, params: Dict[str, Any]) -> requests.Response:
    headers = {"User-Agent": HTTP_UA}
    r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r


def _url_with_params(base: str, params: Dict[str, Any]) -> str:
    parts = urlsplit(base)
    q = parts.query
    add = urlencode(params, doseq=True)
    new_query = (q + "&" + add) if q else add
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def _as_text_safe(b: bytes, limit: int = 20000) -> str:
    try:
        s = b.decode("utf-8", errors="replace")
    except Exception:
        s = str(b[: min(len(b), limit)])
    return s[:limit]


def _canon_key(k: str) -> str:
    return (k or "").strip().lower()


def _looks_numeric(v: Any) -> bool:
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        s = v.strip().replace(",", ".")
        return bool(re.fullmatch(r"-?\d+(\.\d+)?", s))
    return False


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None
    return None


def _is_probably_code(s: str) -> bool:
    if not s:
        return False
    t = s.strip()
    if len(t) > 8:
        return False
    if re.fullmatch(r"[A-Za-z0-9_-]+", t) is None:
        return False
    if re.fullmatch(r"\d+", t):
        return False
    return True


def _normalize_code(s: str) -> str:
    return (s or "").strip().upper()


def _pick_attr(attrs: Dict[str, Any], preferred_fields: List[str], numeric: bool) -> Tuple[str, Any]:
    if not isinstance(attrs, dict) or not attrs:
        return ("", None)

    lower_map = {_canon_key(k): k for k in attrs.keys()}

    # 1) exact
    for pf in preferred_fields:
        k = lower_map.get(_canon_key(pf))
        if k is not None:
            v = attrs.get(k)
            if v is not None and not TECH_FIELD_RX.search(k):
                if numeric and _looks_numeric(v):
                    return (k, v)
                if (not numeric) and (not _looks_numeric(v)):
                    return (k, v)

    # 2) substring
    keys = list(attrs.keys())
    for pf in preferred_fields:
        pf_l = _canon_key(pf)
        for k in keys:
            if TECH_FIELD_RX.search(k):
                continue
            if pf_l and pf_l in _canon_key(k):
                v = attrs.get(k)
                if v is None:
                    continue
                if numeric and _looks_numeric(v):
                    return (k, v)
                if (not numeric) and (not _looks_numeric(v)):
                    return (k, v)

    # 3) fallback
    for k, v in attrs.items():
        if v is None:
            continue
        if TECH_FIELD_RX.search(k):
            continue
        if numeric and _looks_numeric(v):
            return (k, v)
        if (not numeric) and (not _looks_numeric(v)):
            return (k, v)

    return ("", None)


def _bin_numeric(value: float, step: float) -> float:
    if step <= 0:
        return value
    return round(value / step) * step


def _format_numeric(value: float, unit: str, decimals: int = 1) -> str:
    if unit:
        return f"{value:.{decimals}f} {unit}"
    return f"{value:.{decimals}f}"


# ------------------------------------------------------------
# 3) CRS / GEOMETRY
# ------------------------------------------------------------

_crs_4326 = CRS.from_string("EPSG:4326")
_crs_map = CRS.from_string(MAP_CRS)
_to_map = Transformer.from_crs(_crs_4326, _crs_map, always_xy=True)
_geod = Geod(ellps="WGS84")


def geodesic_area_km2(geojson_geom: Dict[str, Any]) -> float:
    if geojson_geom.get("type") == "Polygon":
        rings = geojson_geom["coordinates"]
        lon, lat = zip(*rings[0])
        area_m2, _ = _geod.polygon_area_perimeter(lon, lat)
        return abs(area_m2) / 1_000_000.0
    if geojson_geom.get("type") == "MultiPolygon":
        total = 0.0
        for poly in geojson_geom["coordinates"]:
            lon, lat = zip(*poly[0])
            area_m2, _ = _geod.polygon_area_perimeter(lon, lat)
            total += abs(area_m2)
        return total / 1_000_000.0
    return 0.0


def random_points_in_polygon(geojson_geom: Dict[str, Any], n: int, seed: int = 0) -> List[Tuple[float, float]]:
    if shape is None or Point is None:
        raise RuntimeError("shapely is required for AOI sampling (install shapely).")

    random.seed(seed or int(time.time()))
    poly = shape(geojson_geom)

    minx, miny, maxx, maxy = poly.bounds
    pts: List[Tuple[float, float]] = []
    tries = 0
    max_tries = max(4000, n * 250)

    while len(pts) < n and tries < max_tries:
        tries += 1
        x = random.uniform(minx, maxx)
        y = random.uniform(miny, maxy)
        p = Point(x, y)
        if poly.contains(p):
            pts.append((x, y))

    if len(pts) < n:
        raise RuntimeError(f"Could not sample {n} points inside AOI (got {len(pts)}).")
    return pts


# ------------------------------------------------------------
# 4) WMS CAPABILITIES / LAYER PICK
# ------------------------------------------------------------

@lru_cache(maxsize=64)
def fetch_wms_capabilities(src_id: str) -> Dict[str, Any]:
    import xml.etree.ElementTree as ET

    src = SOURCES[src_id]
    wms_url = src["wms_url"]
    wms_version = src.get("wms_version", "1.3.0")

    params = {"SERVICE": "WMS", "REQUEST": "GetCapabilities", "VERSION": wms_version}
    r = _http_get(wms_url, params=params)
    root = ET.fromstring(r.content)

    def strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    layers: List[Dict[str, str]] = []
    getmap_formats: List[str] = []
    fi_formats: List[str] = []

    for el in root.iter():
        if strip_ns(el.tag) == "Request":
            for req in el:
                name = strip_ns(req.tag)
                if name in ("GetMap", "GetFeatureInfo"):
                    fmts = []
                    for c in req:
                        if strip_ns(c.tag) == "Format" and (c.text or "").strip():
                            fmts.append((c.text or "").strip())
                    if name == "GetMap":
                        getmap_formats = fmts
                    else:
                        fi_formats = fmts

    for el in root.iter():
        if strip_ns(el.tag) == "Layer":
            lname = ""
            ltitle = ""
            labstract = ""
            for c in el:
                t = strip_ns(c.tag)
                if t == "Name" and (c.text or "").strip():
                    lname = (c.text or "").strip()
                elif t == "Title" and (c.text or "").strip():
                    ltitle = (c.text or "").strip()
                elif t == "Abstract" and (c.text or "").strip():
                    labstract = (c.text or "").strip()
            if lname:
                layers.append({"name": lname, "title": ltitle, "abstract": labstract})

    layers = sorted(layers, key=lambda d: (d.get("title", ""), d.get("name", "")))

    return {
        "src_id": src_id,
        "layers": layers,
        "getmap_formats": getmap_formats,
        "featureinfo_formats": fi_formats,
        "wms_url": wms_url,
        "wms_version": wms_version,
    }


def pick_default_layer(src_id: str) -> str:
    src = SOURCES[src_id]
    hint = src.get("layer_hint") or ""
    try:
        caps = fetch_wms_capabilities(src_id)
        if caps.get("layers"):
            if hint:
                rx = re.compile(hint, re.IGNORECASE)
                for lyr in caps["layers"]:
                    hay = " ".join([lyr.get("name", ""), lyr.get("title", ""), lyr.get("abstract", "")])
                    if rx.search(hay):
                        return lyr["name"]
            return caps["layers"][0]["name"]
    except Exception:
        pass
    # robust fallback für BGR (häufig "0")
    return "0"


def pick_info_format(src_id: str) -> str:
    caps = fetch_wms_capabilities(src_id)
    fmts = [f.lower() for f in caps.get("featureinfo_formats", [])]
    for wanted in ["application/geo+json", "application/json", "application/vnd.geo+json"]:
        if wanted in fmts:
            for f in caps.get("featureinfo_formats", []):
                if f.lower() == wanted:
                    return f
    for wanted in [
        "text/plain",
        "text/html",
        "text/xml",
        "application/vnd.esri.wms_featureinfo_xml",
        "application/vnd.esri.wms_raw_xml",
    ]:
        if wanted.lower() in fmts:
            for f in caps.get("featureinfo_formats", []):
                if f.lower() == wanted.lower():
                    return f
    return (caps.get("featureinfo_formats") or ["text/plain"])[0]


def wms_legend_url(src_id: str, layer: str) -> str:
    src = SOURCES[src_id]
    return _url_with_params(
        src["wms_url"],
        {
            "request": "GetLegendGraphic",
            "version": src.get("wms_version", "1.3.0"),
            "format": "image/png",
            "layer": layer,
        },
    )


# ------------------------------------------------------------
# 5) WMS: GetFeatureInfo / GetMap
# ------------------------------------------------------------

def wms_getfeatureinfo_point(src_id: str, lon: float, lat: float, layer: str) -> Dict[str, Any]:
    src = SOURCES[src_id]
    wms_url = src["wms_url"]
    wms_version = src.get("wms_version", "1.3.0")

    x, y = _to_map.transform(lon, lat)
    half = FI_HALF_SIZE_M
    bbox = (x - half, y - half, x + half, y + half)

    w = FI_IMG_PX
    h = FI_IMG_PX
    i = w // 2
    j = h // 2

    info_format = pick_info_format(src_id)

    params = {
        "SERVICE": "WMS",
        "VERSION": wms_version,
        "REQUEST": "GetFeatureInfo",
        "LAYERS": layer,
        "QUERY_LAYERS": layer,
        "STYLES": "",
        "FORMAT": "image/png",
        "CRS" if wms_version == "1.3.0" else "SRS": MAP_CRS,
        "BBOX": ",".join([f"{v:.3f}" for v in bbox]),
        "WIDTH": str(w),
        "HEIGHT": str(h),
        "INFO_FORMAT": info_format,
        "FEATURE_COUNT": "5",
    }
    if wms_version == "1.3.0":
        params["I"] = str(i)
        params["J"] = str(j)
    else:
        params["X"] = str(i)
        params["Y"] = str(j)

    r = _http_get(wms_url, params=params)
    raw = r.content
    txt = _as_text_safe(raw)

    if txt.lstrip().startswith("{") or txt.lstrip().startswith("["):
        try:
            return {"ok": True, "format": "json", "data": json.loads(txt)}
        except Exception:
            return {"ok": True, "format": "text", "data": txt}
    return {"ok": True, "format": "text", "data": txt}


def wms_getmap_png(
    src_id: str,
    bbox_3857: Tuple[float, float, float, float],
    layer: str,
    width: int = 1400,
    height: int = 900,
) -> bytes:
    src = SOURCES[src_id]
    caps = fetch_wms_capabilities(src_id)
    fmts = [f.lower() for f in caps.get("getmap_formats", [])]
    wanted = "image/png"
    if wanted not in fmts and caps.get("getmap_formats"):
        wanted = caps["getmap_formats"][0]

    params = {
        "SERVICE": "WMS",
        "VERSION": src.get("wms_version", "1.3.0"),
        "REQUEST": "GetMap",
        "LAYERS": layer,
        "STYLES": "",
        "FORMAT": wanted,
        "TRANSPARENT": "TRUE",
        "CRS" if src.get("wms_version", "1.3.0") == "1.3.0" else "SRS": MAP_CRS,
        "BBOX": ",".join([f"{v:.3f}" for v in bbox_3857]),
        "WIDTH": str(int(width)),
        "HEIGHT": str(int(height)),
    }
    r = _http_get(src["wms_url"], params=params)
    return r.content


# ------------------------------------------------------------
# 6) INTERPRETATION / CARDS
# ------------------------------------------------------------

def make_cards_for_source(src_id: str, distribution: List[Dict[str, Any]], legend: str) -> List[Dict[str, Any]]:
    src = SOURCES[src_id]
    ctype = src.get("card_type", "generic")

    cards: List[Dict[str, Any]] = []
    top = distribution[0] if distribution else {}
    share = float(top.get("share", 0.0))
    label = (top.get("label") or "").strip()

    if ctype == "buek_profile":
        cards.append({
            "title": "BUEK – Kartiereinheit / Leitprofil (Überblick, nicht schlagscharf)",
            "dominance_pct": round(share, 1),
            "class_label": label or "Unbekannt",
            "summary": "BUEK1000 ist generalisiert (1:1.000.000). Eignet sich als regionaler Standort-Kontext, nicht als parzellengenaue Bodenkartierung.",
            "practice": [
                "Als Kontextlayer verwenden (Region/Standorttyp).",
                "Für Feldentscheidungen: höhere Maßstäbe / Landesdaten / Proben ergänzen.",
            ],
            "risks": [
                "Maßstabsbedingte Generalisierung.",
                "Kartiereinheit kann mehrere Subtypen enthalten.",
            ],
            "legend_url": legend,
        })
        return cards

    if ctype == "texture_topsoil":
        cards.append({
            "title": "Bodenart (Oberboden) – Texturklasse (indikativ)",
            "dominance_pct": round(share, 1),
            "class_label": label or "Unbekannt",
            "summary": "Textur beeinflusst Infiltration, Bearbeitbarkeit, Verschlämmung, Nährstoff-/Wasserhaltevermögen.",
            "practice": [
                "Bewässerung und Bodenbearbeitung grob an Textur anlehnen.",
                "Bei heterogenen Schlägen zonieren (Satellit/Ertrag/EC/Proben).",
            ],
            "risks": ["Generalisierung (Maßstab 1:1.000.000)."],
            "legend_url": legend,
        })
        return cards

    if ctype == "plant_available_water":
        cards.append({
            "title": "Nutzbare Feldkapazität – Bewässerungs-Kontext",
            "dominance_pct": round(share, 1),
            "class_label": label or "Unbekannt",
            "summary": "Indikator für pflanzenverfügbares Wasser im Wurzelraum (Trockenstress-/Bewässerungsfenster).",
            "practice": [
                "Als Rangfolge nutzen: niedrig → früher Trockenstress, hoch → mehr Puffer.",
                "Mit Kultur/Wurzeltiefe/Management kombinieren.",
            ],
            "risks": ["Indikativ; lokale Variabilität durch Management/Relief/Struktur nicht enthalten."],
            "legend_url": legend,
        })
        return cards

    if ctype == "soil_depth":
        cards.append({
            "title": "Gründigkeit / Bodentiefe / Durchwurzelung – Standortlimit",
            "dominance_pct": round(share, 1),
            "class_label": label or "Unbekannt",
            "summary": "Begrenzt Wurzelraum und damit Wasser-/Nährstoffpuffer; relevant für Kulturwahl und Bewässerungsstrategie.",
            "practice": [
                "Flachgründig: stressanfälliger, konservative Bewässerungsfenster.",
                "Tiefgründig: mehr Puffer, stärkere Tiefenerschließung möglich.",
            ],
            "risks": ["Indikativ; lokal können Horizonte/Staunässe/Steinigkeit stark variieren."],
            "legend_url": legend,
        })
        return cards

    if ctype == "soil_quality":
        cards.append({
            "title": "SQR – Soil Quality Rating (Index)",
            "dominance_pct": round(share, 1),
            "class_label": label or "Unbekannt",
            "summary": "Indexhafte Standortgüte – gut für grobe Vergleichbarkeit, nicht als alleinige Entscheidungsgrundlage am Schlag.",
            "practice": ["Als Ranking/Benchmark nutzen; für Details Textur/NFK/Proben heranziehen."],
            "risks": ["Index/Generalisation: nicht schlagscharf."],
            "legend_url": legend,
        })
        return cards

    cards.append({
        "title": "Standort – Hinweis",
        "dominance_pct": round(share, 1),
        "class_label": label or "Unbekannt",
        "summary": "Der Dienst liefert keine eindeutig interpretierbare Klasse über die aktuelle Heuristik.",
        "practice": ["INFO_FORMAT/Capabilities prüfen; ggf. anderes Layer/Dienst wählen."],
        "risks": [],
        "legend_url": legend,
    })
    return cards


# ------------------------------------------------------------
# 7) EVALUATION + AOI ANALYSIS
# ------------------------------------------------------------

def evaluate_point(src_id: str, lon: float, lat: float, layer: str) -> Dict[str, Any]:
    src = SOURCES[src_id]
    vtype = src.get("value_type", "categorical")
    preferred = src.get("preferred_fields", [])
    want_numeric = (vtype == "numeric")

    fi = wms_getfeatureinfo_point(src_id, lon, lat, layer=layer)

    if fi.get("format") == "json":
        data = fi.get("data")
        if isinstance(data, dict) and isinstance(data.get("features"), list) and data["features"]:
            props = (data["features"][0] or {}).get("properties") or {}
            if isinstance(props, dict):
                k, v = _pick_attr(props, preferred_fields=preferred, numeric=want_numeric)

                if want_numeric:
                    fv = _to_float(v)
                    if fv is not None:
                        return {"ok": True, "code": "", "label": str(fv), "value": fv, "picked_field": k}

                s = (str(v).strip() if v is not None else "")
                if s:
                    code = _normalize_code(s) if _is_probably_code(s) else ""
                    return {"ok": True, "code": code, "label": s[:220], "value": None, "picked_field": k}

    txt = str(fi.get("data") or "").strip()

    if want_numeric:
        fv = _to_float(txt)
        if fv is not None:
            return {"ok": True, "code": "", "label": str(fv), "value": fv, "picked_field": ""}

    first = txt.split()[0] if txt else ""
    code = _normalize_code(first) if _is_probably_code(first) else ""
    label = (txt[:220] if txt else "Unbekannt")
    return {"ok": True, "code": code, "label": label, "value": None, "picked_field": ""}


def summarize_distribution(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total = sum(v["count"] for v in values) or 1
    dist = []
    for v in sorted(values, key=lambda x: x["count"], reverse=True):
        dist.append({
            "code": v.get("code", ""),
            "label": v.get("label", ""),
            "count": int(v["count"]),
            "share": 100.0 * (v["count"] / total),
        })
    return dist


def analyze_points_for_source(src_id: str, points: List[Tuple[float, float]], layer: str) -> Dict[str, Any]:
    src = SOURCES[src_id]
    legend = wms_legend_url(src_id, layer)
    vtype = src.get("value_type", "categorical")
    unit = src.get("unit", "") or ""
    bin_step = src.get("bin_step", None)

    # CATEGORICAL
    if vtype != "numeric":
        counts: Dict[str, Dict[str, Any]] = {}
        for (lon, lat) in points:
            ev = evaluate_point(src_id, lon, lat, layer=layer)
            code = (ev.get("code") or "").strip().lower()
            label = (ev.get("label") or "Unbekannt").strip()

            # BOART mapping
            if src_id == "boart1000ob":
                if code and code in BOART_CODE_TO_LABEL_DE:
                    label = BOART_CODE_TO_LABEL_DE[code]
                else:
                    l0 = label.strip().lower()
                    if l0 in BOART_CODE_TO_LABEL_DE:
                        label = BOART_CODE_TO_LABEL_DE[l0]
                        code = l0

            key = (code.upper() if code else label)
            if key not in counts:
                counts[key] = {"code": code.upper() if code else "", "label": label, "count": 0}
            counts[key]["count"] += 1

        dist = summarize_distribution(list(counts.values()))
        dominant = dist[0] if dist else {"code": "", "label": "Unbekannt", "share": 0.0}
        hetero = 100.0 - float(dominant.get("share", 0.0))
        cards = make_cards_for_source(src_id, dist, legend=legend)

        return {
            "ok": True,
            "src_id": src_id,
            "src_title": src["title"],
            "layer": layer,
            "legend_url": legend,
            "dominant_label": dominant.get("label", "Unbekannt"),
            "dominant_share_pct": round(float(dominant.get("share", 0.0)), 2),
            "heterogeneity_pct": round(hetero, 2),
            "distribution": dist[:12],
            "cards": cards,
            "stats": None,
        }

    # NUMERIC
    values: List[float] = []
    binned_counts: Dict[str, int] = {}

    for (lon, lat) in points:
        ev = evaluate_point(src_id, lon, lat, layer=layer)
        v = ev.get("value", None)
        if v is None:
            v = _to_float(ev.get("label"))
        if v is None:
            continue

        fv = float(v)
        values.append(fv)

        if bin_step:
            b = _bin_numeric(fv, float(bin_step))
            b_label = _format_numeric(b, unit, decimals=0 if float(bin_step) >= 1 else 1)
        else:
            b_label = _format_numeric(fv, unit, decimals=1)
        binned_counts[b_label] = binned_counts.get(b_label, 0) + 1

    legend = wms_legend_url(src_id, layer)

    if not values:
        dist = [{"code": "", "label": "Unbekannt", "count": 0, "share": 0.0}]
        cards = make_cards_for_source(src_id, dist, legend=legend)
        return {
            "ok": True,
            "src_id": src_id,
            "src_title": src["title"],
            "layer": layer,
            "legend_url": legend,
            "dominant_label": "Unbekannt",
            "dominant_share_pct": 0.0,
            "heterogeneity_pct": 0.0,
            "distribution": dist,
            "cards": cards,
            "stats": None,
        }

    mean_v = sum(values) / len(values)
    min_v = min(values)
    max_v = max(values)

    total = sum(binned_counts.values()) or 1
    dist = []
    for lbl, cnt in sorted(binned_counts.items(), key=lambda kv: kv[1], reverse=True):
        dist.append({"code": "", "label": lbl, "count": cnt, "share": 100.0 * (cnt / total)})

    dominant = dist[0] if dist else {"label": _format_numeric(mean_v, unit, decimals=1), "share": 0.0}
    hetero = 100.0 - float(dominant.get("share", 0.0))

    cards = make_cards_for_source(src_id, dist, legend=legend)
    if cards:
        cards[0]["mean"] = _format_numeric(mean_v, unit, decimals=1)
        cards[0]["range"] = f"{_format_numeric(min_v, unit, 1)} – {_format_numeric(max_v, unit, 1)}"
        cards[0]["n"] = len(values)

        if src_id == "physgru1000":
            cards[0]["mean_cm"] = f"{mean_v:.1f} dm (≈ {mean_v*10:.0f} cm)"
            cards[0]["range_cm"] = f"{min_v:.1f}–{max_v:.1f} dm (≈ {min_v*10:.0f}–{max_v*10:.0f} cm)"

    return {
        "ok": True,
        "src_id": src_id,
        "src_title": src["title"],
        "layer": layer,
        "legend_url": legend,
        "dominant_label": dominant.get("label", _format_numeric(mean_v, unit, 1)),
        "dominant_share_pct": round(float(dominant.get("share", 0.0)), 2),
        "heterogeneity_pct": round(hetero, 2),
        "distribution": dist[:12],
        "cards": cards,
        "stats": {
            "unit": unit,
            "mean": round(mean_v, 3),
            "min": round(min_v, 3),
            "max": round(max_v, 3),
            "n": len(values),
        },
    }


# ------------------------------------------------------------
# 8) FLASK APP
# ------------------------------------------------------------

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["JSON_AS_ASCII"] = False


@app.after_request
def _add_headers(resp: Response) -> Response:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Cache-Control"] = "no-store"
    return resp


@lru_cache(maxsize=1)
def overlays_cached() -> Dict[str, Any]:
    overlays = []
    for sid in RUN_ORDER:
        layer = pick_default_layer(sid)
        overlays.append({
            "id": sid,
            "title": SOURCES[sid]["title"],
            "wms_url": SOURCES[sid]["wms_url"],
            "wms_version": SOURCES[sid].get("wms_version", "1.3.0"),
            "layer": layer,
            "legend_url": wms_legend_url(sid, layer) if layer else "",
        })
    return {"ok": True, "overlays": overlays, "map_crs": MAP_CRS, "geojson_crs": GEOJSON_CRS}


@app.get("/api/overlays")
def api_overlays():
    try:
        return jsonify(overlays_cached())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/getmap.png")
def api_getmap_png():
    try:
        src_id = (request.args.get("src") or "").strip()
        if src_id not in SOURCES:
            return jsonify({"ok": False, "error": "Unknown src"}), 400

        layer = (request.args.get("layer") or "").strip() or pick_default_layer(src_id)
        bbox_s = (request.args.get("bbox") or "").strip()
        if not bbox_s:
            return jsonify({"ok": False, "error": "Missing bbox"}), 400
        parts = [float(x) for x in bbox_s.split(",")]
        if len(parts) != 4:
            return jsonify({"ok": False, "error": "Invalid bbox"}), 400

        w = int(request.args.get("w", "1400"))
        h = int(request.args.get("h", "900"))
        png = wms_getmap_png(src_id, tuple(parts), layer=layer, width=w, height=h)
        return send_file(BytesIO(png), mimetype="image/png", as_attachment=False, download_name=f"{src_id}_{MAP_CRS}.png")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/analyze")
def api_analyze():
    try:
        payload = request.get_json(force=True, silent=False)
        feature = payload.get("feature")
        if not feature or feature.get("type") != "Feature":
            return jsonify({"ok": False, "error": "Body must contain a GeoJSON Feature"}), 400

        quality = (payload.get("quality") or "standard").strip().lower()
        if quality not in QUALITY_TO_SAMPLES:
            quality = "standard"

        geom = feature.get("geometry") or {}
        gtype = geom.get("type")
        if gtype not in ("Polygon", "MultiPolygon"):
            return jsonify({"ok": False, "error": "This UI expects an AOI Polygon/Rectangle (Polygon/MultiPolygon)."}), 400

        area_km2 = geodesic_area_km2(geom)
        if area_km2 <= 0:
            return jsonify({"ok": False, "error": "AOI area could not be computed"}), 400
        if area_km2 > MAX_AOI_KM2:
            return jsonify({"ok": False, "error": f"AOI too large ({area_km2:.2f} km²). Max is {MAX_AOI_KM2:.2f} km²."}), 400

        sources_to_run = [sid for sid in RUN_ORDER if sid in SOURCES]
        n_sources = max(1, len(sources_to_run))

        requested_samples = int(QUALITY_TO_SAMPLES[quality])
        requested_samples = max(5, min(requested_samples, MAX_SAMPLES))

        # Call-Budget: n_sources * n_samples <= MAX_FEATURE_CALLS
        max_samples_budget = max(5, MAX_FEATURE_CALLS // n_sources)
        effective_samples = min(requested_samples, max_samples_budget)

        pts = random_points_in_polygon(geom, n=effective_samples, seed=int(time.time()))

        results = []
        for sid in sources_to_run:
            try:
                lyr = pick_default_layer(sid)
                if not lyr:
                    results.append({"ok": False, "src_id": sid, "src_title": SOURCES[sid]["title"], "error": "No layer"})
                    continue
                results.append(analyze_points_for_source(sid, pts, layer=lyr))
            except Exception as e:
                results.append({"ok": False, "src_id": sid, "src_title": SOURCES[sid]["title"], "error": str(e)})

        return jsonify({
            "ok": True,
            "mode": "aoi",
            "timestamp_utc": int(time.time()),
            "aoi_area_km2": round(area_km2, 4),
            "quality": quality,
            "requested_samples": requested_samples,
            "effective_samples": effective_samples,
            "sources_run": sources_to_run,
            "note": "AOI = Sampling vieler Punkte im Polygon. Ergebnisse sind indikativ (maßstabsbedingt).",
            "results": results,
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ------------------------------------------------------------
# 9) WEB UI
# ------------------------------------------------------------

INDEX_HTML = r"""
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{app_title}}</title>

  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <link rel="stylesheet" href="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.css">

  <style>
    :root{
      --bg:#0b1020;
      --panel:#0f172a;
      --card:#0b1224;
      --line:#1f2a44;
      --text:#e6eaf2;
      --muted:#a6b0c3;
      --accent:#78a6ff;
      --good:#2dd4bf;
      --warn:#fb7185;
      --radius:16px;
      --shadow: 0 12px 36px rgba(0,0,0,.35);
    }

    *{ box-sizing:border-box; }
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
      background: radial-gradient(1200px 700px at 20% -10%, rgba(120,166,255,.22), transparent 55%),
                  radial-gradient(900px 600px at 110% 10%, rgba(45,212,191,.12), transparent 55%),
                  var(--bg);
      color:var(--text);
    }

    .wrap{ max-width:1280px; margin:0 auto; padding:16px; }
    header{
      display:flex; gap:12px; align-items:flex-end; justify-content:space-between;
      margin-bottom:14px;
    }
    .brand h1{ margin:0; font-size:18px; letter-spacing:.2px; }
    .brand .sub{
      margin-top:4px;
      font-size:12px;
      color:var(--muted);
      display:flex; gap:10px; flex-wrap:wrap; align-items:center;
    }
    a{ color:var(--accent); text-decoration:none; }
    a:hover{ text-decoration:underline; }

    .grid{
      display:grid;
      grid-template-columns: 1.35fr 1fr;
      gap:12px;
      align-items:stretch;
    }
    @media (max-width: 980px){
      .grid{ grid-template-columns: 1fr; }
    }

    .panel{
      background: rgba(15,23,42,.84);
      border:1px solid rgba(31,42,68,.9);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      overflow:hidden;
      backdrop-filter: blur(8px);
    }
    .phead{
      padding:12px 12px 10px 12px;
      border-bottom:1px solid rgba(31,42,68,.9);
      display:flex; justify-content:space-between; gap:10px; align-items:center;
    }
    .phead .title{ font-size:13px; color:var(--muted); }
    .pbody{ padding:12px; }

    #map{ height: 66vh; min-height:520px; }
    @media (max-width: 980px){ #map{ height: 54vh; min-height:420px; } }

    .controls{
      display:flex; gap:10px; flex-wrap:wrap; align-items:center;
    }
    .controls > *{ flex:0 0 auto; }
    select, button, input{
      background: rgba(11,18,36,.95);
      color: var(--text);
      border:1px solid rgba(31,42,68,.95);
      border-radius: 12px;
      padding:10px 12px;
      font-size:14px;
      outline:none;
    }
    select:hover, button:hover{ border-color: rgba(120,166,255,.55); }
    button{ cursor:pointer; }
    button.primary{ border-color: rgba(120,166,255,.7); }
    button.ghost{ background: transparent; }

    .badge{
      display:inline-flex; align-items:center; gap:8px;
      padding:6px 10px;
      border-radius:999px;
      border:1px solid rgba(31,42,68,.95);
      color:var(--muted);
      font-size:12px;
      white-space:nowrap;
    }
    .badge.good{ border-color: rgba(45,212,191,.6); color: rgba(45,212,191,.95); }
    .badge.warn{ border-color: rgba(251,113,133,.65); color: rgba(251,113,133,.95); }

    .hint{
      margin-top:10px;
      color:var(--muted);
      font-size:12px;
      line-height:1.35;
    }

    .reportTop{
      display:flex; gap:10px; flex-wrap:wrap; align-items:center;
      margin-bottom:10px;
    }

    .exportRow{
      display:flex; gap:8px; flex-wrap:wrap; align-items:center;
      margin: 10px 0 6px 0;
    }

    details{
      border:1px solid rgba(31,42,68,.85);
      border-radius: 14px;
      background: rgba(11,18,36,.78);
      overflow:hidden;
    }
    details > summary{
      cursor:pointer;
      list-style:none;
      padding:10px 12px;
      color:var(--text);
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      font-size:13px;
    }
    details > summary::-webkit-details-marker{ display:none; }
    .detailsBody{ padding:10px 12px 12px 12px; color:var(--muted); font-size:12px; }

    textarea{
      width:100%;
      min-height:140px;
      resize:vertical;
      background: rgba(6,10,20,.9);
      color: var(--text);
      border:1px solid rgba(31,42,68,.95);
      border-radius: 12px;
      padding:10px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size:12px;
    }

    .cards{
      display:grid;
      grid-template-columns: 1fr;
      gap:10px;
      margin-top:10px;
    }

    .card{
      background: rgba(11,18,36,.78);
      border:1px solid rgba(31,42,68,.9);
      border-radius: 16px;
      padding:12px;
    }
    .card h3{
      margin:0 0 8px 0;
      font-size:14px;
      display:flex;
      justify-content:space-between;
      gap:10px;
      align-items:flex-start;
    }
    .card h3 .mini{
      font-size:12px;
      color:var(--muted);
      font-weight:500;
    }

    .kpiRow{
      display:flex; gap:8px; flex-wrap:wrap; align-items:center;
      margin: 0 0 8px 0;
    }
    .kpi{
      border:1px solid rgba(31,42,68,.9);
      border-radius: 14px;
      padding:8px 10px;
      background: rgba(6,10,20,.6);
      min-width: 140px;
    }
    .kpi .k{ font-size:11px; color:var(--muted); }
    .kpi .v{ font-size:13px; color:var(--text); margin-top:2px; }

    .dist{
      margin-top:6px;
      display:flex;
      flex-direction:column;
      gap:6px;
    }
    .distRow{
      display:grid;
      grid-template-columns: 1fr 70px;
      gap:10px;
      align-items:center;
      font-size:12px;
      color: var(--muted);
    }
    .bar{
      height:10px;
      border-radius:999px;
      border:1px solid rgba(31,42,68,.9);
      background: rgba(6,10,20,.55);
      overflow:hidden;
    }
    .bar > i{
      display:block;
      height:100%;
      width:0%;
      background: linear-gradient(90deg, rgba(120,166,255,.85), rgba(45,212,191,.65));
    }

    ul{ margin:6px 0 0 18px; padding:0; }
    li{ margin: 2px 0; }

    .splitLine{ height:1px; background: rgba(31,42,68,.9); margin:10px 0; }

    /* Leaflet tweaks */
    .leaflet-control-layers{
      border-radius: 14px !important;
      border:1px solid rgba(31,42,68,.9) !important;
      background: rgba(11,18,36,.92) !important;
      color: var(--text) !important;
      box-shadow: var(--shadow) !important;
    }
    .leaflet-control-layers label{ color: var(--text) !important; }
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand">
        <h1>{{app_title}}</h1>
        <div class="sub">
          <span>BGR WMS · AOI Auto-Analyse · Exporte (JSON/CSV/MD)</span>
          <span>·</span>
          <a href="{{landing_url}}">Landing</a>
          <a href="{{cookbook_url}}">Cookbook</a>
        </div>
      </div>
      <div class="badge">CRS: GeoJSON {{geojson_crs}} · Map {{map_crs}}</div>
    </header>

    <div class="grid">
      <div class="panel">
        <div id="map"></div>
        <div class="pbody">
          <div class="controls">
            <div class="badge" id="status">bereit</div>

            <label class="badge" style="gap:8px">
              Analyse-Qualität
              <select id="quality">
                <option value="fast">fast</option>
                <option value="standard" selected>standard</option>
                <option value="detailed">detailed</option>
              </select>
            </label>

            <button class="primary" id="btnRecalc">Neu berechnen</button>
            <button class="ghost" id="btnClear">AOI löschen</button>
          </div>

          <div class="hint">
            Layer links sind reine Overlays (standardmäßig aus). Zeichne ein Polygon/Rechteck – die Analyse startet automatisch
            und erzeugt einen exportierbaren Report (indikativ, Maßstab 1:1.000.000).
          </div>
        </div>
      </div>

      <div class="panel">
        <div class="phead">
          <div class="title">Report</div>
          <div class="badge" id="kpiBadge">—</div>
        </div>
        <div class="pbody">
          <div class="reportTop" id="reportTop"></div>

          <div class="exportRow">
            <button class="primary" id="btnJson" disabled>report.json</button>
            <button class="primary" id="btnCsv" disabled>summary.csv</button>
            <button class="primary" id="btnMd" disabled>report.md</button>
            <button id="btnGeojson" disabled>AOI.geojson</button>
          </div>

          <div id="cards" class="cards"></div>

          <div class="splitLine"></div>

          <details>
            <summary>
              <span>AOI GeoJSON ({{geojson_crs}})</span>
              <span class="mini" style="color:var(--muted)">aufklappen</span>
            </summary>
            <div class="detailsBody">
              <textarea id="geojson" spellcheck="false" placeholder="Zeichne eine AOI…"></textarea>
            </div>
          </details>

          <div class="hint" style="margin-top:10px">
            Export-Hinweis: JSON = kompletter Maschinenreport, CSV = 1 Zeile je Dataset (dominant/hetero/stats), MD = kompakter Textreport.
          </div>
        </div>
      </div>
    </div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>

  <script>
    const statusEl = document.getElementById("status");
    const cardsEl = document.getElementById("cards");
    const geojsonEl = document.getElementById("geojson");
    const reportTopEl = document.getElementById("reportTop");
    const kpiBadge = document.getElementById("kpiBadge");

    const btnJson = document.getElementById("btnJson");
    const btnCsv  = document.getElementById("btnCsv");
    const btnMd   = document.getElementById("btnMd");
    const btnGeo  = document.getElementById("btnGeojson");

    function setStatus(text, kind=""){
      statusEl.textContent = text;
      statusEl.className = "badge " + (kind || "");
    }

    // Robust JSON fetch helper (verhindert "JSON.parse unexpected character" im UI)
    async function fetchJson(url, opts){
      const r = await fetch(url, opts);
      const t = await r.text();
      try{
        return JSON.parse(t);
      }catch(e){
        const head = t.slice(0, 240);
        throw new Error("Response ist kein JSON (" + r.status + "): " + head);
      }
    }

    const map = L.map('map').setView([51.1, 10.4], 6);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap'
    }).addTo(map);

    const drawn = new L.FeatureGroup();
    map.addLayer(drawn);

    const drawControl = new L.Control.Draw({
      draw: {
        polyline:false, circle:false, circlemarker:false, marker:false,
        polygon:{ allowIntersection:false, showArea:true },
        rectangle:true
      },
      edit: { featureGroup: drawn, remove:false }
    });
    map.addControl(drawControl);

    let overlaysMeta = [];
    let overlayLayers = {}; // id -> L.tileLayer.wms
    let activeFeature = null;
    let activeGeojson = null;
    let lastReport = null;

    function setActive(layer){
      drawn.clearLayers();
      drawn.addLayer(layer);
      activeFeature = layer;
      activeGeojson = layer.toGeoJSON();
      geojsonEl.value = JSON.stringify(activeGeojson, null, 2);
      updateExportButtons();
    }

    function clearActive(){
      drawn.clearLayers();
      activeFeature = null;
      activeGeojson = null;
      lastReport = null;
      geojsonEl.value = "";
      cardsEl.innerHTML = "";
      reportTopEl.innerHTML = "";
      kpiBadge.textContent = "—";
      updateExportButtons();
      setStatus("bereit");
    }

    map.on(L.Draw.Event.CREATED, function(e){
      setActive(e.layer);
      scheduleAnalyze();
    });

    map.on(L.Draw.Event.EDITED, function(e){
      const layers = e.layers.getLayers();
      if(layers && layers[0]){
        setActive(layers[0]);
        scheduleAnalyze();
      }
    });

    // Nur EIN Feature: bei erneutem Draw wird ohnehin replaced (drawn.clearLayers in setActive)
    // Der Draw-Controller erstellt aber neue Layer. Alles ok.

    document.getElementById("btnClear").addEventListener("click", clearActive);

    document.getElementById("btnRecalc").addEventListener("click", () => {
      if(!activeGeojson){
        setStatus("keine AOI", "warn");
        return;
      }
      runAnalyze();
    });

    document.getElementById("quality").addEventListener("change", () => {
      if(activeGeojson) scheduleAnalyze();
    });

    function updateExportButtons(){
      const hasAoi = !!activeGeojson;
      const hasRep = !!lastReport;
      btnGeo.disabled = !hasAoi;
      btnJson.disabled = !hasRep;
      btnCsv.disabled  = !hasRep;
      btnMd.disabled   = !hasRep;
    }

    function downloadText(filename, mime, text){
      const blob = new Blob([text], {type:mime});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    }

    btnGeo.addEventListener("click", () => {
      if(!activeGeojson) return;
      downloadText("aoi_epsg4326.geojson", "application/geo+json", JSON.stringify(activeGeojson, null, 2));
    });

    btnJson.addEventListener("click", () => {
      if(!lastReport) return;
      downloadText("soil_report.json", "application/json", JSON.stringify(lastReport, null, 2));
    });

    function toCsvRow(cols){
      return cols.map(v => {
        const s = (v === null || v === undefined) ? "" : String(v);
        const needs = /[",\n]/.test(s);
        return needs ? '"' + s.replaceAll('"','""') + '"' : s;
      }).join(",");
    }

    btnCsv.addEventListener("click", () => {
      if(!lastReport) return;
      const head = [
        "src_id","src_title","dominant_label","dominant_share_pct","heterogeneity_pct",
        "mean","min","max","unit","n","layer","legend_url"
      ];
      const rows = [toCsvRow(head)];
      for(const r of (lastReport.results || [])){
        if(!r.ok){
          rows.push(toCsvRow([r.src_id, r.src_title || "", "ERROR: " + (r.error||""), "", "", "", "", "", "", "", "", ""]));
          continue;
        }
        const st = r.stats || {};
        rows.push(toCsvRow([
          r.src_id,
          r.src_title,
          r.dominant_label,
          r.dominant_share_pct,
          r.heterogeneity_pct,
          st.mean ?? "",
          st.min ?? "",
          st.max ?? "",
          st.unit ?? "",
          st.n ?? "",
          r.layer ?? "",
          r.legend_url ?? ""
        ]));
      }
      downloadText("soil_summary.csv", "text/csv", rows.join("\n"));
    });

    function toMarkdown(rep){
      const dt = new Date((rep.timestamp_utc||0)*1000).toISOString().replace("T"," ").replace("Z"," UTC");
      let md = "";
      md += "# FieldLense Soil Report\n\n";
      md += `- Zeitpunkt: ${dt}\n`;
      md += `- AOI Fläche: ${rep.aoi_area_km2} km²\n`;
      md += `- Qualität: ${rep.quality} (Samples: ${rep.effective_samples}/${rep.requested_samples})\n`;
      md += `- Hinweis: ${rep.note}\n\n`;
      md += "## Zusammenfassung\n\n";
      md += "| Dataset | Dominant | Dominanz | Heterogenität | Stats |\n";
      md += "|---|---|---:|---:|---|\n";
      for(const r of (rep.results||[])){
        if(!r.ok){
          md += `| ${r.src_title||r.src_id} | ERROR |  |  | ${r.error||""} |\n`;
          continue;
        }
        const st = r.stats ? (`mean ${r.stats.mean}${r.stats.unit?(" "+r.stats.unit):""}, range ${r.stats.min}-${r.stats.max}`) : "";
        md += `| ${r.src_title} | ${r.dominant_label} | ${r.dominant_share_pct}% | ${r.heterogeneity_pct}% | ${st} |\n`;
      }
      md += "\n## Details\n\n";
      for(const r of (rep.results||[])){
        md += `### ${r.src_title||r.src_id}\n\n`;
        if(!r.ok){
          md += `Fehler: ${r.error||"unbekannt"}\n\n`;
          continue;
        }
        md += `- Dominant: ${r.dominant_label} (${r.dominant_share_pct}%)\n`;
        md += `- Heterogenität: ${r.heterogeneity_pct}%\n`;
        if(r.stats){
          md += `- Stats: mean=${r.stats.mean} ${r.stats.unit||""}, min=${r.stats.min}, max=${r.stats.max}, n=${r.stats.n}\n`;
        }
        const top = (r.distribution||[]).slice(0,5);
        if(top.length){
          md += "\nTop-Verteilung:\n";
          for(const d of top){
            md += `- ${d.label}: ${d.share.toFixed(1)}% (${d.count})\n`;
          }
        }
        md += `\nLegende: ${r.legend_url||""}\n\n`;
      }
      return md;
    }

    btnMd.addEventListener("click", () => {
      if(!lastReport) return;
      downloadText("soil_report.md", "text/markdown", toMarkdown(lastReport));
    });

    function pct(n){ return Math.max(0, Math.min(100, n)); }

    function bbox3857FromLeafletBounds(bounds){
      function project(lat, lon){
        const R = 6378137;
        const x = R * lon * Math.PI/180;
        const y = R * Math.log(Math.tan(Math.PI/4 + (lat*Math.PI/180)/2));
        return [x,y];
      }
      const sw = bounds.getSouthWest();
      const ne = bounds.getNorthEast();
      const a = project(sw.lat, sw.lng);
      const b = project(ne.lat, ne.lng);
      return [a[0], a[1], b[0], b[1]];
    }

    function currentAoiBbox3857(){
      if(!activeFeature) return null;
      if(activeFeature.getBounds){
        const bounds = activeFeature.getBounds();
        return bbox3857FromLeafletBounds(bounds);
      }
      return null;
    }

    function renderReport(rep){
      lastReport = rep;
      updateExportButtons();

      const dt = new Date((rep.timestamp_utc||0)*1000);
      const dtStr = dt.toISOString().replace("T"," ").replace("Z"," UTC");

      kpiBadge.textContent = `${rep.aoi_area_km2} km² · Samples ${rep.effective_samples}/${rep.requested_samples}`;

      reportTopEl.innerHTML = "";
      const b1 = document.createElement("span");
      b1.className = "badge good";
      b1.textContent = "AOI Report";
      const b2 = document.createElement("span");
      b2.className = "badge";
      b2.textContent = dtStr;
      const b3 = document.createElement("span");
      b3.className = "badge";
      b3.textContent = "Qualität: " + rep.quality;
      reportTopEl.appendChild(b1);
      reportTopEl.appendChild(b2);
      reportTopEl.appendChild(b3);

      cardsEl.innerHTML = "";

      for(const r of (rep.results || [])){
        const card = document.createElement("div");
        card.className = "card";

        const h = document.createElement("h3");
        const left = document.createElement("div");
        left.textContent = r.src_title || r.src_id;

        const right = document.createElement("div");
        right.className = "mini";
        if(r.ok){
          right.textContent = `Dominant: ${r.dominant_label}`;
        }else{
          right.textContent = "Fehler";
        }
        h.appendChild(left);
        h.appendChild(right);
        card.appendChild(h);

        if(!r.ok){
          const p = document.createElement("div");
          p.className = "hint";
          p.textContent = r.error || "unbekannter Fehler";
          card.appendChild(p);
          cardsEl.appendChild(card);
          continue;
        }

        const kpiRow = document.createElement("div");
        kpiRow.className = "kpiRow";

        const k1 = document.createElement("div");
        k1.className = "kpi";
        k1.innerHTML = `<div class="k">Dominanz</div><div class="v">${r.dominant_share_pct}%</div>`;

        const k2 = document.createElement("div");
        k2.className = "kpi";
        k2.innerHTML = `<div class="k">Heterogenität</div><div class="v">${r.heterogeneity_pct}%</div>`;

        kpiRow.appendChild(k1);
        kpiRow.appendChild(k2);

        if(r.stats){
          const st = r.stats;
          const k3 = document.createElement("div");
          k3.className = "kpi";
          const unit = st.unit ? (" " + st.unit) : "";
          k3.innerHTML = `<div class="k">Mittelwert</div><div class="v">${st.mean}${unit}</div>`;
          const k4 = document.createElement("div");
          k4.className = "kpi";
          k4.innerHTML = `<div class="k">Spanne</div><div class="v">${st.min}${unit} – ${st.max}${unit}</div>`;
          kpiRow.appendChild(k3);
          kpiRow.appendChild(k4);
        }

        card.appendChild(kpiRow);

        // Distribution (Top 5)
        const dist = (r.distribution || []).slice(0,5);
        if(dist.length){
          const distWrap = document.createElement("div");
          distWrap.className = "dist";
          for(const d of dist){
            const row = document.createElement("div");
            row.className = "distRow";

            const label = document.createElement("div");
            label.textContent = d.label;

            const pctEl = document.createElement("div");
            pctEl.style.textAlign = "right";
            pctEl.textContent = d.share.toFixed(1) + "%";

            const bar = document.createElement("div");
            bar.className = "bar";
            const fill = document.createElement("i");
            fill.style.width = pct(d.share) + "%";
            bar.appendChild(fill);

            const leftCol = document.createElement("div");
            leftCol.appendChild(label);
            leftCol.appendChild(bar);

            row.appendChild(leftCol);
            row.appendChild(pctEl);
            distWrap.appendChild(row);
          }
          card.appendChild(distWrap);
        }

        // Praxis/Risiken aus erster Karte
        const c0 = (r.cards || [])[0] || null;
        if(c0){
          const sep = document.createElement("div");
          sep.className = "splitLine";
          card.appendChild(sep);

          const sum = document.createElement("div");
          sum.className = "hint";
          sum.style.color = "var(--text)";
          sum.textContent = c0.summary || "";
          card.appendChild(sum);

          if(Array.isArray(c0.practice) && c0.practice.length){
            const u = document.createElement("ul");
            for(const it of c0.practice){
              const li = document.createElement("li");
              li.textContent = it;
              u.appendChild(li);
            }
            card.appendChild(u);
          }

          if(Array.isArray(c0.risks) && c0.risks.length){
            const hr = document.createElement("div");
            hr.className = "splitLine";
            card.appendChild(hr);
            const t = document.createElement("div");
            t.className = "hint";
            t.textContent = "Risiken / Grenzen:";
            card.appendChild(t);

            const u = document.createElement("ul");
            for(const it of c0.risks){
              const li = document.createElement("li");
              li.textContent = it;
              u.appendChild(li);
            }
            card.appendChild(u);
          }
        }

        // Links: Legend + optional Map PNG
        const linkRow = document.createElement("div");
        linkRow.className = "exportRow";
        const aLeg = document.createElement("a");
        aLeg.className = "badge";
        aLeg.href = r.legend_url || "#";
        aLeg.target = "_blank";
        aLeg.rel = "noopener";
        aLeg.textContent = "Legende";

        linkRow.appendChild(aLeg);

        const bb = currentAoiBbox3857();
        if(bb){
          const aPng = document.createElement("a");
          aPng.className = "badge";
          aPng.textContent = "Map PNG";
          aPng.href = "/api/getmap.png?src=" + encodeURIComponent(r.src_id)
                   + "&bbox=" + bb.map(v => v.toFixed(3)).join(",")
                   + "&w=1400&h=900";
          aPng.target = "_blank";
          aPng.rel = "noopener";
          linkRow.appendChild(aPng);
        }

        card.appendChild(linkRow);
        cardsEl.appendChild(card);
      }
    }

    let analyzeTimer = null;
    function scheduleAnalyze(){
      if(analyzeTimer) clearTimeout(analyzeTimer);
      analyzeTimer = setTimeout(runAnalyze, 350);
    }

    async function runAnalyze(){
      try{
        if(!activeGeojson){
          setStatus("keine AOI", "warn");
          return;
        }
        setStatus("Analyse…");
        const quality = document.getElementById("quality").value;

        const body = { feature: activeGeojson, quality: quality };
        const j = await fetchJson("/api/analyze", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(body)
        });

        if(!j.ok){
          setStatus("Fehler", "warn");
          cardsEl.innerHTML = "";
          reportTopEl.innerHTML = "";
          kpiBadge.textContent = "—";
          const err = document.createElement("div");
          err.className = "card";
          err.innerHTML = `<h3>Fehler</h3><div class="hint">${j.error || "Analyse fehlgeschlagen"}</div>`;
          cardsEl.appendChild(err);
          lastReport = null;
          updateExportButtons();
          return;
        }

        setStatus("ok", "good");
        renderReport(j);
      }catch(e){
        setStatus("Fehler", "warn");
        cardsEl.innerHTML = "";
        const err = document.createElement("div");
        err.className = "card";
        err.innerHTML = `<h3>Fehler</h3><div class="hint">${String(e)}</div>`;
        cardsEl.appendChild(err);
        lastReport = null;
        updateExportButtons();
      }
    }

    async function initOverlays(){
      setStatus("Overlays…");
      const j = await fetchJson("/api/overlays");
      if(!j.ok) throw new Error(j.error || "overlays failed");
      overlaysMeta = j.overlays || [];

      const overlayControl = {};
      overlayLayers = {};

      for(const o of overlaysMeta){
        const lyr = L.tileLayer.wms(o.wms_url, {
          layers: o.layer,
          format: "image/png",
          transparent: true,
          version: o.wms_version || "1.3.0"
        });
        overlayLayers[o.id] = lyr;
        overlayControl[o.title] = lyr; // not added by default
      }

      L.control.layers(null, overlayControl, {collapsed:false}).addTo(map);
      setStatus("bereit", "good");
    }

    (async function(){
      await initOverlays();
      clearActive();
    })();
  </script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(
        INDEX_HTML,
        app_title=APP_TITLE,
        landing_url=LANDING_URL,
        cookbook_url=COOKBOOK_URL,
        map_crs=MAP_CRS,
        geojson_crs=GEOJSON_CRS,
    )


# ------------------------------------------------------------
# 10) ENTRYPOINT
# ------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
