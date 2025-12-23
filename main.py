#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import random
import re
import time
from functools import lru_cache
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, render_template_string, request, send_file
from pyproj import CRS, Transformer, Geod
from urllib.parse import urlencode, urlsplit, urlunsplit

try:
    from shapely.geometry import shape, Point
except Exception:
    shape = None
    Point = None


# ------------------------------------------------------------
# 0) META / CONFIG
# ------------------------------------------------------------

APP_TITLE = os.getenv("APP_TITLE", "FieldLense – fl-bgr-soil (WMS)")

LANDING_URL = os.getenv("LANDING_URL", "https://data-tales.dev/")
COOKBOOK_URL = os.getenv("COOKBOOK_URL", "https://data-tales.dev/cookbook/")

MAP_CRS = os.getenv("MAP_CRS", "EPSG:3857")
GEOJSON_CRS = os.getenv("GEOJSON_CRS", "EPSG:4326")

MAX_AOI_KM2 = float(os.getenv("MAX_AOI_KM2", "25"))

MAX_SAMPLES = int(os.getenv("MAX_SAMPLES", "80"))
DEFAULT_SAMPLES = int(os.getenv("DEFAULT_SAMPLES", "25"))

# Multi-Source Call-Budget (Samples werden ggf. reduziert)
MAX_FEATURE_CALLS = int(os.getenv("MAX_FEATURE_CALLS", "60"))

# GetFeatureInfo – BBOX um Punkt (in Meter, EPSG:3857)
FI_HALF_SIZE_M = float(os.getenv("FI_HALF_SIZE_M", "750"))
FI_IMG_PX = int(os.getenv("FI_IMG_PX", "101"))

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "14"))
HTTP_UA = os.getenv("HTTP_UA", "FieldLense/fl-bgr-soil (+https://data-tales.dev)")

# Optional: zusätzliche Quelle SQR
ENABLE_SQR = os.getenv("ENABLE_SQR", "0").strip().lower() in ("1", "true", "yes", "on")

# Sources Registry (BGR WMS)
SOURCES: Dict[str, Dict[str, Any]] = {
    "buek1000en": {
        "id": "buek1000en",
        "title": "BUEK1000EN – Kartiereinheiten / Leitprofil (1:1.000.000)",
        "wms_url": "https://services.bgr.de/wms/boden/buek1000en/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|buek|mapping|unit|soil)",
        "card_type": "buek_profile",
        "arcgis_rest_base": "",  # optional override
    },
    "boart1000ob": {
        "id": "boart1000ob",
        "title": "BOART1000OB – Bodenart (Textur) Oberboden",
        "wms_url": "https://services.bgr.de/wms/boden/boart1000ob/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|boart|bodenart|texture|oberboden|topsoil)",
        "card_type": "texture_topsoil",
        "arcgis_rest_base": "",
    },
    "nfkwe1000": {
        "id": "nfkwe1000",
        "title": "NFKWE1000 – Nutzbare Feldkapazität (eff. Wurzelraum)",
        "wms_url": "https://services.bgr.de/wms/boden/nfkwe1000/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|nfk|feldkap|water|wurzel|root|available)",
        "card_type": "plant_available_water",
        "arcgis_rest_base": "",
    },
    "physgru1000": {
        "id": "physgru1000",
        "title": "PHYSGRU1000 – Gründigkeit / Bodentiefe / Durchwurzelung",
        "wms_url": "https://services.bgr.de/wms/boden/physgru1000/",
        "wms_version": "1.3.0",
        "layer_hint": r"(^0$|phys|gruend|depth|root|durchwur)",
        "card_type": "soil_depth",
        "arcgis_rest_base": "",
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
        "arcgis_rest_base": "",
    }

PRIMARY_DEFAULT_SOURCE = os.getenv("PRIMARY_DEFAULT_SOURCE", "buek1000en").strip()
EXTRA_SOURCES_DEFAULT = os.getenv(
    "EXTRA_SOURCES_DEFAULT",
    "boart1000ob,nfkwe1000,physgru1000" + (",sqr1000" if ENABLE_SQR else "")
).strip()

# ------------------------------------------------------------
# 0b) SOURCE-SPEZIFISCHE AUSWERTUNGSREGELN (wichtig!)
# ------------------------------------------------------------

# BOART Textur-Gruppen gemäß Legende (Screenshot)
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
    # Sonderflächen (falls geliefert)
    "watt": "Watt",
    "siedlung": "Siedlung",
    "abbau": "Abbauflächen",
    "gewaesser": "Gewässer",
    "gewässer": "Gewässer",
    "sonstige": "Sonstige Flächen",
}

# Technische/irrelevante Felder (sollen NIE als „Klasse“ gewählt werden)
TECH_FIELD_RX = re.compile(
    r"(shape(_)?(area|len(gth)?)|objectid|fid|gid|id$|globalid|perimeter|"
    r"created|updated|editor|timestamp|st_area|st_length|geom|geometry)",
    re.IGNORECASE
)

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

def _pick_attr(attrs: Dict[str, Any], preferred_fields: List[str], numeric: bool) -> Tuple[str, Any]:
    """
    Deterministische Auswahl:
    1) exakte (case-insensitive) Feldnamen aus preferred_fields
    2) substring-match (case-insensitive)
    3) fallback: erstes nicht-technisches Feld, das numeric/non-numeric passt
    """
    if not isinstance(attrs, dict) or not attrs:
        return ("", None)

    # 1) exact
    lower_map = {_canon_key(k): k for k in attrs.keys()}
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

    # 3) fallback: first usable
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

# Regeln je Source ergänzen (kein Umbau deiner Struktur nötig)
SOURCES["buek1000en"].update({
    "value_type": "categorical",
    "unit": "",
    "bin_step": None,
    # BUEK liefert oft ohnehin Text; hier nur „nicht-technisch“ sicherstellen
    "preferred_fields": ["name", "title", "soil", "unit", "leit", "profil", "beschreibung", "bezeichnung"],
})

SOURCES["boart1000ob"].update({
    "value_type": "categorical",
    "unit": "",
    "bin_step": None,
    # WICHTIG: Bodart/Texturcode-Felder bevorzugen
    "preferred_fields": [
        "bodart_gr", "bodartgr", "bodart", "bodenart", "textur", "bodenart_gr",
        "bodenartengruppe", "bodenartgruppe", "bodart_txt", "bodart_bez"
    ],
})

SOURCES["nfkwe1000"].update({
    "value_type": "numeric",
    "unit": "mm",
    "bin_step": 5.0,     # dominant über 5-mm-Bins statt rohe float-Strings
    "preferred_fields": ["nfkwe", "nfk", "value", "pixel", "raster", "mm"],
})

SOURCES["physgru1000"].update({
    "value_type": "numeric",
    "unit": "dm",
    "bin_step": 1.0,     # dm-Klassen sinnvoll in 1-dm-Bins
    "preferred_fields": ["physgru", "gruend", "gründ", "depth", "value", "pixel", "dm"],
})

# ------------------------------------------------------------
# 1) HTTP / URL HELPERS
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


def get_source(src_id: str) -> Dict[str, Any]:
    sid = (src_id or "").strip()
    if not sid:
        sid = PRIMARY_DEFAULT_SOURCE
    if sid not in SOURCES:
        raise ValueError(f"Unknown source: {sid}")
    return SOURCES[sid]


# ------------------------------------------------------------
# 2) WMS CAPABILITIES / LAYER PICK (per source)
# ------------------------------------------------------------

@lru_cache(maxsize=64)
def fetch_wms_capabilities(src_id: str) -> Dict[str, Any]:
    import xml.etree.ElementTree as ET

    src = get_source(src_id)
    wms_url = src["wms_url"]
    wms_version = src.get("wms_version", "1.3.0")

    params = {"SERVICE": "WMS", "REQUEST": "GetCapabilities", "VERSION": wms_version}
    r = _http_get(wms_url, params=params)
    xml_bytes = r.content

    root = ET.fromstring(xml_bytes)

    def strip_ns(tag: str) -> str:
        return tag.split("}", 1)[-1] if "}" in tag else tag

    service_title = ""
    layers: List[Dict[str, str]] = []
    getmap_formats: List[str] = []
    fi_formats: List[str] = []

    for el in root.iter():
        if strip_ns(el.tag) == "Service":
            for c in el:
                if strip_ns(c.tag) == "Title" and (c.text or "").strip():
                    service_title = (c.text or "").strip()
                    break
            break

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
        "service_title": service_title,
        "layers": layers,
        "getmap_formats": getmap_formats,
        "featureinfo_formats": fi_formats,
        "wms_url": wms_url,
        "wms_version": wms_version,
    }


def pick_default_layer(src_id: str) -> str:
    src = get_source(src_id)
    hint = src.get("layer_hint") or ""
    caps = fetch_wms_capabilities(src_id)

    if caps.get("layers"):
        if hint:
            rx = re.compile(hint, re.IGNORECASE)
            for lyr in caps["layers"]:
                hay = " ".join([lyr.get("name", ""), lyr.get("title", ""), lyr.get("abstract", "")])
                if rx.search(hay):
                    return lyr["name"]
        return caps["layers"][0]["name"]
    return ""


def pick_info_format(src_id: str) -> str:
    caps = fetch_wms_capabilities(src_id)
    fmts = [f.lower() for f in caps.get("featureinfo_formats", [])]
    for wanted in ["application/geo+json", "application/json", "application/vnd.geo+json"]:
        if wanted in fmts:
            for f in caps.get("featureinfo_formats", []):
                if f.lower() == wanted:
                    return f
    for wanted in ["text/plain", "text/html", "text/xml", "application/vnd.esri.wms_featureinfo_xml", "application/vnd.esri.wms_raw_xml"]:
        if wanted.lower() in fmts:
            for f in caps.get("featureinfo_formats", []):
                if f.lower() == wanted.lower():
                    return f
    return caps.get("featureinfo_formats", ["text/plain"])[0]


def wms_legend_url(src_id: str, layer: str) -> str:
    src = get_source(src_id)
    return _url_with_params(src["wms_url"], {
        "request": "GetLegendGraphic",
        "version": src.get("wms_version", "1.3.0"),
        "format": "image/png",
        "layer": layer
    })


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
# 4) ARC GIS REST (für BGR WMS-Backends) – optional, robust fallback
# ------------------------------------------------------------

def derive_arcgis_rest_base_from_wms(wms_url: str, override: str = "") -> str:
    """
    BGR Pattern:
      https://services.bgr.de/wms/boden/<svc>/  ->
      https://services.bgr.de/arcgis/rest/services/boden/<svc>/MapServer
    """
    if override:
        return override

    u = (wms_url or "").strip()
    if "services.bgr.de/wms/" not in u:
        return ""

    u = u.split("?", 1)[0].rstrip("/")
    u = u.replace("services.bgr.de/wms/", "services.bgr.de/arcgis/rest/services/")
    return u + "/MapServer"


def _arcgis_layer_id_for_wms_layer(layer: str) -> Optional[int]:
    if (layer or "").strip().isdigit():
        return int(layer.strip())
    return None


def _best_string_from_dict(d: Dict[str, Any]) -> Tuple[str, str]:
    if not isinstance(d, dict) or not d:
        return ("", "")

    key_bonus = [
        ("name", 40), ("title", 35), ("bezeich", 35), ("beschreib", 35),
        ("unit", 30), ("soil", 25), ("leit", 25), ("profil", 25),
        ("klasse", 20), ("class", 20), ("textur", 20), ("bodenart", 20),
        ("nfk", 20), ("wurzel", 15), ("depth", 15), ("sqr", 15),
        ("code", 10), ("symbol", 10), ("value", 8),
    ]

    best_k = ""
    best_v = ""
    best_score = -1.0

    for k, v in d.items():
        if v is None or isinstance(v, (dict, list)):
            continue
        s = str(v).strip()
        if not s:
            continue

        kl = str(k).lower()
        score = 0.0
        for tok, bonus in key_bonus:
            if tok in kl:
                score += bonus
        score += min(len(s), 140)

        if _is_probably_code(s) and not any(t in kl for t in ["code", "value", "unit", "class", "symbol"]):
            score -= 30

        if score > best_score:
            best_score = score
            best_k = str(k)
            best_v = s

    return (best_k, best_v)


def arcgis_query_point(rest_base: str, layer_id: int, lon: float, lat: float) -> Optional[Dict[str, Any]]:
    url = f"{rest_base}/{layer_id}/query"
    params = {
        "f": "pjson",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "false",
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "resultRecordCount": "1",
    }
    r = _http_get(url, params=params)
    j = r.json()
    feats = j.get("features") or []
    if not feats:
        return None
    attrs = feats[0].get("attributes") or {}
    return {"attributes": attrs, "raw": j}


# ------------------------------------------------------------
# 5) WMS: GetFeatureInfo / GetMap
# ------------------------------------------------------------

def wms_getfeatureinfo_point(src_id: str, lon: float, lat: float, layer: str) -> Dict[str, Any]:
    src = get_source(src_id)
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
            return {"ok": True, "source": "wms", "format": "json", "data": json.loads(txt)}
        except Exception:
            return {"ok": True, "source": "wms", "format": "text", "data": txt}
    return {"ok": True, "source": "wms", "format": "text", "data": txt}


def wms_getmap_png(src_id: str, bbox_3857: Tuple[float, float, float, float], layer: str,
                   width: int = 1024, height: int = 1024) -> bytes:
    src = get_source(src_id)
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
# 6) INTERPRETATION + CARDS
# ------------------------------------------------------------

def interpret_kind(src_id: str, code: str, label: str) -> str:
    src = get_source(src_id)
    ctype = src.get("card_type", "generic")

    # Quelle bestimmt Semantik (nicht nur "code vs text")
    if ctype == "buek_profile":
        return "buek_profile"
    if ctype == "texture_topsoil":
        return "texture_topsoil"
    if ctype == "plant_available_water":
        return "plant_available_water"
    if ctype == "soil_depth":
        return "soil_depth"
    if ctype == "soil_quality":
        return "soil_quality"
    return "generic"


def make_cards_for_source(src_id: str, distribution: List[Dict[str, Any]], legend: str) -> List[Dict[str, Any]]:
    src = get_source(src_id)
    ctype = src.get("card_type", "generic")

    cards: List[Dict[str, Any]] = []
    top = distribution[0] if distribution else {}
    share = float(top.get("share", 0.0))
    code = (top.get("code") or "").strip()
    label = (top.get("label") or "").strip()

    if ctype == "buek_profile":
        cards.append({
            "Titel": "BUEK – Kartiereinheit / Leitprofil (Überblick, nicht schlagscharf)",
            "Dominanz": f"{share:.0f} %",
            "Profil": label or (code or "Unbekannt"),
            "Kurzprofil": (
                "BUEK1000 ist generalisiert (1:1.000.000). Die Ausgabe beschreibt Kartiereinheiten/Leitprofile "
                "und eignet sich als regionaler Standort-Kontext, nicht als parzellengenaue Bodenkartierung."
            ),
            "Praxis": [
                "Als Kontextlayer verwenden (Region/Standorttyp)",
                "Für Feldentscheidungen: höhere Maßstäbe / Landesdaten / Proben ergänzen",
            ],
            "Risiken": [
                "Maßstabsbedingte Generalisierung",
                "Kartiereinheit kann mehrere Subtypen enthalten",
            ],
            "Legende": legend,
        })
        return cards

    if ctype == "texture_topsoil":
        cards.append({
            "Titel": "Bodenart (Oberboden) – Texturklasse (indikativ)",
            "Dominanz": f"{share:.0f} %",
            "Klasse": label or (code or "Unbekannt"),
            "Kurzprofil": "Textur beeinflusst Infiltration, Bearbeitbarkeit, Verschlämmung, Nährstoff-/Wasserhaltevermögen.",
            "Praxis": [
                "Bewässerungs- und Bodenbearbeitung an Textur anlehnen (als grobe Orientierung)",
                "Bei heterogenen Schlägen zonieren (Satellit/Ertrag/EC/Proben)",
            ],
            "Legende": legend,
        })
        return cards

    if ctype == "plant_available_water":
        cards.append({
            "Titel": "Nutzbare Feldkapazität (effektiver Wurzelraum) – Bewässerungs-Kontext",
            "Dominanz": f"{share:.0f} %",
            "Klasse": label or (code or "Unbekannt"),
            "Kurzprofil": "Indikator für pflanzenverfügbares Wasser im Wurzelraum (Planung von Trockenstress-/Bewässerungsfenstern).",
            "Praxis": [
                "Als Rangfolge: niedrig → früher Trockenstress, hoch → mehr Puffer",
                "Mit Kultur/Wurzeltiefe/Management kombinieren",
            ],
            "Legende": legend,
        })
        return cards

    if ctype == "soil_depth":
        cards.append({
            "Titel": "Gründigkeit / Bodentiefe / Durchwurzelung – Standortlimit",
            "Dominanz": f"{share:.0f} %",
            "Klasse": label or (code or "Unbekannt"),
            "Kurzprofil": "Begrenzt Wurzelraum und damit Wasser-/Nährstoffpuffer; relevant für Kulturwahl und Bewässerungsstrategie.",
            "Praxis": [
                "Flachgründig: stressanfälliger, eher konservative Bewässerungsfenster",
                "Tiefgründig: mehr Puffer, stärkere Tiefenerschließung möglich",
            ],
            "Legende": legend,
        })
        return cards

    if ctype == "soil_quality":
        cards.append({
            "Titel": "SQR – Soil Quality Rating (Index)",
            "Dominanz": f"{share:.0f} %",
            "Klasse": label or (code or "Unbekannt"),
            "Kurzprofil": "Indexhafte Standortgüte – gut für grobe Vergleichbarkeit, nicht als alleinige Entscheidungsgrundlage am Schlag.",
            "Praxis": ["Als Ranking/Benchmark nutzen; für Details Textur/NFK/Proben heranziehen"],
            "Legende": legend,
        })
        return cards

    # generic fallback
    cards.append({
        "Titel": "Boden/Standort – Hinweis",
        "Dominanz": f"{share:.0f} %",
        "WMS_Label": label or (code or "Unbekannt"),
        "Kurzprofil": "Der Dienst liefert keine eindeutig interpretierbare Klasse über die aktuelle Heuristik.",
        "Praxis": ["INFO_FORMAT/ArcGIS-Query prüfen", "ggf. anderes Layer/Dienst wählen"],
        "Legende": legend,
    })
    return cards


# ------------------------------------------------------------
# 7) FEATURE EVALUATION (ArcGIS bevorzugt, WMS fallback)
# ------------------------------------------------------------

def evaluate_point(src_id: str, lon: float, lat: float, layer: str) -> Dict[str, Any]:
    src = get_source(src_id)
    vtype = src.get("value_type", "categorical")
    preferred = src.get("preferred_fields", [])
    want_numeric = (vtype == "numeric")

    # 1) ArcGIS Query (wenn ableitbar)
    rest_base = derive_arcgis_rest_base_from_wms(src["wms_url"], override=src.get("arcgis_rest_base", ""))
    layer_id = _arcgis_layer_id_for_wms_layer(layer)

    if rest_base and layer_id is not None:
        try:
            q = arcgis_query_point(rest_base, layer_id, lon, lat)
            if q and isinstance(q.get("attributes"), dict):
                attrs = q["attributes"]

                k, v = _pick_attr(attrs, preferred_fields=preferred, numeric=want_numeric)

                # Fallback: wenn nichts gefunden, nimm das "beste" – aber ohne Technikfelder
                if v is None:
                    # alte Heuristik (verbessert): Technikfelder ausfiltern
                    filtered = {kk: vv for kk, vv in attrs.items() if not TECH_FIELD_RX.search(str(kk))}
                    k, v = _best_string_from_dict(filtered)  # bleibt als letzter Notnagel

                # Normalize
                if want_numeric:
                    fv = _to_float(v)
                    if fv is not None:
                        return {
                            "ok": True, "source": "arcgis",
                            "code": "", "label": str(fv),
                            "value": fv, "picked_field": k,
                            "raw": q.get("raw"),
                        }
                else:
                    s = (str(v).strip() if v is not None else "")
                    if s:
                        # für BOART: Codes nicht abwerten, sondern als Code behandeln
                        code = _normalize_code(s) if _is_probably_code(s) else ""
                        return {
                            "ok": True, "source": "arcgis",
                            "code": code, "label": s[:220],
                            "value": None, "picked_field": k,
                            "raw": q.get("raw"),
                        }
        except Exception:
            pass

    # 2) WMS GetFeatureInfo (Fallback)
    fi = wms_getfeatureinfo_point(src_id, lon, lat, layer=layer)

    if fi.get("format") == "json":
        data = fi.get("data")
        if isinstance(data, dict) and isinstance(data.get("features"), list) and data["features"]:
            props = (data["features"][0] or {}).get("properties") or {}
            if isinstance(props, dict):
                k, v = _pick_attr(props, preferred_fields=preferred, numeric=want_numeric)
                if v is None:
                    filtered = {kk: vv for kk, vv in props.items() if not TECH_FIELD_RX.search(str(kk))}
                    k, v = _best_string_from_dict(filtered)

                if want_numeric:
                    fv = _to_float(v)
                    if fv is not None:
                        return {"ok": True, "source": "wms", "code": "", "label": str(fv), "value": fv, "picked_field": k, "raw": fi}

                s = (str(v).strip() if v is not None else "")
                if s:
                    code = _normalize_code(s) if _is_probably_code(s) else ""
                    return {"ok": True, "source": "wms", "code": code, "label": s[:220], "value": None, "picked_field": k, "raw": fi}

    # Text-Fallback
    txt = str(fi.get("data") or "").strip()

    if want_numeric:
        fv = _to_float(txt)
        if fv is not None:
            return {"ok": True, "source": "wms", "code": "", "label": str(fv), "value": fv, "picked_field": "", "raw": fi}

    first = txt.split()[0] if txt else ""
    code = _normalize_code(first) if _is_probably_code(first) else ""
    label = (txt[:220] if txt else "Unbekannt")
    return {"ok": True, "source": "wms", "code": code, "label": label, "value": None, "picked_field": "", "raw": fi}


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
    src = get_source(src_id)
    legend = wms_legend_url(src_id, layer)

    vtype = src.get("value_type", "categorical")
    unit = src.get("unit", "") or ""
    bin_step = src.get("bin_step", None)

    raw_preview: List[Dict[str, Any]] = []

    # ---------- CATEGORICAL ----------
    if vtype != "numeric":
        counts: Dict[str, Dict[str, Any]] = {}

        for idx, (lon, lat) in enumerate(points):
            ev = evaluate_point(src_id, lon, lat, layer=layer)
            code = (ev.get("code") or "").strip().lower()
            label = (ev.get("label") or "Unbekannt").strip()

            # BOART: Code -> Klartext
            if src_id == "boart1000ob":
                # wenn label schon Klartext ist, lassen
                # wenn label nur Code ist, mappen
                if code and code in BOART_CODE_TO_LABEL_DE:
                    label = BOART_CODE_TO_LABEL_DE[code]
                else:
                    # label könnte "ls" sein → mappen
                    l0 = label.strip().lower()
                    if l0 in BOART_CODE_TO_LABEL_DE:
                        label = BOART_CODE_TO_LABEL_DE[l0]
                        code = l0

            kind = interpret_kind(src_id, code=code, label=label)

            key = (code.upper() if code else label)
            if key not in counts:
                counts[key] = {"code": code.upper() if code else "", "label": label, "kind": kind, "count": 0}
            counts[key]["count"] += 1

            if idx < 6:
                raw_preview.append({
                    "lon": lon, "lat": lat,
                    "code": counts[key]["code"],
                    "label": counts[key]["label"],
                    "debug": {"picked_field": ev.get("picked_field"), "source": ev.get("source")},
                })

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
            "dominant_code": dominant.get("code", ""),
            "dominant_label": dominant.get("label", "Unbekannt"),
            "heterogeneity_pct": round(hetero, 2),
            "distribution": dist[:12],
            "cards": cards,
            "raw_samples_preview": raw_preview,
        }

    # ---------- NUMERIC ----------
    values: List[float] = []
    binned_counts: Dict[str, int] = {}

    for idx, (lon, lat) in enumerate(points):
        ev = evaluate_point(src_id, lon, lat, layer=layer)
        v = ev.get("value", None)

        if v is None:
            # fallback: label könnte numeric-string sein
            v = _to_float(ev.get("label"))

        if v is None:
            continue

        values.append(float(v))

        # Bin für Dominanz / Heterogenität
        if bin_step:
            b = _bin_numeric(float(v), float(bin_step))
            b_label = _format_numeric(b, unit, decimals=0 if bin_step >= 1 else 1)
        else:
            b_label = _format_numeric(float(v), unit, decimals=1)

        binned_counts[b_label] = binned_counts.get(b_label, 0) + 1

        if idx < 6:
            raw_preview.append({
                "lon": lon, "lat": lat,
                "code": "",
                "label": _format_numeric(float(v), unit, decimals=1),
                "debug": {"picked_field": ev.get("picked_field"), "source": ev.get("source")},
            })

    if not values:
        dist = [{"code": "", "label": "Unbekannt", "count": 0, "share": 0.0}]
        cards = make_cards_for_source(src_id, dist, legend=legend)
        return {
            "ok": True,
            "src_id": src_id,
            "src_title": src["title"],
            "layer": layer,
            "legend_url": legend,
            "dominant_code": "",
            "dominant_label": "Unbekannt",
            "heterogeneity_pct": 0.0,
            "distribution": dist,
            "cards": cards,
            "raw_samples_preview": raw_preview,
        }

    mean_v = sum(values) / len(values)
    min_v = min(values)
    max_v = max(values)

    # Distribution aus Bins
    total = sum(binned_counts.values()) or 1
    dist = []
    for lbl, cnt in sorted(binned_counts.items(), key=lambda kv: kv[1], reverse=True):
        dist.append({"code": "", "label": lbl, "count": cnt, "share": 100.0 * (cnt / total)})

    dominant = dist[0] if dist else {"label": _format_numeric(mean_v, unit, decimals=1), "share": 0.0}
    hetero = 100.0 - float(dominant.get("share", 0.0))

    # Karten: wir reichern die erste Karte pro numeric-Quelle mit Mittel/Spanne an
    cards = make_cards_for_source(src_id, dist, legend=legend)
    if cards:
        # einheitliche Felder
        cards[0]["Mittelwert"] = _format_numeric(mean_v, unit, decimals=1)
        cards[0]["Spanne"] = f"{_format_numeric(min_v, unit, 1)} – {_format_numeric(max_v, unit, 1)}"

        # PHYSGRU zusätzlich in cm
        if src_id == "physgru1000":
            cards[0]["Mittelwert (ca.)"] = f"{mean_v:.1f} dm (≈ {mean_v*10:.0f} cm)"
            cards[0]["Spanne (ca.)"] = f"{min_v:.1f}–{max_v:.1f} dm (≈ {min_v*10:.0f}–{max_v*10:.0f} cm)"

    return {
        "ok": True,
        "src_id": src_id,
        "src_title": src["title"],
        "layer": layer,
        "legend_url": legend,
        "dominant_code": "",
        "dominant_label": dominant.get("label", _format_numeric(mean_v, unit, 1)),
        "heterogeneity_pct": round(hetero, 2),
        "distribution": dist[:12],
        "cards": cards,
        "raw_samples_preview": raw_preview,
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


@app.get("/api/sources")
def api_sources():
    out = []
    for sid, s in SOURCES.items():
        out.append({"id": sid, "title": s["title"], "wms_url": s["wms_url"], "wms_version": s.get("wms_version", "1.3.0")})
    out = sorted(out, key=lambda x: x["title"])
    return jsonify({"ok": True, "sources": out, "primary_default": PRIMARY_DEFAULT_SOURCE, "extras_default": EXTRA_SOURCES_DEFAULT})


@app.get("/api/wms-caps")
def api_wms_caps():
    try:
        src_id = (request.args.get("src") or PRIMARY_DEFAULT_SOURCE).strip()
        caps = fetch_wms_capabilities(src_id)
        default_layer = pick_default_layer(src_id)
        return jsonify({
            "ok": True,
            "src_id": src_id,
            "caps": caps,
            "default_layer": default_layer,
            "picked_info_format": pick_info_format(src_id),
            "legend_url": wms_legend_url(src_id, default_layer) if default_layer else "",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/getmap.png")
def api_getmap_png():
    try:
        src_id = (request.args.get("src") or PRIMARY_DEFAULT_SOURCE).strip()
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
        return send_file(BytesIO(png), mimetype="image/png", as_attachment=False, download_name="soil_map.png")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/analyze")
def api_analyze():
    try:
        payload = request.get_json(force=True, silent=False)
        feature = payload.get("feature")
        if not feature or feature.get("type") != "Feature":
            return jsonify({"ok": False, "error": "Body must contain a GeoJSON Feature"}), 400

        geom = feature.get("geometry") or {}
        gtype = geom.get("type")

        primary_src = (payload.get("src") or PRIMARY_DEFAULT_SOURCE).strip()
        primary_layer = (payload.get("layer") or "").strip() or pick_default_layer(primary_src)
        if not primary_layer:
            return jsonify({"ok": False, "error": "No WMS layer available for primary source"}), 500

        extras_enabled = bool(payload.get("extras", True))
        extras_list = (payload.get("extras_list") or EXTRA_SOURCES_DEFAULT).strip()
        extra_src_ids = [x.strip() for x in extras_list.split(",") if x.strip()]
        extra_src_ids = [x for x in extra_src_ids if x in SOURCES and x != primary_src]
        # "Extras" dürfen nie erzwungen werden – wenn nicht erreichbar, wird das Ergebnis je Quelle separat markiert
        sources_to_run = [primary_src] + (extra_src_ids if extras_enabled else [])

        if gtype == "Point":
            lon, lat = geom["coordinates"][0], geom["coordinates"][1]
            results = []
            for sid in sources_to_run:
                try:
                    lyr = pick_default_layer(sid)
                    if sid == primary_src:
                        lyr = primary_layer
                    if not lyr:
                        results.append({"ok": False, "src_id": sid, "error": "No layer"})
                        continue
                    results.append(analyze_points_for_source(sid, [(lon, lat)], layer=lyr))
                except Exception as e:
                    results.append({"ok": False, "src_id": sid, "src_title": SOURCES[sid]["title"], "error": str(e)})

            return jsonify({
                "ok": True,
                "mode": "point",
                "point": {"lon": lon, "lat": lat},
                "primary": {"src_id": primary_src, "layer": primary_layer},
                "results": results,
                "note": "Point = 1 Sample. Zusatzquellen liefern Kontextdaten; keine standortgenaue Bodenkartierung.",
            })

        if gtype in ("Polygon", "MultiPolygon"):
            area_km2 = geodesic_area_km2(geom)
            if area_km2 <= 0:
                return jsonify({"ok": False, "error": "AOI area could not be computed"}), 400
            if area_km2 > MAX_AOI_KM2:
                return jsonify({"ok": False, "error": f"AOI too large ({area_km2:.2f} km²). Max is {MAX_AOI_KM2:.2f} km²."}), 400

            requested_samples = int(payload.get("samples") or DEFAULT_SAMPLES)
            requested_samples = max(5, min(requested_samples, MAX_SAMPLES))

            # Call-Budget: n_sources * n_samples <= MAX_FEATURE_CALLS
            n_sources = max(1, len(sources_to_run))
            max_samples_budget = max(5, MAX_FEATURE_CALLS // n_sources)
            effective_samples = min(requested_samples, max_samples_budget)

            pts = random_points_in_polygon(geom, n=effective_samples, seed=int(time.time()))

            results = []
            for sid in sources_to_run:
                try:
                    lyr = pick_default_layer(sid)
                    if sid == primary_src:
                        lyr = primary_layer
                    if not lyr:
                        results.append({"ok": False, "src_id": sid, "src_title": SOURCES[sid]["title"], "error": "No layer"})
                        continue
                    results.append(analyze_points_for_source(sid, pts, layer=lyr))
                except Exception as e:
                    results.append({"ok": False, "src_id": sid, "src_title": SOURCES[sid]["title"], "error": str(e)})

            return jsonify({
                "ok": True,
                "mode": "aoi",
                "aoi_area_km2": round(area_km2, 4),
                "requested_samples": requested_samples,
                "effective_samples": effective_samples,
                "sources_run": sources_to_run,
                "primary": {"src_id": primary_src, "layer": primary_layer},
                "results": results,
                "note": "AOI = Sampling vieler Punkte im Polygon. Ergebnisse sind indikativ (maßstabsbedingt).",
            })

        return jsonify({"ok": False, "error": f"Unsupported geometry type: {gtype}"}), 400

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
      --bg:#0b0f19; --panel:#111827; --text:#e5e7eb; --muted:#9ca3af;
      --line:#1f2937; --accent:#60a5fa; --danger:#f87171; --ok:#34d399;
      --card:#0f172a; --radius:16px;
    }
    body{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background:var(--bg); color:var(--text); }
    .wrap{ max-width:1200px; margin:0 auto; padding:16px; }
    header{ display:flex; gap:12px; align-items:center; justify-content:space-between; margin-bottom:12px; }
    .brand{ display:flex; flex-direction:column; gap:2px; }
    .brand h1{ margin:0; font-size:18px; }
    .brand .sub{ color:var(--muted); font-size:12px; }

    .grid{ display:grid; grid-template-columns: 1.4fr 1fr; gap:12px; }
    @media (max-width: 980px){ .grid{ grid-template-columns: 1fr; } }

    .panel{ background:var(--panel); border:1px solid var(--line); border-radius:var(--radius); overflow:hidden; }
    #map{ height: 560px; }
    .pbody{ padding:12px; }
    .row{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
    label{ font-size:12px; color:var(--muted); }
    select, input, textarea, button{
      background:#0b1224; color:var(--text); border:1px solid var(--line); border-radius:12px;
      padding:10px 12px; font-size:14px;
    }
    button{ cursor:pointer; }
    button.primary{ border-color: rgba(96,165,250,.5); }
    button.danger{ border-color: rgba(248,113,113,.5); }
    .hint{ color:var(--muted); font-size:12px; line-height:1.35; }
    textarea{ width:100%; min-height:140px; resize:vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size:12px; }

    .cards{ display:grid; grid-template-columns: 1fr; gap:10px; margin-top:10px; }
    .card{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:12px; }
    .card h3{ margin:0 0 6px 0; font-size:14px; }
    .kv{ display:grid; grid-template-columns: 140px 1fr; gap:6px 10px; font-size:13px; }
    .kv div:nth-child(odd){ color:var(--muted); }
    .badge{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; border:1px solid var(--line); }
    .badge.ok{ border-color: rgba(52,211,153,.6); }
    .badge.warn{ border-color: rgba(248,113,113,.6); }

    .hr{ height:1px; background:var(--line); margin:12px 0; }
    .small{ font-size:12px; color:var(--muted); }
    a{ color: var(--accent); text-decoration:none; }

    .chk{ display:flex; gap:8px; align-items:center; margin-top:8px; }
    .chk input{ width:16px; height:16px; }
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="brand">
        <h1>{{app_title}}</h1>
        <div class="sub">
          BGR WMS · AOI Sampling ·
          <a href="{{landing_url}}">Landing</a> · <a href="{{cookbook_url}}">Cookbook</a>
        </div>
      </div>
      <div class="small">CRS: GeoJSON {{geojson_crs}} · Map {{map_crs}}</div>
    </header>

    <div class="grid">
      <div class="panel">
        <div id="map"></div>
        <div class="pbody">
          <div class="row">
            <div>
              <label>Kartenquelle</label><br/>
              <select id="srcSelect" style="min-width:320px"></select>
            </div>
            <div>
              <label>Layer</label><br/>
              <select id="layerSelect" style="min-width:320px"></select>
            </div>
            <div>
              <label>Samples (AOI)</label><br/>
              <input id="samples" type="number" min="5" max="80" step="1" value="25" style="width:120px"/>
            </div>
            <div style="flex:1"></div>
            <button class="primary" id="btnAnalyze">Analysieren</button>
            <button id="btnPreview">PNG Preview</button>
            <button class="danger" id="btnClear">Löschen</button>
          </div>

          <div class="chk">
            <input type="checkbox" id="chkExtras" checked />
            <label for="chkExtras" style="margin:0">Mehr Infos (BOART/NFK/PHYSGRU{{ " + optional SQR" if enable_sqr else "" }})</label>
          </div>

          <div class="hint" style="margin-top:8px">
            Zeichne Polygon/Rectangle (AOI) oder klicke für Punkt. Es wird immer nur ein Feature gehalten.
            Ergebnisse sind indikativ (maßstabsbedingt).
          </div>
        </div>
      </div>

      <div class="panel">
        <div class="pbody">
          <label>Aktuelles Feature (GeoJSON, {{geojson_crs}})</label>
          <textarea id="geojson" spellcheck="false"></textarea>

          <div class="hr"></div>

          <div class="row">
            <a id="dlGeojson" href="#" class="badge">GeoJSON herunterladen</a>
            <a id="dlPng" href="#" class="badge">PNG herunterladen</a>
            <a id="legendLink" href="#" class="badge" target="_blank" rel="noopener">Legende</a>
            <span id="status" class="badge">bereit</span>
          </div>

          <div class="cards" id="cards"></div>

          <div class="hr"></div>
          <div class="small">
            AOI = Sampling vieler Punkte im Polygon. Für Feldentscheidungen sind höher aufgelöste Quellen/Proben empfehlenswert.
          </div>
        </div>
      </div>
    </div>
  </div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>

  <script>
    const statusEl = document.getElementById("status");
    const legendLink = document.getElementById("legendLink");

    function setStatus(text, kind=""){
      statusEl.textContent = text;
      statusEl.className = "badge " + (kind || "");
    }

    const map = L.map('map').setView([51.1, 10.4], 6);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap'
    }).addTo(map);

    let wmsLayer = null;
    let activeFeature = null;
    let activeGeojson = null;
    let sources = [];

    const drawn = new L.FeatureGroup();
    map.addLayer(drawn);

    const drawControl = new L.Control.Draw({
      draw: {
        polyline: false, circle: false, circlemarker: false, marker: false,
        polygon: { allowIntersection:false, showArea:true },
        rectangle: true
      },
      edit: { featureGroup: drawn, remove: false }
    });
    map.addControl(drawControl);

    function setActiveFeatureFromLayer(layer){
      drawn.clearLayers();
      drawn.addLayer(layer);
      activeFeature = layer;
      activeGeojson = layer.toGeoJSON();
      document.getElementById("geojson").value = JSON.stringify(activeGeojson, null, 2);
      updateDownloads();
    }

    function clearActive(){
      drawn.clearLayers();
      activeFeature = null;
      activeGeojson = null;
      document.getElementById("geojson").value = "";
      updateDownloads();
      document.getElementById("cards").innerHTML = "";
      setStatus("bereit");
    }

    map.on(L.Draw.Event.CREATED, function (e) {
      setActiveFeatureFromLayer(e.layer);
    });

    map.on("click", (ev) => {
      if(activeFeature) return;
      const m = L.circleMarker(ev.latlng, {radius:7});
      setActiveFeatureFromLayer(m);
    });

    async function loadSources(){
      const r = await fetch("/api/sources");
      const j = await r.json();
      if(!j.ok) throw new Error(j.error || "sources failed");
      sources = j.sources || [];
      const sel = document.getElementById("srcSelect");
      sel.innerHTML = "";
      for(const s of sources){
        const opt = document.createElement("option");
        opt.value = s.id;
        opt.textContent = s.title;
        sel.appendChild(opt);
      }
      sel.value = j.primary_default || (sources[0] ? sources[0].id : "");
      return j;
    }

    async function loadCapsForSrc(srcId){
      setStatus("Capabilities…");
      const r = await fetch("/api/wms-caps?src=" + encodeURIComponent(srcId));
      const j = await r.json();
      if(!j.ok){
        setStatus("Caps Fehler", "warn");
        throw new Error(j.error || "caps failed");
      }
      const layerSel = document.getElementById("layerSelect");
      layerSel.innerHTML = "";
      for(const lyr of (j.caps.layers || [])){
        const opt = document.createElement("option");
        opt.value = lyr.name;
        opt.textContent = (lyr.title ? (lyr.title + " — " + lyr.name) : lyr.name);
        layerSel.appendChild(opt);
      }
      if(j.default_layer){
        layerSel.value = j.default_layer;
        legendLink.href = j.legend_url || "#";
      }
      setStatus("bereit", "ok");
      return j;
    }

    function ensureWmsOverlay(){
      const srcId = document.getElementById("srcSelect").value;
      const layerName = document.getElementById("layerSelect").value;
      const src = sources.find(x => x.id === srcId);
      if(!src) return;

      if(wmsLayer) map.removeLayer(wmsLayer);
      wmsLayer = L.tileLayer.wms(src.wms_url, {
        layers: layerName,
        format: "image/png",
        transparent: true,
        version: src.wms_version || "1.3.0"
      });
      wmsLayer.addTo(map);

      fetch("/api/wms-caps?src=" + encodeURIComponent(srcId))
        .then(r => r.json())
        .then(j => { if(j.ok) legendLink.href = j.legend_url || "#"; })
        .catch(()=>{});
    }

    document.getElementById("srcSelect").addEventListener("change", async () => {
      const srcId = document.getElementById("srcSelect").value;
      await loadCapsForSrc(srcId);
      ensureWmsOverlay();
      updateDownloads();
    });

    document.getElementById("layerSelect").addEventListener("change", () => {
      ensureWmsOverlay();
      updateDownloads();
    });

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

    function updateDownloads(){
      const aGeo = document.getElementById("dlGeojson");
      const aPng = document.getElementById("dlPng");

      if(!activeGeojson){
        aGeo.href = "#"; aGeo.removeAttribute("download");
        aPng.href = "#"; aPng.removeAttribute("download");
        return;
      }

      const blob = new Blob([JSON.stringify(activeGeojson, null, 2)], {type:"application/geo+json"});
      const url = URL.createObjectURL(blob);
      aGeo.href = url;
      aGeo.download = "aoi_epsg4326.geojson";

      const srcId = document.getElementById("srcSelect").value;
      const layerName = document.getElementById("layerSelect").value;

      let bounds = null;
      if(activeFeature.getBounds){
        bounds = activeFeature.getBounds();
      }else{
        const ll = activeFeature.getLatLng();
        bounds = L.latLngBounds([ll.lat-0.01, ll.lng-0.01],[ll.lat+0.01, ll.lng+0.01]);
      }
      const bb = bbox3857FromLeafletBounds(bounds).map(v => v.toFixed(3)).join(",");
      aPng.href = "/api/getmap.png?src=" + encodeURIComponent(srcId)
              + "&layer=" + encodeURIComponent(layerName)
              + "&bbox=" + bb + "&w=1400&h=900";
      aPng.download = "soil_wms_epsg3857.png";
    }

    document.getElementById("btnClear").addEventListener("click", clearActive);

    document.getElementById("btnAnalyze").addEventListener("click", async () => {
      try{
        if(!activeGeojson){
          setStatus("kein Feature", "warn");
          return;
        }
        setStatus("Analyse…");

        const srcId = document.getElementById("srcSelect").value;
        const layerName = document.getElementById("layerSelect").value;
        const samples = parseInt(document.getElementById("samples").value || "25", 10);
        const extras = document.getElementById("chkExtras").checked;

        const body = { feature: activeGeojson, src: srcId, layer: layerName, samples: samples, extras: extras };
        const r = await fetch("/api/analyze", {
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(body)
        });
        const j = await r.json();
        if(!j.ok){
          setStatus("Fehler", "warn");
          renderCards([{Titel:"Fehler", Kurzprofil:j.error || "Analyse fehlgeschlagen"}]);
          return;
        }
        setStatus("ok", "ok");

        const cards = [];
        if(j.mode === "aoi"){
          cards.push({
            Titel:"Ergebnis (AOI)",
            Kurzprofil:`Fläche: ${j.aoi_area_km2} km² · Samples: ${j.effective_samples}/${j.requested_samples}`
          });
        }else{
          cards.push({
            Titel:"Ergebnis (Punkt)",
            Kurzprofil:`Quelle: ${j.primary.src_id}`
          });
        }

        for(const res of (j.results || [])){
          if(!res.ok){
            cards.push({
              Titel:`Quelle: ${res.src_title || res.src_id} – Fehler`,
              Kurzprofil: res.error || "unbekannter Fehler"
            });
            continue;
          }
          const meta = {
            Titel: `Quelle: ${res.src_title}`,
            Kurzprofil: `Dominant: ${res.dominant_label} · Heterogenität: ${res.heterogeneity_pct}%`,
            Legende: res.legend_url || ""
          };
          cards.push(meta);
          for(const c of (res.cards || [])){
            cards.push(c);
          }
        }

        renderCards(cards);

        // legend link: primary
        const primaryRes = (j.results || []).find(x => x.ok && x.src_id === j.primary.src_id);
        if(primaryRes && primaryRes.legend_url){
          legendLink.href = primaryRes.legend_url;
        }

        updateDownloads();
      }catch(e){
        setStatus("Fehler", "warn");
        renderCards([{Titel:"Fehler", Kurzprofil:String(e)}]);
      }
    });

    document.getElementById("btnPreview").addEventListener("click", () => {
      if(!activeFeature){
        setStatus("kein Feature", "warn");
        return;
      }
      updateDownloads();
      setStatus("PNG bereit", "ok");
      document.getElementById("dlPng").click();
    });

    function renderCards(cards){
      const el = document.getElementById("cards");
      el.innerHTML = "";
      for(const c of cards){
        const div = document.createElement("div");
        div.className = "card";
        const h = document.createElement("h3");
        h.textContent = c.Titel || "Karte";
        div.appendChild(h);

        const kv = document.createElement("div");
        kv.className = "kv";

        for(const k of Object.keys(c)){
          if(k === "Titel") continue;
          const v = c[k];
          kv.appendChild(Object.assign(document.createElement("div"), {textContent:k}));

          const vv = document.createElement("div");
          if(Array.isArray(v)){
            vv.textContent = v.join(" · ");
          }else{
            const s = String(v);
            if(s.startsWith("http://") || s.startsWith("https://")){
              const a = document.createElement("a");
              a.href = s; a.target = "_blank"; a.rel = "noopener";
              a.textContent = s;
              vv.appendChild(a);
            }else{
              vv.textContent = s;
            }
          }
          kv.appendChild(vv);
        }
        div.appendChild(kv);
        el.appendChild(div);
      }
    }

    (async function(){
      await loadSources();
      const srcId = document.getElementById("srcSelect").value;
      await loadCapsForSrc(srcId);
      ensureWmsOverlay();
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
        enable_sqr=ENABLE_SQR,
    )


# ------------------------------------------------------------
# 10) ENTRYPOINT
# ------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
