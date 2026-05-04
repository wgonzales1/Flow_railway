"""
recolectar_flujo.py
-------------------
Recolecta datos de flujo de tráfico (Traffic Flow) desde la API de TomTom
para la ciudad de Antofagasta, los acumula en un CSV local y los sube a
GitHub cada 6 ciclos (~2 horas, si se ejecuta cada 20 minutos).

Variables de entorno requeridas:
  TOMTOM_API_KEY  — clave de la API de TomTom
  GITHUB_TOKEN    — token de acceso personal de GitHub
"""

import requests
import pandas as pd
from datetime import datetime, timezone
import os
import time
import schedule
import base64
import itertools

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────
API_KEY      = os.environ.get("TOMTOM_API_KEY", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "wgonzales1/Incidentes_railway"   # ← ajusta si el repo es diferente
OUTPUT_FILE  = "data/flujo_antofagasta.csv"

# Bounding box de Antofagasta (lon_min, lat_min, lon_max, lat_max)
ANTOFAGASTA_BBOX = (-70.45, -23.75, -70.35, -23.55)

# Zoom usado para consultar flowSegmentData (14–18 recomendado para ciudad)
ZOOM = 14

# Resolución de la grilla de puntos de muestreo dentro del bbox
# Más puntos = más cobertura pero más llamadas a la API
GRID_STEP_LON = 0.02   # ~1.5 km aprox.
GRID_STEP_LAT = 0.02

# ─────────────────────────────────────────────
# Schema del CSV de salida
# ─────────────────────────────────────────────
COLUMNAS_SCHEMA = {
    # Identificación
    "sample_point_lon":             "float64",
    "sample_point_lat":             "float64",
    # Datos de flujo
    "current_speed_kmh":            "float64",
    "free_flow_speed_kmh":          "float64",
    "current_travel_time_seconds":  "float64",
    "free_flow_travel_time_seconds":"float64",
    "confidence":                   "float64",
    "road_closure":                 "bool",
    # Clasificación vial
    "frc":                          "object",   # Functional Road Class (FRC0–FRC7)
    "road_type":                    "object",
    # Geometría del segmento
    "segment_lon_start":            "float64",
    "segment_lat_start":            "float64",
    "segment_lon_end":              "float64",
    "segment_lat_end":              "float64",
    "coordinates_wkt":              "object",   # LINESTRING WKT
    # Métricas derivadas
    "speed_ratio":                  "float64",  # current / free_flow (1.0 = flujo libre)
    "congestion_level":             "object",   # libre / lento / congestión / cierre
    # Contexto temporal
    "created_at":                   "object",
    "hora_del_dia":                 "int64",
    "dia_semana":                   "object",
    "ingestion_batch_id":           "object",
    # Contexto geográfico
    "ciudad":                       "object",
    "region":                       "object",
    "region_codigo":                "int64",
}


# ─────────────────────────────────────────────
# Generación de grilla de puntos de muestreo
# ─────────────────────────────────────────────
def generar_grilla(bbox, step_lon, step_lat):
    """Genera lista de (lon, lat) dentro del bounding box."""
    min_lon, min_lat, max_lon, max_lat = bbox
    lons = []
    lat = min_lat
    while lat <= max_lat:
        lons.append(lat)
        lat = round(lat + step_lat, 6)

    puntos = []
    lon = min_lon
    while lon <= max_lon:
        for lat in lons:
            puntos.append((round(lon, 6), round(lat, 6)))
        lon = round(lon + step_lon, 6)
    return puntos


# ─────────────────────────────────────────────
# Consulta a la API de TomTom Flow
# ─────────────────────────────────────────────
def consultar_flujo_punto(lon, lat, zoom=ZOOM):
    """
    Llama a flowSegmentData para un punto dado.
    Retorna el dict de propiedades o None si falla.
    """
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json"
        f"?key={API_KEY}&point={lat},{lon}&unit=KMPH&openLr=false"
    )
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return None   # Sin datos viales en ese punto — normal en zonas sin cobertura
        r.raise_for_status()
        return r.json().get("flowSegmentData", None)
    except Exception as e:
        print(f"    ⚠️  Error en ({lon}, {lat}): {e}")
        return None


# ─────────────────────────────────────────────
# Clasificación de congestión
# ─────────────────────────────────────────────
def clasificar_congestion(speed_ratio, road_closure):
    if road_closure:
        return "cierre"
    if speed_ratio is None:
        return None
    if speed_ratio >= 0.85:
        return "libre"
    if speed_ratio >= 0.60:
        return "lento"
    return "congestión"


# ─────────────────────────────────────────────
# Parseo de respuesta de flujo
# ─────────────────────────────────────────────
def parsear_segmento(data, sample_lon, sample_lat, ts, batch_id):
    """Convierte un dict flowSegmentData en una fila del DataFrame."""
    if not data:
        return None

    current_speed   = data.get("currentSpeed")
    freeflow_speed  = data.get("freeFlowSpeed")
    current_tt      = data.get("currentTravelTime")
    freeflow_tt     = data.get("freeFlowTravelTime")
    confidence      = data.get("confidence")
    road_closure    = data.get("roadClosure", False)
    frc             = data.get("frc")
    road_type       = data.get("roadType")

    # Speed ratio
    speed_ratio = None
    if current_speed and freeflow_speed and freeflow_speed > 0:
        speed_ratio = round(current_speed / freeflow_speed, 4)

    # Geometría del segmento
    coords = data.get("coordinates", {}).get("coordinate", [])
    seg_start_lon = seg_start_lat = seg_end_lon = seg_end_lat = None
    wkt = None
    if coords:
        seg_start_lon = coords[0].get("longitude")
        seg_start_lat = coords[0].get("latitude")
        seg_end_lon   = coords[-1].get("longitude")
        seg_end_lat   = coords[-1].get("latitude")
        wkt = "LINESTRING(" + ", ".join(
            f"{c.get('longitude')} {c.get('latitude')}" for c in coords
        ) + ")"

    return {
        "sample_point_lon":              sample_lon,
        "sample_point_lat":              sample_lat,
        "current_speed_kmh":             current_speed,
        "free_flow_speed_kmh":           freeflow_speed,
        "current_travel_time_seconds":   current_tt,
        "free_flow_travel_time_seconds": freeflow_tt,
        "confidence":                    confidence,
        "road_closure":                  road_closure,
        "frc":                           frc,
        "road_type":                     road_type,
        "segment_lon_start":             seg_start_lon,
        "segment_lat_start":             seg_start_lat,
        "segment_lon_end":               seg_end_lon,
        "segment_lat_end":               seg_end_lat,
        "coordinates_wkt":               wkt,
        "speed_ratio":                   speed_ratio,
        "congestion_level":              clasificar_congestion(speed_ratio, road_closure),
        "created_at":                    ts.isoformat(),
        "hora_del_dia":                  ts.hour,
        "dia_semana":                    ts.strftime("%A"),
        "ingestion_batch_id":            batch_id,
        "ciudad":                        "Antofagasta",
        "region":                        "Antofagasta",
        "region_codigo":                 2,
    }


# ─────────────────────────────────────────────
# Aplicar schema al DataFrame
# ─────────────────────────────────────────────
def aplicar_schema(df):
    for col in COLUMNAS_SCHEMA:
        if col not in df.columns:
            df[col] = None
    df = df[list(COLUMNAS_SCHEMA.keys())]
    for col, dtype in COLUMNAS_SCHEMA.items():
        try:
            if dtype == "bool":
                df[col] = df[col].fillna(False).astype(bool)
            elif dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(str).where(df[col].notna(), other=None)
        except Exception:
            pass
    return df


# ─────────────────────────────────────────────
# Deduplicación de segmentos
# ─────────────────────────────────────────────
def deduplicar(df):
    """
    Elimina segmentos duplicados dentro del mismo batch:
    el mismo segmento vial puede ser devuelto por varios puntos de muestreo.
    Se considera duplicado si tienen el mismo WKT + ingestion_batch_id.
    """
    if "coordinates_wkt" in df.columns and "ingestion_batch_id" in df.columns:
        antes = len(df)
        df = df.drop_duplicates(subset=["coordinates_wkt", "ingestion_batch_id"])
        print(f"  Deduplicados {antes - len(df)} segmentos repetidos dentro del batch")
    return df


# ─────────────────────────────────────────────
# Exportar CSV a GitHub
# ─────────────────────────────────────────────
def exportar_a_github():
    if not GITHUB_TOKEN:
        print("  ⚠️  Sin GITHUB_TOKEN, saltando exportación")
        return
    try:
        with open(OUTPUT_FILE, "rb") as f:
            contenido = base64.b64encode(f.read()).decode()

        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{OUTPUT_FILE}"

        r = requests.get(url, headers=headers)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""

        payload = {
            "message": f"flujo {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC",
            "content": contenido,
            "sha": sha,
        }
        r = requests.put(url, headers=headers, json=payload)
        if r.status_code in (200, 201):
            print(f"  📤 CSV exportado a GitHub ({r.status_code})")
        else:
            print(f"  ⚠️  Error GitHub: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  ⚠️  Excepción exportando: {e}")


# ─────────────────────────────────────────────
# Ciclo principal de recolección
# ─────────────────────────────────────────────
GRILLA = generar_grilla(ANTOFAGASTA_BBOX, GRID_STEP_LON, GRID_STEP_LAT)
print(f"Grilla generada: {len(GRILLA)} puntos de muestreo en Antofagasta")

ciclo = 0

def recolectar():
    global ciclo
    ciclo += 1
    ts = datetime.now(timezone.utc)
    batch_id = f"flujo_batch_{int(ts.timestamp())}"
    print(f"\n[{ts.strftime('%Y-%m-%d %H:%M:%S')} UTC] Ciclo {ciclo} — Consultando flujo...")

    filas = []
    sin_datos = 0
    for lon, lat in GRILLA:
        data = consultar_flujo_punto(lon, lat)
        if data:
            fila = parsear_segmento(data, lon, lat, ts, batch_id)
            if fila:
                filas.append(fila)
        else:
            sin_datos += 1
        time.sleep(0.05)  # ~20 req/s — respetar rate limit de TomTom

    print(f"  Puntos con datos: {len(filas)} | Sin cobertura vial: {sin_datos}")

    if not filas:
        print("  Sin filas nuevas — se omite escritura")
        return

    df_nuevo = pd.DataFrame(filas)
    df_nuevo = aplicar_schema(df_nuevo)
    df_nuevo = deduplicar(df_nuevo)

    os.makedirs("data", exist_ok=True)

    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        df_viejo = pd.read_csv(OUTPUT_FILE)
        df_total = pd.concat([df_viejo, df_nuevo], ignore_index=True)
    else:
        df_total = df_nuevo

    df_total = aplicar_schema(df_total)
    df_total.to_csv(OUTPUT_FILE, index=False)
    print(f"  ✅ Total acumulado: {len(df_total)} filas guardadas en {OUTPUT_FILE}")

    # Estadísticas rápidas del batch actual
    if "current_speed_kmh" in df_nuevo.columns:
        vel_media = df_nuevo["current_speed_kmh"].mean()
        congest = df_nuevo["congestion_level"].value_counts().to_dict()
        print(f"  📊 Velocidad media: {vel_media:.1f} km/h | Niveles: {congest}")

    # Exportar a GitHub cada 6 ciclos (~2 horas)
    if ciclo % 6 == 0:
        print("  Exportando a GitHub...")
        exportar_a_github()


# ─────────────────────────────────────────────
# Arranque
# ─────────────────────────────────────────────
recolectar()
#cambiar 20
schedule.every(5).minutes.do(recolectar)

print(f"\nScheduler activo. Recolectando cada 20 min, exportando a GitHub cada 2 horas (~6 ciclos)...")
while True:
    schedule.run_pending()
    time.sleep(1)