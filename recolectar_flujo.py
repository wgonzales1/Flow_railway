"""
recolectar_flujo.py
-------------------
Recolecta datos de flujo de tráfico (Traffic Flow) desde la API de TomTom
para vías principales de Antofagasta, usando puntos fijos sobre cada avenida.

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

API_KEY      = os.environ.get("TOMTOM_API_KEY", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "wgonzales1/Incidentes_railway"
OUTPUT_FILE  = "data/flujo_antofagasta.csv"

ZOOM = 16  # Mayor zoom = segmentos más precisos a nivel calle

# ─────────────────────────────────────────────────────────────
# PUNTOS FIJOS POR VÍA
# Cada punto: (latitud, longitud, nombre_via, tramo)
# Espaciados ~300-400 m entre sí sobre cada vía
# ─────────────────────────────────────────────────────────────
PUNTOS_FIJOS = [

    # ── Av. Pedro Aguirre Cerda (norte a sur, ~10 km) ──────────
    (-23.5440, -70.3882, "Av. Pedro Aguirre Cerda", "Norte - Mall La Portada"),
    (-23.5510, -70.3888, "Av. Pedro Aguirre Cerda", "Norte - La Chimba"),
    (-23.5580, -70.3895, "Av. Pedro Aguirre Cerda", "Norte - sector industrial"),
    (-23.5650, -70.3900, "Av. Pedro Aguirre Cerda", "Norte - acceso ciudad"),
    (-23.5720, -70.3908, "Av. Pedro Aguirre Cerda", "Centro norte"),
    (-23.5790, -70.3915, "Av. Pedro Aguirre Cerda", "Centro norte - Huanchaca"),
    (-23.5860, -70.3920, "Av. Pedro Aguirre Cerda", "Centro - Manuel Verbal"),
    (-23.5930, -70.3928, "Av. Pedro Aguirre Cerda", "Centro"),
    (-23.6000, -70.3935, "Av. Pedro Aguirre Cerda", "Centro sur"),
    (-23.6070, -70.3940, "Av. Pedro Aguirre Cerda", "Sur - Villa Olímpica"),
    (-23.6140, -70.3945, "Av. Pedro Aguirre Cerda", "Sur - sector residencial"),

    # ── Av. Argentina (norte a sur, vía interior) ──────────────
    (-23.5900, -70.3960, "Av. Argentina", "Norte - inicio"),
    (-23.5980, -70.3975, "Av. Argentina", "Centro norte"),
    (-23.6060, -70.3988, "Av. Argentina", "Centro"),
    (-23.6140, -70.3998, "Av. Argentina", "Centro sur"),
    (-23.6220, -70.4005, "Av. Argentina", "Sur - sector población"),
    (-23.6300, -70.4010, "Av. Argentina", "Sur"),
    (-23.6380, -70.4012, "Av. Argentina", "Sur - La Portada"),
    (-23.6460, -70.4010, "Av. Argentina", "Sur - acceso sur"),
    (-23.6540, -70.4008, "Av. Argentina", "Sur extremo"),
    (-23.6620, -70.4007, "Av. Argentina", "Sur - límite"),
    (-23.6700, -70.4007, "Av. Argentina", "Sur - Portezuelo"),

    # ── Av. Balmaceda / Costanera (norte a sur, frente al mar) ─
    (-23.6200, -70.3985, "Av. Balmaceda", "Norte - terminal pesquero"),
    (-23.6260, -70.3978, "Av. Balmaceda", "Norte - sector puerto"),
    (-23.6320, -70.3972, "Av. Balmaceda", "Centro norte - malecón"),
    (-23.6380, -70.3968, "Av. Balmaceda", "Centro - frente playa"),
    (-23.6440, -70.3975, "Av. Balmaceda", "Centro - MallPlaza"),
    (-23.6500, -70.3990, "Av. Balmaceda", "Centro sur - costanera"),
    (-23.6560, -70.4005, "Av. Balmaceda", "Sur - sector recreativo"),
    (-23.6620, -70.4015, "Av. Balmaceda", "Sur - playa Trocadero"),

    # ── Ruta 1 / Acceso Norte ───────────────────────────────────
    (-23.5050, -70.3820, "Ruta 1 Norte", "Acceso norte - km 0"),
    (-23.5150, -70.3838, "Ruta 1 Norte", "Acceso norte - km 1"),
    (-23.5250, -70.3852, "Ruta 1 Norte", "Acceso norte - km 2"),
    (-23.5350, -70.3865, "Ruta 1 Norte", "Acceso norte - km 3"),
    (-23.5450, -70.3877, "Ruta 1 Norte", "Acceso norte - km 4 (empalme PAC)"),

    # ── Acceso Sur (Ruta 1 sur) ─────────────────────────────────
    (-23.7000, -70.4012, "Acceso Sur", "Sur - salida ciudad"),
    (-23.7100, -70.4015, "Acceso Sur", "Sur - km 1"),
    (-23.7200, -70.4018, "Acceso Sur", "Sur - km 2"),
    (-23.7300, -70.4020, "Acceso Sur", "Sur - km 3"),

    # ── Sector Centro (grilla densa ~300 m) ────────────────────
    (-23.6300, -70.3980, "Centro", "Plaza Colón norte"),
    (-23.6300, -70.4020, "Centro", "Sector judicial norte"),
    (-23.6300, -70.4060, "Centro", "Sector oeste norte"),
    (-23.6330, -70.3980, "Centro", "Calle Prat"),
    (-23.6330, -70.4020, "Centro", "Calle San Martín"),
    (-23.6330, -70.4060, "Centro", "Calle Matta"),
    (-23.6360, -70.3980, "Centro", "Plaza Colón"),
    (-23.6360, -70.4020, "Centro", "Centro comercial"),
    (-23.6360, -70.4060, "Centro", "Sector banco / financiero"),
    (-23.6390, -70.3980, "Centro", "Calle Iquique sur"),
    (-23.6390, -70.4020, "Centro", "Av. Grecia"),
    (-23.6390, -70.4060, "Centro", "Sector Uribe"),
    (-23.6420, -70.3980, "Centro", "Sur centro - Latorre"),
    (-23.6420, -70.4020, "Centro", "Sur centro - Condell"),
    (-23.6420, -70.4060, "Centro", "Sur centro - sector poniente"),
    (-23.6450, -70.3980, "Centro", "Acceso MallPlaza este"),
    (-23.6450, -70.4020, "Centro", "MallPlaza - Balmaceda"),
    (-23.6450, -70.4060, "Centro", "Sector poniente sur"),
    (-23.6480, -70.3980, "Centro", "Sur límite centro"),
    (-23.6480, -70.4020, "Centro", "Sur límite centro oeste"),
]

print(f"Total de puntos de muestreo: {len(PUNTOS_FIJOS)}")
for via in ["Av. Pedro Aguirre Cerda", "Av. Argentina", "Av. Balmaceda",
            "Ruta 1 Norte", "Acceso Sur", "Centro"]:
    n = sum(1 for p in PUNTOS_FIJOS if p[2] == via)
    print(f"  {via}: {n} puntos")

# ─────────────────────────────────────────────
# Schema del CSV de salida
# ─────────────────────────────────────────────
COLUMNAS_SCHEMA = {
    "via":                           "object",
    "tramo":                         "object",
    "sample_point_lat":              "float64",
    "sample_point_lon":              "float64",
    "current_speed_kmh":             "float64",
    "free_flow_speed_kmh":           "float64",
    "current_travel_time_seconds":   "float64",
    "free_flow_travel_time_seconds": "float64",
    "confidence":                    "float64",
    "road_closure":                  "bool",
    "frc":                           "object",
    "road_type":                     "object",
    "segment_lat_start":             "float64",
    "segment_lon_start":             "float64",
    "segment_lat_end":               "float64",
    "segment_lon_end":               "float64",
    "coordinates_wkt":               "object",
    "speed_ratio":                   "float64",
    "congestion_level":              "object",
    "created_at":                    "object",
    "hora_del_dia":                  "int64",
    "dia_semana":                    "object",
    "ingestion_batch_id":            "object",
    "ciudad":                        "object",
    "region":                        "object",
    "region_codigo":                 "int64",
}


# ─────────────────────────────────────────────
# Consulta a la API de TomTom Flow
# ─────────────────────────────────────────────
def consultar_flujo_punto(lat, lon):
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{ZOOM}/json"
        f"?key={API_KEY}&point={lat},{lon}&unit=KMPH&openLr=false"
    )
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("flowSegmentData", None)
    except Exception as e:
        print(f"    ⚠️  Error en ({lat}, {lon}): {e}")
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
# Parseo de respuesta
# ─────────────────────────────────────────────
def parsear_segmento(data, lat, lon, via, tramo, ts, batch_id):
    if not data:
        return None

    current_speed  = data.get("currentSpeed")
    freeflow_speed = data.get("freeFlowSpeed")
    current_tt     = data.get("currentTravelTime")
    freeflow_tt    = data.get("freeFlowTravelTime")
    confidence     = data.get("confidence")
    road_closure   = data.get("roadClosure", False)
    frc            = data.get("frc")
    road_type      = data.get("roadType")

    speed_ratio = None
    if current_speed and freeflow_speed and freeflow_speed > 0:
        speed_ratio = round(current_speed / freeflow_speed, 4)

    coords = data.get("coordinates", {}).get("coordinate", [])
    seg_start_lat = seg_start_lon = seg_end_lat = seg_end_lon = None
    wkt = None
    if coords:
        seg_start_lat = coords[0].get("latitude")
        seg_start_lon = coords[0].get("longitude")
        seg_end_lat   = coords[-1].get("latitude")
        seg_end_lon   = coords[-1].get("longitude")
        wkt = "LINESTRING(" + ", ".join(
            f"{c.get('longitude')} {c.get('latitude')}" for c in coords
        ) + ")"

    return {
        "via":                           via,
        "tramo":                         tramo,
        "sample_point_lat":              lat,
        "sample_point_lon":              lon,
        "current_speed_kmh":             current_speed,
        "free_flow_speed_kmh":           freeflow_speed,
        "current_travel_time_seconds":   current_tt,
        "free_flow_travel_time_seconds": freeflow_tt,
        "confidence":                    confidence,
        "road_closure":                  road_closure,
        "frc":                           frc,
        "road_type":                     road_type,
        "segment_lat_start":             seg_start_lat,
        "segment_lon_start":             seg_start_lon,
        "segment_lat_end":               seg_end_lat,
        "segment_lon_end":               seg_end_lon,
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
# Aplicar schema
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
# Exportar a GitHub
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
# Ciclo principal
# ─────────────────────────────────────────────
ciclo = 0

def recolectar():
    global ciclo
    ciclo += 1
    ts = datetime.now(timezone.utc)
    batch_id = f"flujo_batch_{int(ts.timestamp())}"
    print(f"\n[{ts.strftime('%Y-%m-%d %H:%M:%S')} UTC] Ciclo {ciclo} — {len(PUNTOS_FIJOS)} puntos...")

    filas = []
    sin_datos = 0

    for lat, lon, via, tramo in PUNTOS_FIJOS:
        data = consultar_flujo_punto(lat, lon)
        if data:
            fila = parsear_segmento(data, lat, lon, via, tramo, ts, batch_id)
            if fila:
                filas.append(fila)
        else:
            sin_datos += 1
            print(f"    Sin datos: {via} / {tramo}")
        time.sleep(0.1)  # 10 req/s — margen seguro

    print(f"  ✅ Con datos: {len(filas)} | Sin cobertura: {sin_datos}")

    if not filas:
        print("  Sin filas nuevas — se omite escritura")
        return

    df_nuevo = pd.DataFrame(filas)
    df_nuevo = aplicar_schema(df_nuevo)

    # Resumen por vía
    resumen = df_nuevo.groupby("via")["current_speed_kmh"].mean().round(1)
    for via, vel in resumen.items():
        print(f"    {via}: {vel} km/h promedio")

    os.makedirs("data", exist_ok=True)

    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        df_viejo = pd.read_csv(OUTPUT_FILE)
        df_total = pd.concat([df_viejo, df_nuevo], ignore_index=True)
    else:
        df_total = df_nuevo

    df_total = aplicar_schema(df_total)
    df_total.to_csv(OUTPUT_FILE, index=False)
    print(f"  Total acumulado: {len(df_total)} filas en {OUTPUT_FILE}")

    if ciclo % 6 == 0:
        print("  Exportando a GitHub...")
        exportar_a_github()


# ─────────────────────────────────────────────
# Arranque
# ─────────────────────────────────────────────
recolectar()
#cambiar 1 a 20 min
schedule.every(1).minutes.do(recolectar)

print("\nScheduler activo. Recolectando cada 20 min, exportando a GitHub cada 2 horas...")
while True:
    schedule.run_pending()
    time.sleep(1)
