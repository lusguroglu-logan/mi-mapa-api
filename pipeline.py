import os
import sys
import subprocess
import requests
import json
import psycopg2
from psycopg2.extras import execute_values, register_hstore
from tqdm import tqdm

# ==============================================================================
# --- CONFIGURACIÓN PRINCIPAL ---
# ==============================================================================

COUNTRIES_TO_PROCESS = {
    "argentina": {
        "url": "https://download.geofabrik.de/south-america/argentina-latest.osm.pbf",
        "limites_file": "limites_argentina.geojson"
    },
    #"chile": {"url": "https://download.geofabrik.de/south-america/chile-latest.osm.pbf"
    #          , "limites_file": "provincias_chile.geojson"},
    #"uruguay": {"url": "https://download.geofabrik.de/south-america/uruguay-latest.osm.pbf"},
    #"peru": {"url": "https://download.geofabrik.de/south-america/peru-latest.osm.pbf"},
    #"colombia": {"url": "https://download.geofabrik.de/south-america/colombia-latest.osm.pbf"},
    #"ecuador": {"url": "https://download.geofabrik.de/south-america/ecuador-latest.osm.pbf"},
    #"panama": {"url": "https://download.geofabrik.de/central-america/panama-latest.osm.pbf"},
    #"costa_rica": {"url": "https://download.geofabrik.de/central-america/costa-rica-latest.osm.pbf"},
    #"guatemala": {"url": "https://download.geofabrik.de/central-america/guatemala-latest.osm.pbf"},
    #"honduras": {"url": "https://download.geofabrik.de/central-america/honduras-latest.osm.pbf"},
    #"nicaragua": {"url": "https://download.geofabrik.de/central-america/nicaragua-latest.osm.pbf"},
    #"el_salvador": {"url": "https://download.geofabrik.de/central-america/el-salvador-latest.osm.pbf"},
    #"puerto_rico": {"url": "https://download.geofabrik.de/central-america/cuba-latest.osm.pbf"}, # Nota: Puerto Rico está en el archivo de Cuba
    #"republica_dominicana": {"url": "https://download.geofabrik.de/central-america/haiti-and-domrep-latest.osm.pbf"}, # Nota: Rep. Dom. está con Haití
    #"mexico": {"url": "https://download.geofabrik.de/north-america/mexico-latest.osm.pbf"},
    #"estados_unidos": {"url": "https://download.geofabrik.de/north-america/us-latest.osm.pbf"}, # Archivo muy grande
    #"brasil": {"url": "https://download.geofabrik.de/south-america/brazil-latest.osm.pbf"}, # --- URL CORREGIDA ---
    #"espana": {"url": "https://download.geofabrik.de/europe/spain-latest.osm.pbf"},
    #"italia": {"url": "https://download.geofabrik.de/europe/italy-latest.osm.pbf"},

}

DB_CONFIG = {
    "host": "aws-1-sa-east-1.pooler.supabase.com", "port": "6543", "dbname": "postgres",
    "user": "postgres.pvfkmhikrhxdciuypxru", "password": "j2OQpNfA4CNK1z3f"
}

OGR2OGR_FILTER = "other_tags LIKE '%\"amenity\"=>%' OR other_tags LIKE '%\"shop\"=>%' OR other_tags LIKE '%\"tourism\"=>%' OR other_tags LIKE '%\"office\"=>%' OR other_tags LIKE '%\"leisure\"=>%' OR other_tags LIKE '%\"sport\"=>%' OR other_tags LIKE '%\"healthcare\"=>%' OR other_tags LIKE '%\"building\"=>%' OR other_tags LIKE '%\"railway\"=>%'"

# ==============================================================================
# --- LÓGICA DEL PIPELINE ---
# ==============================================================================

def download_file(url, filename):
    # ... (sin cambios)
    print(f"Descargando '{filename}' desde {url}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size = int(r.headers.get('content-length', 0))
            with open(filename, 'wb') as f, tqdm(
                total=total_size, unit='iB', unit_scale=True, unit_divisor=1024,
                desc=filename, file=sys.stdout, miniters=1
            ) as bar:
                for chunk in r.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    bar.update(size)
        return True
    except Exception as e:
        print(f"ERROR: Falló la descarga de {url}. Error: {e}")
        return False

def convert_province_to_geojson(pbf_path, province_geom, geojson_path):
    """Recorta el PBF por la geometría de una provincia y lo convierte a GeoJSON."""
    # Guardar la geometría de la provincia en un archivo temporal
    clip_src_file = "temp_clip.geojson"
    with open(clip_src_file, 'w') as f:
        json.dump(province_geom, f)

    command = [
        'ogr2ogr', '-f', 'GeoJSON', '-overwrite',
        '-clipsrc', clip_src_file,
        '-where', OGR2OGR_FILTER,
        geojson_path, pbf_path, 'points'
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        os.remove(clip_src_file) # Limpiar el archivo temporal
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: Falló la conversión para una provincia. Error: {e.stderr}")
        os.remove(clip_src_file)
        return False

def upload_geojson_to_supabase(geojson_path):
    # ... (sin cambios)
    try:
        with psycopg2.connect(**DB_CONFIG, connect_timeout=15) as conn:
            register_hstore(conn)
            with open(geojson_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            features = data.get('features', [])
            if not features: return True

            data_to_insert = []
            for feature in features:
                properties, geometry = feature.get('properties', {}), feature.get('geometry', {})
                if not properties or not geometry or geometry.get('type') != 'Point': continue
                name = properties.get('name')
                hstore_properties = {str(k): str(v) if v is not None else None for k, v in properties.items()}
                coords = geometry.get('coordinates')
                if not coords or len(coords) < 2: continue
                lon, lat = coords[0], coords[1]
                point_wkt = f"SRID=4326;POINT({lon} {lat})"
                data_to_insert.append((name, hstore_properties, point_wkt))

            with conn.cursor() as cur:
                sql = "INSERT INTO pois (name, tags, geom) VALUES %s"
                execute_values(cur, sql, data_to_insert, page_size=500)
                conn.commit()
            return True
    except Exception as e:
        print(f"\nERROR: Falló la carga a Supabase para una provincia. Error: {e}")
        return False

def main():
    print("--- INICIANDO PIPELINE DE CARGA AUTOMATIZADO ---")
    
    # ... (la parte de crear la tabla sigue igual)

    for country_name, info in COUNTRIES_TO_PROCESS.items():
        print(f"\n================ Procesando País: {country_name.upper()} ================")
        pbf_file = f"{country_name}-latest.osm.pbf"
        limites_file = info["limites_file"]

        if not os.path.exists(limites_file):
            print(f"CRÍTICO: No se encuentra el archivo de límites '{limites_file}'. Saltando país.")
            continue
        
        if not download_file(info["url"], pbf_file): continue

        with open(limites_file, 'r', encoding='utf-8') as f:
            limites_data = json.load(f)
        
        provincias = limites_data.get('features', [])
        print(f"Se procesarán {len(provincias)} subdivisiones para {country_name.upper()}.")

        for provincia in tqdm(provincias, desc=f"Procesando {country_name.upper()}", unit="provincia"):
            prov_name = provincia['properties'].get('nombre') or provincia['properties'].get('NAMEUNIT')
            prov_geom = provincia['geometry']
            
            if not prov_name or not prov_geom: continue
            
            geojson_file = f"temp_{prov_name.replace(' ', '_')}.geojson"
            
            if convert_province_to_geojson(pbf_file, prov_geom, geojson_file):
                upload_geojson_to_supabase(geojson_file)
                os.remove(geojson_file)
        
        print(f"Limpiando el archivo PBF para {country_name.upper()}...")
        os.remove(pbf_file)
        print("----------------------------------------------------")
        
    print("\n--- PIPELINE COMPLETADO ---")

if __name__ == "__main__":
    main()