import os
import sys
import subprocess
import requests
import json
import psycopg2
from psycopg2.extras import execute_values, register_hstore
from tqdm import tqdm

# ... (CONFIGURACIÓN PRINCIPAL - SIN CAMBIOS)
COUNTRIES_TO_PROCESS = {
    #"argentina": {"url": "https://download.geofabrik.de/south-america/argentina-latest.osm.pbf"},
    "chile": {"url": "https://download.geofabrik.de/south-america/chile-latest.osm.pbf"},
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

def convert_pbf_to_geojson(pbf_path, geojson_path):
    print(f"Convirtiendo '{pbf_path}' a GeoJSON filtrado...")
    
    # --- CAMBIO APLICADO ---
    # Borra el archivo de salida si ya existe para evitar el error.
    if os.path.exists(geojson_path):
        print(f"El archivo de salida '{geojson_path}' ya existe. Eliminándolo...")
        try:
            os.remove(geojson_path)
        except OSError as e:
            print(f"ERROR: No se pudo eliminar el archivo antiguo. Error: {e}")
            return False

    command = [
        'ogr2ogr', '-f', 'GeoJSON', '-overwrite',
        '-where', OGR2OGR_FILTER,
        geojson_path, pbf_path, 'points'
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        print("Conversión completada con éxito.")
        return True
    except subprocess.CalledProcessError as e:
        print("ERROR: Falló la conversión con ogr2ogr.")
        print(f"Comando: {command}")
        print(f"Error: {e.stderr}")
        return False

def upload_geojson_to_supabase(geojson_path):
    # ... (sin cambios)
    print(f"Iniciando carga de '{geojson_path}' a Supabase...")
    try:
        with psycopg2.connect(**DB_CONFIG, connect_timeout=15) as conn:
            print("-> Conexión a Supabase exitosa.")
            register_hstore(conn)
            with open(geojson_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            features = data.get('features', [])
            total_features = len(features)
            if total_features == 0:
                print("-> No se encontraron puntos de interés en el archivo. Saltando carga.")
                return True

            print(f"-> Se encontraron {total_features} POIs. Preparando para la carga por lotes...")
            data_to_insert = []
            for feature in features:
                properties = feature.get('properties', {})
                geometry = feature.get('geometry', {})
                if not properties or not geometry or geometry.get('type') != 'Point': continue
                name = properties.get('name')
                hstore_properties = {str(k).replace('"', '""'): str(v).replace('"', '""') if v is not None else None for k, v in properties.items()}
                coords = geometry.get('coordinates')
                if not coords or len(coords) < 2: continue
                lon, lat = coords[0], coords[1]
                point_wkt = f"SRID=4326;POINT({lon} {lat})"
                data_to_insert.append((name, hstore_properties, point_wkt))

            with conn.cursor() as cur:
                sql = "INSERT INTO pois (name, tags, geom) VALUES %s"
                execute_values(cur, sql, data_to_insert, page_size=500)
                conn.commit()
            
            print(f"-> ¡Carga completada! Se insertaron {len(data_to_insert)} POIs.")
            return True
    except Exception as e:
        print(f"ERROR: Falló la carga a Supabase. Error: {e}")
        return False
        
def main():
    # ... (sin cambios)
    print("--- INICIANDO PIPELINE DE CARGA DE DATOS GEOESPACIALES ---")
    
    try:
        with psycopg2.connect(**DB_CONFIG, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pois (id SERIAL PRIMARY KEY, name TEXT, tags HSTORE, geom GEOMETRY(Point, 4326));
                    CREATE INDEX IF NOT EXISTS pois_geom_idx ON pois USING GIST (geom);
                """)
                conn.commit()
                print("Tabla 'pois' y su índice espacial verificados/creados.")
    except Exception as e:
        print(f"CRÍTICO: No se pudo conectar a la base de datos para crear la tabla. Abortando. Error: {e}")
        return

    for country, info in COUNTRIES_TO_PROCESS.items():
        print(f"\n================ Procesando: {country.upper()} ================")
        pbf_file = f"{country}-latest.osm.pbf"
        geojson_file = f"{country}_pois_filtrados.geojson"
        
        if not download_file(info["url"], pbf_file): continue
        if not convert_pbf_to_geojson(pbf_file, geojson_file):
            try: os.remove(pbf_file)
            except: pass
            continue
        upload_geojson_to_supabase(geojson_file)
            
        print("Limpiando archivos temporales...")
        try:
            os.remove(pbf_file)
            os.remove(geojson_file)
        except OSError as e:
            print(f"Error al limpiar archivos: {e}")
        print("----------------------------------------------------")
        
    print("\n--- PIPELINE COMPLETADO ---")

if __name__ == "__main__":
    main()