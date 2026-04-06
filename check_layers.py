import duckdb
import os
import zipfile
import shutil

# Caminhos
zip_path = "data/raw/ana/Base_Hidrogr%C3%A1fica_Ottocodificada_2017_50K_-_trecho_de_drenagem.zip"
extract_path = "data/debug_ana"

if os.path.exists(extract_path):
    shutil.rmtree(extract_path)
os.makedirs(extract_path)

with zipfile.ZipFile(zip_path, 'r') as z:
    z.extractall(extract_path)

# Acha o SHP
shp_file = next(os.path.join(root, f) for root, _, files in os.walk(extract_path) for f in files if f.endswith('.shp'))

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

print("--- 1. TESTE DE TRANSFORMAÇÃO ---")
# Se isso retornar NULL, seu ambiente Nix está quebrado para transformações
res_trans = con.execute("SELECT ST_AsText(ST_Transform(ST_Point(-46.0, -18.0), 'EPSG:4326', 'EPSG:3857'))").fetchone()[0]
print(f"Transformação (4326 -> 3857): {res_trans}")

print("\n--- 2. EXTENSÃO REAL DOS DADOS (BOUNDING BOX DO ARQUIVO) ---")
extent = con.execute(f"SELECT ST_AsText(ST_Extent(geom)) FROM st_read('{shp_file}')").fetchone()[0]
print(f"Extent do arquivo (no CRS original): {extent}")

print("\n--- 3. AMOSTRA DE GEOMETRIA (PRIMEIRAS 3 LINHAS) ---")
sample = con.execute(f"SELECT ST_AsText(geom) FROM st_read('{shp_file}') LIMIT 3").fetchall()
for s in sample:
    print(f"Geom: {s[0][:100]}...")

print("\n--- 4. TESTE DE INTERSEÇÃO SEM TRANSFORM ---")
# Vamos ver se algo intercepta se usarmos a BBOX convertida manualmente para 3857
# BBOX aproximada em 3857 para a região:
bbox_3857 = "ST_MakeEnvelope(-8237634, -2049247, -5120707, 591121)"
count = con.execute(f"SELECT count(*) FROM st_read('{shp_file}') WHERE ST_Intersects(geom, {bbox_3857})").fetchone()[0]
print(f"Linhas encontradas usando BBOX em metros (manual): {count}")

shutil.rmtree(extract_path)