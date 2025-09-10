from flask import Blueprint, request, jsonify, send_file
import pandas as pd
import requests
import time
from sklearn.cluster import KMeans
import math
import os
import tempfile
import matplotlib.pyplot as plt
import io
import openpyxl.drawing.image
from werkzeug.utils import secure_filename
import traceback

procesamiento_bp = Blueprint("procesamiento", __name__)

GEOAPIFY_API_KEY = "65de779bc48c42d8a1208a5f5e9320b4"

# Coordenadas de la base (oficina/depósito) para el cálculo de distancias
BASE_LAT = -34.6  # Ejemplo: Latitud de Buenos Aires
BASE_LNG = -58.4  # Ejemplo: Longitud de Buenos Aires

def geocode_address_geoapify(address):
    url = f"https://api.geoapify.com/v1/geocode/search?text={address}&apiKey={GEOAPIFY_API_KEY}"
    try:
        response = requests.get(url )
        response.raise_for_status()
        data = response.json()
        if data and data["features"]:
            lon = data["features"][0]["geometry"]["coordinates"][0]
            lat = data["features"][0]["geometry"]["coordinates"][1]
            return lat, lon
        else:
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud a Geoapify para {address}: {e}")
        return None, None
    except Exception as e:
        print(f"Error procesando la respuesta de Geoapify para {address}: {e}")
        return None, None

def geocode_addresses(df):
    if "Provincia" in df.columns:
        df["Direccion_Completa"] = df["Domicilio"] + ", " + df["Localidad"] + ", " + df["Provincia"]
    else:
        df["Direccion_Completa"] = df["Domicilio"] + ", " + df["Localidad"]
    
    latitudes = []
    longitudes = []
    
    total_addresses = len(df)
    for index, row in df.iterrows():
        address = row["Direccion_Completa"]
        lat, lon = geocode_address_geoapify(address)
        latitudes.append(lat)
        longitudes.append(lon)
        time.sleep(0.1)  # Rate limiting
    
    df["Latitud"] = latitudes
    df["Longitud"] = longitudes
    
    df_geocoded = df.dropna(subset=["Latitud", "Longitud"])
    
    return df_geocoded

def unify_data(df_clientes, df_ventas):
    month_columns = [col for col in df_ventas.columns if col in ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]]
    if "Importe PS" in df_ventas.columns:
        month_columns.append("Importe PS")
    elif "IMPORTE" in df_ventas.columns:
        month_columns.append("IMPORTE")

    for col in month_columns:
        df_ventas[col] = pd.to_numeric(df_ventas[col], errors="coerce").fillna(0)

    df_ventas_grouped = df_ventas.groupby("Cliente")[month_columns].sum().reset_index()

    if "Importe PS" in df_ventas.columns:
        df_ventas_grouped["Venta_Total_Anual"] = df_ventas_grouped["Importe PS"]
    elif "IMPORTE" in df_ventas.columns:
        df_ventas_grouped["Venta_Total_Anual"] = df_ventas_grouped["IMPORTE"]
    else:
        df_ventas_grouped["Venta_Total_Anual"] = df_ventas_grouped[month_columns].sum(axis=1)

    df_unificado = pd.merge(df_clientes, df_ventas_grouped, left_on="Razón Social", right_on="Cliente", how="left")
    df_unificado["Venta_Total_Anual"] = df_unificado["Venta_Total_Anual"].fillna(0)
    
    return df_unificado

def clasificar_abc(df):
    df_sorted = df.sort_values("Venta_Total_Anual", ascending=False).reset_index(drop=True)
    
    total_ventas = df_sorted["Venta_Total_Anual"].sum()
    if total_ventas == 0:
        df_sorted["Categoria_ABC"] = "C"
        return df_sorted
    
    df_sorted["venta_acumulada"] = df_sorted["Venta_Total_Anual"].cumsum()
    df_sorted["porcentaje_acumulado"] = df_sorted["venta_acumulada"] / total_ventas
    
    df_sorted["Categoria_ABC"] = df_sorted["porcentaje_acumulado"].apply(
        lambda x: "A" if x <= 0.8 else ("B" if x <= 0.95 else "C")
    )
    
    return df_sorted

def calcular_score_oportunidad(row):
    score = 0

    # Factor 1: Categoría ABC (40%)
    categoria_abc = row.get("Categoria_ABC", "C")
    if categoria_abc == "A":
        score += 40
    elif categoria_abc == "B":
        score += 25
    elif categoria_abc == "C":
        score += 10

    # Factor 2: Segmento (30%)
    segmento = row.get("Segmento", "Otros/Sin datos")
    if segmento == "Distribuidor A":
        score += 30
    elif segmento == "Distribuidor B":
        score += 25
    elif segmento == "Mostrador A":
        score += 20
    elif segmento == "Mostrador B":
        score += 15
    else:
        score += 10

    # Factor 3: Rubro (20%)
    rubro = row.get("Rubro", "Otros")
    if rubro == "Industrial":
        score += 20
    elif rubro == "Eléctrico":
        score += 18
    elif rubro == "Ferretero":
        score += 15
    elif rubro == "Repuestero":
        score += 12
    else:
        score += 10

    # Factor 4: Volumen de Ventas (10%)
    venta_total = row.get("Venta_Total_Anual", 0)
    if venta_total >= 10000000:
        score += 10
    elif venta_total >= 5000000:
        score += 8
    elif venta_total >= 1000000:
        score += 6
    else:
        score += 3

    return score

def calcular_frecuencia_visita(row):
    categoria = row.get("Categoria_ABC", "C")
    score_oportunidad = row.get("Score_Oportunidad", 0)

    if categoria == "A":
        if score_oportunidad >= 80:
            return 7  # Semanal
        else:
            return 14 # Quincenal
    elif categoria == "B":
        return 21 # Cada 3 semanas
    else: # C
        return 30 # Mensual

def calcular_distancia(lat1, lng1, lat2, lng2):
    R = 6371 # Radio de la Tierra en km
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)

    a = (math.sin(dlat/2) * math.sin(dlat/2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlng/2) * math.sin(dlng/2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distancia = R * c
    return distancia

def calcular_prioridad_final(row):
    score_oportunidad_norm = row.get("Score_Oportunidad", 0) / 100  # Normalizado 0-1
    
    # Asegurarse de que Latitud y Longitud existan antes de calcular distancia
    if pd.isna(row.get("Latitud")) or pd.isna(row.get("Longitud")):
        distancia_desde_base = 999999 # Asignar un valor muy alto si no hay coordenadas
    else:
        distancia_desde_base = calcular_distancia(BASE_LAT, BASE_LNG, row["Latitud"], row["Longitud"])

    # Normalización de distancia: Inversa normalizada
    if distancia_desde_base == 0:
        distancia_norm = 1.0 # Si está en la base, score máximo
    else:
        distancia_norm = 1 / (1 + distancia_desde_base / 10)

    frecuencia_visita_dias = row.get("Frecuencia_Visita_Dias", 30)
    # Normalización de frecuencia: Inversa normalizada
    if frecuencia_visita_dias == 0:
        frecuencia_norm = 1.0 # Si la frecuencia es 0, score máximo
    else:
        frecuencia_norm = 1 / (frecuencia_visita_dias / 30)

    # Pesos según el PDF
    peso_oportunidad = 0.5 # 50%
    peso_distancia = 0.2 # 20%
    peso_frecuencia = 0.3 # 30%

    score_final = (score_oportunidad_norm * peso_oportunidad +
                   distancia_norm * peso_distancia +
                   frecuencia_norm * peso_frecuencia) * 100
    
    return score_final

def cluster_clients(df, num_clusters):
    df_valid = df.dropna(subset=["Latitud", "Longitud"])
    
    if len(df_valid) < num_clusters:
        num_clusters = max(1, len(df_valid))
    
    if len(df_valid) > 0:
        kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
        df_valid["Cluster"] = kmeans.fit_predict(df_valid[["Latitud", "Longitud"]])
        
        df = df.merge(df_valid[["Razón Social", "Cluster"]], on="Razón Social", how="left")
        df["Cluster"] = df["Cluster"].fillna(-1)
    else:
        df["Cluster"] = -1
    
    return df

def create_my_maps_sheet(df, writer):
    """
    Crea la hoja My Maps con formato simplificado para Google My Maps
    Incluye: nombre, latitud, longitud, cluster
    """
    # Filtrar solo clientes geocodificados
    df_geocoded = df.dropna(subset=["Latitud", "Longitud"]).copy()
    
    if len(df_geocoded) == 0:
        # Si no hay clientes geocodificados, crear hoja vacía con headers
        my_maps_data = pd.DataFrame(columns=["nombre", "latitud", "longitud", "cluster"])
    else:
        # Crear DataFrame simplificado para My Maps
        my_maps_data = pd.DataFrame({
            "nombre": df_geocoded["Razón Social"],
            "latitud": df_geocoded["Latitud"],
            "longitud": df_geocoded["Longitud"],
            "cluster": df_geocoded["Cluster"].apply(lambda x: f"Zona {int(x)+1}" if x != -1 else "Sin Zona")
        })
    
    # Guardar en la primera hoja
    my_maps_data.to_excel(writer, sheet_name="My Maps", index=False)
    
    return len(my_maps_data)

def create_dashboard_sheet(df, writer, chart_path):
    total_clientes = len(df)
    venta_total = df["Venta_Total_Anual"].sum()
    
    abc_counts = df["Categoria_ABC"].value_counts()
    abc_sales = df.groupby("Categoria_ABC")["Venta_Total_Anual"].sum()
    
    score_oportunidad_promedio = df["Score_Oportunidad"].mean() if "Score_Oportunidad" in df.columns else 0
    score_prioridad_promedio = df["Score_Prioridad_Final"].mean() if "Score_Prioridad_Final" in df.columns else 0
    
    # Calcular distancia promedio solo para clientes geocodificados
    df_geocoded_only = df.dropna(subset=["Latitud", "Longitud"])
    distancia_promedio = df_geocoded_only["Distancia_Desde_Base"].mean() if "Distancia_Desde_Base" in df_geocoded_only.columns else 0

    dashboard_data = []
    
    dashboard_data.append(["ESTADÍSTICAS GENERALES", ""])
    dashboard_data.append(["", ""])
    dashboard_data.append(["Métrica", "Valor"])
    dashboard_data.append(["--- Distribución de Clientes ---", ""])
    
    for categoria in ["A", "B", "C"]:
        count = abc_counts.get(categoria, 0)
        percentage = (count / total_clientes * 100) if total_clientes > 0 else 0
        dashboard_data.append([f'Cantidad de Clientes "{categoria}"', f"{count} ({percentage:.1f}%)"])
    
    dashboard_data.append(["Total", f"{total_clientes}"])
    dashboard_data.append(["--- Promedios ---", ""])
    dashboard_data.append(["Score de Oportunidad Promedio", f"{score_oportunidad_promedio:.1f}"])
    dashboard_data.append(["Score de Prioridad Promedio", f"{score_prioridad_promedio:.1f}"])
    dashboard_data.append(["Distancia Promedio (km)", f"{distancia_promedio:.1f}"])
    
    # Top 5 Clientes por Priorización
    dashboard_data.append(["", ""])
    dashboard_data.append(["TOP 5 CLIENTES POR PRIORIZACIÓN", ""])
    dashboard_data.append(["Posición: Razón Social", "Score Prioridad Final"])
    if "Score_Prioridad_Final" in df.columns:
        top_5_clientes = df.sort_values(by="Score_Prioridad_Final", ascending=False).head(5)
        for i, (index, row) in enumerate(top_5_clientes.iterrows()):
            dashboard_data.append([str(i + 1) + ": " + row["Razón Social"], str(row["Score_Prioridad_Final"])])
    else:
        dashboard_data.append(["N/A: N/A", "N/A"])

    dashboard_df = pd.DataFrame(dashboard_data, columns=["Métrica", "Valor"])
    dashboard_df.to_excel(writer, sheet_name="Dashboard", index=False)

def generate_abc_sales_chart(df, chart_path):
    abc_sales = df.groupby("Categoria_ABC")["Venta_Total_Anual"].sum()
    if not abc_sales.empty:
        fig, ax = plt.subplots()
        abc_sales.plot(kind="bar", ax=ax, color=["red", "orange", "green"])
        ax.set_title("Ventas por Categoría ABC")
        ax.set_xlabel("Categoría ABC")
        ax.set_ylabel("Venta Total Anual")
        plt.tight_layout()
        plt.savefig(chart_path)
        plt.close(fig)

@procesamiento_bp.route("/procesar", methods=["POST"])
def procesar_datos():
    print("\n--- Inicio de procesar_datos ---")
    print(f"Método de solicitud: {request.method}")
    print(f"Archivos recibidos: {list(request.files.keys())}")
    print(f"Datos de formulario recibidos: {list(request.form.keys())}")
    try:
        if "archivo_clientes" not in request.files or "archivo_ventas" not in request.files:
            return jsonify({"error": "Faltan archivos"}), 400
        
        archivo_clientes = request.files["archivo_clientes"]
        archivo_ventas = request.files["archivo_ventas"]
        num_clusters = int(request.form.get("num_clusters", 5))
        
        if archivo_clientes.filename == "" or archivo_ventas.filename == "":
            return jsonify({"error": "No se seleccionaron archivos"}), 400
        
        temp_dir = tempfile.mkdtemp()
        
        clientes_path = os.path.join(temp_dir, secure_filename(archivo_clientes.filename))
        ventas_path = os.path.join(temp_dir, secure_filename(archivo_ventas.filename))
        
        archivo_clientes.save(clientes_path)
        archivo_ventas.save(ventas_path)
        
        df_clientes = pd.read_excel(clientes_path)
        df_ventas = pd.read_excel(ventas_path, sheet_name="MIX POR CLIENTE")
        
        if "Unnamed: 0" in df_ventas.columns:
            df_ventas = df_ventas.rename(columns={"Unnamed: 0": "Cliente"})
        
        print("Columnas de df_clientes después de la lectura:", df_clientes.columns.tolist())
        print("Columnas de df_ventas después de la lectura:", df_ventas.columns.tolist())
        
        df_unificado = unify_data(df_clientes, df_ventas)
        print("Columnas de df_unificado después de unify_data:", df_unificado.columns.tolist())
        
        df_clasificado = clasificar_abc(df_unificado)
        
        # Asegurarse de que la columna 'Segmento' exista antes de calcular el Score de Oportunidad
        if 'Segmento' not in df_clasificado.columns:
            df_clasificado["Segmento"] = "Otros/Sin datos"

        # Calcular Score de Oportunidad
        df_clasificado["Score_Oportunidad"] = df_clasificado.apply(calcular_score_oportunidad, axis=1)

        # Geocode addresses (antes de calcular distancia)
        df_geocoded = geocode_addresses(df_clasificado)
        
        # Calcular Distancia_Desde_Base
        df_geocoded["Distancia_Desde_Base"] = df_geocoded.apply(
            lambda row: calcular_distancia(BASE_LAT, BASE_LNG, row["Latitud"], row["Longitud"]) 
            if pd.notna(row["Latitud"]) and pd.notna(row["Longitud"]) else None, axis=1
        )

        # Calcular frecuencia (ahora depende de Score_Oportunidad)
        df_geocoded["Frecuencia_Visita_Dias"] = df_geocoded.apply(calcular_frecuencia_visita, axis=1)
        
        # Calcular Score de Prioridad Final
        df_geocoded["Score_Prioridad_Final"] = df_geocoded.apply(calcular_prioridad_final, axis=1)

        # Cluster clients
        df_clustered = cluster_clients(df_geocoded, num_clusters)
        
        output_path = os.path.join(temp_dir, "resultados.xlsx")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            chart_path = os.path.join(temp_dir, "abc_sales_chart.png")
            generate_abc_sales_chart(df_clustered, chart_path)

            # 1. Hoja My Maps (PRIMERA PÁGINA) - nombre, latitud, longitud, cluster
            my_maps_count = create_my_maps_sheet(df_clustered, writer)
            print(f"Hoja My Maps creada con {my_maps_count} registros geocodificados")

            # 2. Hoja de Detalle de Clientes (SEGUNDA PÁGINA)
            df_detalle_clientes = df_clustered[[
                "Razón Social", "Categoria_ABC", "Score_Oportunidad", 
                "Score_Prioridad_Final", "Cluster", "Venta_Total_Anual", 
                "Frecuencia_Visita_Dias", "Distancia_Desde_Base", "Latitud", "Longitud"
            ]].copy()
            df_detalle_clientes.rename(columns={
                "Razón Social": "Cliente",
                "Categoria_ABC": "Categoría ABC",
                "Score_Oportunidad": "Score Oportunidad",
                "Score_Prioridad_Final": "Score Prioridad",
                "Venta_Total_Anual": "Venta Total Anual",
                "Frecuencia_Visita_Dias": "Frecuencia Visita (días)",
                "Distancia_Desde_Base": "Distancia Base (km)"
            }, inplace=True)
            df_detalle_clientes.to_excel(writer, sheet_name="Detalle de Clientes", index=False)

            # 3. Hoja de Dashboard (TERCERA PÁGINA)
            create_dashboard_sheet(df_clustered, writer, chart_path)

            # 4. Hoja de Datos Unificados (CUARTA PÁGINA)
            df_clustered.to_excel(writer, sheet_name="Datos Unificados", index=False)

            # 5. Hojas por Cluster (PÁGINAS SIGUIENTES)
            for cluster_id in sorted(df_clustered["Cluster"].unique()):
                if cluster_id == -1: # Clientes sin geocodificación
                    sheet_name = "Sin Cluster"
                else:
                    sheet_name = f"Cluster {int(cluster_id)}"
                
                df_cluster = df_clustered[df_clustered["Cluster"] == cluster_id].sort_values(by="Score_Prioridad_Final", ascending=False)
                df_cluster_display = df_cluster[[
                    "Razón Social", "Categoria_ABC", "Score_Oportunidad", 
                    "Score_Prioridad_Final", "Cluster", "Venta_Total_Anual", 
                    "Frecuencia_Visita_Dias", "Distancia_Desde_Base"
                ]].copy()
                df_cluster_display.rename(columns={
                    "Razón Social": "Cliente",
                    "Categoria_ABC": "Categoría ABC",
                    "Score_Oportunidad": "Score Oportunidad",
                    "Score_Prioridad_Final": "Score Prioridad",
                    "Venta_Total_Anual": "Venta Total Anual",
                    "Frecuencia_Visita_Dias": "Frecuencia Visita (días)",
                    "Distancia_Desde_Base": "Distancia Base (km)"
                }, inplace=True)
                df_cluster_display.to_excel(writer, sheet_name=sheet_name, index=False)

        return send_file(output_path, as_attachment=True, download_name="resultados_procesados.xlsx")
        
    except Exception as e:
        print(f"Error en procesar_datos: {str(e)}")
        print(traceback.format_exc())
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500