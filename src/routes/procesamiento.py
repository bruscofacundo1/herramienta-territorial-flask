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

procesamiento_bp = Blueprint('procesamiento', __name__)

GEOAPIFY_API_KEY = "65de779bc48c42d8a1208a5f5e9320b4"

def geocode_address_geoapify(address):
    url = f"https://api.geoapify.com/v1/geocode/search?text={address}&apiKey={GEOAPIFY_API_KEY}"
    try:
        response = requests.get(url)
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
    df["Direccion_Completa"] = df["Domicilio"] + ", " + df["Localidad"] + ", " + df["Provincia"]
    
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
    
    # Remove rows with failed geocoding
    df_geocoded = df.dropna(subset=["Latitud", "Longitud"])
    
    return df_geocoded

def unify_data(df_clientes, df_ventas):
    # Process sales data
    # Limpiar la columna 'Importe' antes de pivotar
    if 'Importe' in df_ventas.columns:
        df_ventas['Importe'] = df_ventas['Importe'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df_ventas['Importe'] = pd.to_numeric(df_ventas['Importe'], errors='coerce')

    df_ventas_pivot = df_ventas.pivot_table(
        index="Cliente", 
        columns="Producto", 
        values="Importe", 
        aggfunc="sum", 
        fill_value=0
    ).reset_index()
    
    df_ventas_pivot.columns.name = None
        # The 'Cliente' column from df_ventas is already the correct one for merging
    # No need to rename it to 'Cliente_Ventas' as it will be used directly for merging
    
    # Calculate total annual sales
    df_ventas_pivot["Venta_Total_Anual"] = df_ventas_pivot.drop(columns=["Cliente_Ventas"], errors="ignore").sum(axis=1)
    
    # Merge with clients data
    df_unificado = pd.merge(df_clientes, df_ventas_pivot, left_on="Razón Social", right_on="Cliente", how="left")
    
    # Fill NaN values
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

def calcular_frecuencia_visita(row):
    categoria = row.get("Categoria_ABC", "C")
    if categoria == "A":
        return 15
    elif categoria == "B":
        return 30
    else:
        return 60

def calcular_prioridad_final(row):
    venta_total = row.get("Venta_Total_Anual", 0)
    frecuencia = row.get("Frecuencia_Visita_Dias", 60)
    
    if venta_total > 0:
        score_oportunidad = min(100, (venta_total / 10000) * 50)
    else:
        score_oportunidad = 10
    
    score_frecuencia = max(10, 100 - (frecuencia - 15) * 2)
    
    return (score_oportunidad + score_frecuencia) / 2

def cluster_clients(df, num_clusters):
    df_valid = df.dropna(subset=["Latitud", "Longitud"])
    
    if len(df_valid) < num_clusters:
        num_clusters = max(1, len(df_valid))
    
    if len(df_valid) > 0:
        kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
        df_valid["Cluster"] = kmeans.fit_predict(df_valid[["Latitud", "Longitud"]])
        
        # Merge back with original dataframe
        df = df.merge(df_valid[["Razón Social", "Cluster"]], on="Razón Social", how="left")
        df["Cluster"] = df["Cluster"].fillna(-1)
    else:
        df["Cluster"] = -1
    
    return df

def create_dashboard_sheet(df, writer, chart_path):
    # Calculate statistics
    total_clientes = len(df)
    venta_total = df["Venta_Total_Anual"].sum()
    
    # ABC distribution
    abc_counts = df["Categoria_ABC"].value_counts()
    abc_sales = df.groupby("Categoria_ABC")["Venta_Total_Anual"].sum()
    
    # Averages
    score_oportunidad_promedio = df["Score_Prioridad_Final"].mean() if "Score_Prioridad_Final" in df.columns else 0
    distancia_promedio = 0  # Placeholder for distance calculation
    
    # Create dashboard data
    dashboard_data = []
    
    # Header
    dashboard_data.append(["ESTADÍSTICAS GENERALES", ""])
    dashboard_data.append(["", ""])
    dashboard_data.append(["Métrica", "Valor"])
    dashboard_data.append(["--- Distribución de Clientes ---", ""])
    
    # ABC distribution
    for categoria in ["A", "B", "C"]:
        count = abc_counts.get(categoria, 0)
        percentage = (count / total_clientes * 100) if total_clientes > 0 else 0
        dashboard_data.append([f"Cantidad de Clientes \"{categoria}\"", f"{count} ({percentage:.1f}%)"])
    
    dashboard_data.append(["Total", f"{total_clientes}"])
    dashboard_data.append(["--- Promedios ---", ""])
    dashboard_data.append(["Score de Oportunidad Promedio", f"{score_oportunidad_promedio:.1f}"])
    dashboard_data.append(["Distancia Promedio (km)", f"{distancia_promedio:.1f}"])
    
    # Create DataFrame and write to Excel
    dashboard_df = pd.DataFrame(dashboard_data, columns=["Métrica", "Valor"])
    dashboard_df.to_excel(writer, sheet_name="Dashboard", index=False)

@procesamiento_bp.route('/procesar', methods=['POST'])
def procesar_datos():
    try:
        # Check if files are present
        if 'archivo_clientes' not in request.files or 'archivo_ventas' not in request.files:
            return jsonify({'error': 'Faltan archivos'}), 400
        
        archivo_clientes = request.files['archivo_clientes']
        archivo_ventas = request.files['archivo_ventas']
        num_clusters = int(request.form.get('num_clusters', 5))
        
        if archivo_clientes.filename == '' or archivo_ventas.filename == '':
            return jsonify({'error': 'No se seleccionaron archivos'}), 400
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        
        # Save uploaded files
        clientes_path = os.path.join(temp_dir, secure_filename(archivo_clientes.filename))
        ventas_path = os.path.join(temp_dir, secure_filename(archivo_ventas.filename))
        
        archivo_clientes.save(clientes_path)
        archivo_ventas.save(ventas_path)
        
        # Read Excel files
        df_clientes = pd.read_excel(clientes_path)
        df_ventas = pd.read_excel(ventas_path, sheet_name="MIX POR CLIENTE", dtype={"Importe": str})
        
        # Process data
        df_unificado = unify_data(df_clientes, df_ventas)
        df_clasificado = clasificar_abc(df_unificado)
        
        # Calculate frequency and priority
        df_clasificado["Frecuencia_Visita_Dias"] = df_clasificado.apply(calcular_frecuencia_visita, axis=1)
        df_clasificado["Score_Prioridad_Final"] = df_clasificado.apply(calcular_prioridad_final, axis=1)
        
        # Geocode addresses
        df_geocoded = geocode_addresses(df_clasificado)
        
        # Cluster clients
        df_clustered = cluster_clients(df_geocoded, num_clusters)
        
        # Create output file
        output_path = os.path.join(temp_dir, 'resultados.xlsx')
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            # Generate chart
            chart_path = os.path.join(temp_dir, 'abc_sales_chart.png')
            generate_abc_sales_chart(df_clustered, chart_path)

            df_clustered.to_excel(writer, sheet_name="Resultados", index=False)
            create_dashboard_sheet(df_clustered, writer, chart_path)

        
        return send_file(output_path, as_attachment=True, download_name='resultados_procesados.xlsx')
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500



def generate_abc_sales_chart(df, output_path):
    abc_sales = df.groupby("Categoria_ABC")["Venta_Total_Anual"].sum().reindex(["A", "B", "C"], fill_value=0)
    
    plt.figure(figsize=(8, 6))
    abc_sales.plot(kind="bar", color=["green", "orange", "red"])
    plt.title("Ventas Totales por Categoría ABC")
    plt.xlabel("Categoría ABC")
    plt.ylabel("Ventas Totales")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()




    # Insert chart into dashboard
    worksheet = writer.sheets["Dashboard"]
    img = openpyxl.drawing.image.Image(chart_path)
    img.anchor = "D2"  # Position of the chart
    worksheet.add_image(img)


