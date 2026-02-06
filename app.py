import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v8.8", layout="wide")

# --- 1. FUNCIÓN DE CONEXIÓN (Indispensable para evitar NameError) ---
@st.cache_data(ttl=600)
def load_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        results = service.files().list(q=f"'{folder_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
        dfs = {}
        for item in results.get('files', []):
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

# CARGA INICIAL
data = load_data()

if data:
    # --- 2. MAESTRO DE PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        # Limpieza de columnas clave
        for col in ['Disciplina', 'FRANJA_PRECIO', 'Descripcion']:
            if col not in df_ma.columns: df_ma[col] = 'OTRO'
            df_ma[col] = df_ma[col].fillna('OTRO').astype(str).str.upper()

    # --- 3. SELL OUT (Aislado para evitar duplicados - Objetivo: 457.517) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_stats = pd.DataFrame(columns=['SKU', 'Sell out Total', 'Max_Mensual_3M'])
    
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        
        # Totales por SKU
        so_total_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        
        # Max Mensual (Suma mensual total, luego el máximo por SKU)
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        max_3m = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        
        so_stats = so_total_sku.merge(max_3m, on='SKU', how='left')

    # --- 4. STOCK (Lógica de "Última Foto" por fecha específica) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    st_cli_sum = pd.DataFrame(columns=['SKU', 'Stock Clientes'])
    st_dass_sum = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_limpio'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

        # A. STOCK CLIENTE: Solo foto al 15/12/2025 (Objetivo: 78.358)
        fecha_cli = pd.to_datetime('2025-12-15')
        stk_cli = stk_raw[(~stk_raw['Cliente_limpio'].str.contains('DASS')) & (stk_raw['Fecha_dt'] == fecha_cli)]
        if stk_cli.empty: # Fallback si no hay fecha exacta: última disponible
            stk_cli = stk_raw[~stk_raw['Cliente_limpio'].str.contains('DASS')].sort_values('Fecha_dt').groupby(['SKU', 'Cliente']).tail(1)
        st_cli_sum = stk_cli.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

        # B. STOCK DASS: Solo foto al 05/02/2026 (Objetivo: 162.199)
        fecha_dass = pd.to_datetime('2026-02-05')
        stk_dass = stk_raw[(stk_raw['Cliente_limpio'].str.contains('DASS')) & (stk_raw['Fecha_dt'] <= fecha_dass)]
        stk_dass_ult = stk_dass.sort_values('Fecha_dt').groupby('SKU').tail(1)
        st_dass_sum = stk_dass_ult[['SKU', 'Cant']].rename(columns={'Cant': 'Stock Dass'})

    # --- 5. MERGE FINAL ---
    df = df_ma.merge(so_stats, on='SKU', how='left').merge(st_cli_sum, on='SKU', how='left').merge(st_dass_sum, on='SKU', how='left').fillna(0)

    # --- 6. DASHBOARD ---
    st.title("📊 Torre de Control Dass v8.8")
    
    # KPIs Calibrados
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sell Out Total", f"{df['Sell out Total'].sum():,.0f}") # Objetivo: 457.517
    c2.metric("Stock Cliente (15/12)", f"{df['Stock Clientes'].sum():,.0f}") # Objetivo: 78.358
    c3.metric("Stock Dass (05/02)", f"{df['Stock Dass'].sum():,.0f}") # Objetivo: 162.199
    
    mos_global = df['Stock Clientes'].sum() / df['Max_Mensual_3M'].sum() if df['Max_Mensual_3M'].sum() > 0 else 0
    c4.metric("MOS Global", f"{mos_global:.1f}")

    # Tabla Detalle
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    st.dataframe(
        df.sort_values('Sell out Total', ascending=False),
        hide_index=True,
        use_container_width=True
    )

else:
    st.error("No se detectaron archivos en la carpeta de Drive.")
