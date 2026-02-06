import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v7.4", layout="wide")

# --- MAPA DE COLORES FIJOS ---
COLOR_MAP = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131',
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700',
    'TENIS': '#FFD700', 'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513'
}

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
            # Limpieza de columnas y SKUs
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_data()

if data:
    # --- 1. MAESTRO (Base de la pirámide) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    if not df_ma.empty:
        # Forzar SKU a string y limpiar Franja
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        
        # Validación de columna FRANJA_PRECIO
        col_franja = 'FRANJA_PRECIO' if 'FRANJA_PRECIO' in df_ma.columns else ('Franja' if 'Franja' in df_ma.columns else None)
        if col_franja:
            df_ma['FRANJA_PRECIO'] = df_ma[col_franja].fillna('SIN CATEGORIA').astype(str).str.upper()
        else:
            df_ma['FRANJA_PRECIO'] = 'SIN CATEGORIA'
            
        df_ma['Disciplina'] = df_ma.get('Disciplina', pd.Series(['OTRO']*len(df_ma))).fillna('OTRO').astype(str).str.upper()

    # --- 2. STOCK (Dass vs Clientes) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    st_dass_grp = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    st_cli_grp = pd.DataFrame(columns=['SKU', 'Stock Clientes', 'Cliente'])
    
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Cliente_stk'] = stk_raw['Cliente'].fillna('').astype(str).str.upper().str.strip()
        
        mask_dass = stk_raw['Cliente_stk'].str.contains('DASS', na=False)
        st_dass_grp = stk_raw[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        st_cli_grp = stk_raw[~mask_dass].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

    # --- 3. VENTAS ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_final = pd.DataFrame(columns=['SKU', 'Sell out Clientes', 'Cliente'])
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_final = so_raw.groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Clientes'})

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    clis_all = sorted(list(set(so_final['Cliente'].dropna().unique().tolist() + st_cli_grp['Cliente'].dropna().unique().tolist())))
    f_cli = st.sidebar.multiselect("Seleccionar Cliente", [c for c in clis_all if str(c) not in ['DASS', '0', 'nan']])
    
    # Aplicar filtros a las tablas de movimiento
    if f_cli:
        so_final = so_final[so_final['Cliente'].isin(f_cli)]
        st_cli_grp = st_cli_grp[st_cli_grp['Cliente'].isin(f_cli)]
    
    # --- 5. MERGE TOTAL (Partiendo del Maestro para no perder la Franja) ---
    # Sumarizamos movimientos antes del merge
    so_sum = so_final.groupby('SKU')['Sell out Clientes'].sum().reset_index()
    st_cli_sum = st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index()
    
    df = df_ma.merge(st_dass_grp, on='SKU', how='left')
    df = df.merge(so_sum, on='SKU', how='left')
    df = df.merge(st_cli_sum, on='SKU', how='left')
    df = df.fillna(0)

    # Filtro final por si el usuario solo quiere ver lo que tiene movimiento
    df = df[(df['Sell out Clientes'] > 0) | (df['Stock Clientes'] > 0) | (df['Stock Dass'] > 0)]

    # --- 6. DASHBOARD Y TABLA ---
    st.title("📊 Torre de Control Dass v7.4")
    
    # [Gráficos de torta aquí...]
    # (Omitidos en el snippet para brevedad, pero se mantienen igual que v7.3)

    st.subheader("🏆 Resumen Ejecutivo por SKU (Validación de Franja)")
    
    # Cálculo de KPIs
    df['WOS'] = np.where(df['Sell out Clientes'] > 0, df['Stock Clientes'] / (df['Sell out Clientes'] / 4), 0)

    cols_tabla = ['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Clientes', 'Stock Clientes', 'WOS', 'Stock Dass']
    
    st.dataframe(
        df[cols_tabla].sort_values('Sell out Clientes', ascending=False).style.format({
            'Sell out Clientes': '{:,.0f}', 'Stock Clientes': '{:,.0f}', 
            'WOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }), use_container_width=True
    )
else:
    st.error("Error al conectar con los datos.")
