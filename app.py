import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v8.5", layout="wide")

# --- [CARGA DE DATOS MANTENIDA] ---
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

data = load_data()

if data:
    # --- 1. PROCESAMIENTO SELL OUT (AISLADO PARA EVITAR DUPLICADOS) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        
        # Filtro de clientes para el Sidebar (lo preparamos aquí)
        clis_so = so_raw['Cliente'].dropna().unique().tolist()
    else:
        clis_so = []

    # --- 2. STOCK (AISLADO) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_stk'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
        clis_stk = stk_raw['Cliente'].dropna().unique().tolist()
    else:
        clis_stk = []

    # --- 3. SIDEBAR (FILTROS) ---
    st.sidebar.header("🔍 Filtros")
    clis_all = sorted(list(set(clis_so + clis_stk)))
    f_cli = st.sidebar.multiselect("Clientes", [c for c in clis_all if str(c) not in ['DASS', '0', 'nan']])
    
    # Aplicar filtro de cliente ANTES de cualquier suma
    if f_cli:
        so_working = so_raw[so_raw['Cliente'].isin(f_cli)].copy()
        stk_working = stk_raw[stk_raw['Cliente'].isin(f_cli)].copy()
    else:
        so_working = so_raw.copy()
        stk_working = stk_raw.copy()

    # --- 4. CÁLCULOS POST-FILTRO (PARA TOTALES REALES) ---
    # SELL OUT TOTAL Y MAX MENSUAL (Agrupación pura)
    so_mensual_sku = so_working.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
    max_3m_sku = so_mensual_sku.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
    so_total_sku = so_working.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
    
    # STOCK CLIENTE (Última foto por Cliente/SKU para evitar sumar historial)
    stk_cli_only = stk_working[~stk_working['Cliente_stk'].str.contains('DASS', na=False)]
    stk_ult_foto = stk_cli_only.sort_values('Fecha_dt').groupby(['SKU', 'Cliente'])['Cant'].last().reset_index()
    stk_total_sku = stk_ult_foto.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    
    # STOCK DASS (Siempre del total disponible)
    stk_dass_only = stk_raw[stk_raw['Cliente_stk'].str.contains('DASS', na=False)]
    stk_dass_sku = stk_dass_only.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})

    # --- 5. UNIÓN FINAL (MERGE SOBRE MAESTRO) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    
    # Unimos todo paso a paso
    df = df_ma.merge(so_total_sku, on='SKU', how='left')
    df = df.merge(max_3m_sku, on='SKU', how='left')
    df = df.merge(stk_total_sku, on='SKU', how='left')
    df = df.merge(stk_dass_sku, on='SKU', how='left').fillna(0)

    # --- 6. SUBTOTALES DINÁMICOS ---
    st.title("📊 Torre de Control Dass v8.5")
    
    df_ver = df[(df['Sell out Total'] > 0) | (df['Stock Clientes'] > 0) | (df['Stock Dass'] > 0)].copy()
    
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Sell Out Total", f"{df_ver['Sell out Total'].sum():,.0f}")
    t2.metric("Max Mensual (Pico)", f"{df_ver['Max_Mensual_3M'].sum():,.0f}")
    t3.metric("Stock Cliente", f"{df_ver['Stock Clientes'].sum():,.0f}")
    
    mos_g = df_ver['Stock Clientes'].sum() / df_ver['Max_Mensual_3M'].sum() if df_ver['Max_Mensual_3M'].sum() > 0 else 0
    t4.metric("MOS Global", f"{mos_g:.1f}")
    t5.metric("Stock Dass Disp.", f"{df_ver['Stock Dass'].sum():,.0f}")

    # --- 7. TABLA DETALLE ---
    df_ver['MOS'] = np.where(df_ver['Max_Mensual_3M'] > 0, df_ver['Stock Clientes'] / df_ver['Max_Mensual_3M'], 0)
    
    st.dataframe(
        df_ver[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']]
        .sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
