import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v9.1", layout="wide")

# --- 1. CONFIGURACIÓN DE COLORES ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131',
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700',
    'TENIS': '#FFD700', 'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513',
    'FOOTBALL': '#000000', 'FUTBOL': '#000000'
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
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_data()

if data:
    # --- 2. PROCESAMIENTO MAESTRO (Asegurar Columnas) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        # Normalizar Franja
        col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), None)
        df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper() if col_f else 'SIN CAT'
        # Normalizar Disciplina
        col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), None)
        df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper() if col_d else 'OTRO'
        # Normalizar Descripcion
        if 'Descripcion' not in df_ma.columns: df_ma['Descripcion'] = 'SIN DESCRIPCION'

    # --- 3. SELL OUT (Lógica Solicitada: Suma de Unidades) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    df_so_sku = pd.DataFrame(columns=['SKU', 'Sell out Total', 'Max_Mensual_3M'])
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        
        # Suma Total
        df_so_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        
        # Max Mensual (Suma por mes y luego el pico)
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        max_pico = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        df_so_sku = df_so_sku.merge(max_pico, on='SKU', how='left')

    # --- 4. STOCK (Lógica Solicitada: Fecha más Actual) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    df_stk_cli = pd.DataFrame(columns=['SKU', 'Stock Clientes'])
    df_stk_dass = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
        
        # Fecha más actual del archivo
        max_f = stk_raw['Fecha_dt'].max()
        stk_actual = stk_raw[stk_raw['Fecha_dt'] == max_f]
        
        # Separar Clientes vs Dass
        df_stk_cli = stk_actual[~stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
        df_stk_dass = stk_actual[stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})

    # --- 5. UNIÓN Y FILTROS ---
    df = df_ma.merge(df_so_sku, on='SKU', how='left').merge(df_stk_cli, on='SKU', how='left').merge(df_stk_dass, on='SKU', how='left').fillna(0)

    st.sidebar.header("🔍 Filtros")
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df['Disciplina'].unique().tolist()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(df['FRANJA_PRECIO'].unique().tolist()))
    sku_search = st.sidebar.text_input("📦 Buscar SKU")

    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
    if sku_search: df = df[df['SKU'].str.contains(sku_search.upper())]

    # --- 6. DASHBOARD ---
    st.title("📊 Torre de Control Dass v9.1")
    
    # KPIs con subtotales automáticos
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out Total", f"{df['Sell out Total'].sum():,.0f}")
    k2.metric("Stock Clientes (Actual)", f"{df['Stock Clientes'].sum():,.0f}")
    k3.metric("Stock Dass (Actual)", f"{df['Stock Dass'].sum():,.0f}")
    mos_g = df['Stock Clientes'].sum() / df['Max_Mensual_3M'].sum() if df['Max_Mensual_3M'].sum() > 0 else 0
    k4.metric("MOS Global", f"{mos_g:.1f}")

    def safe_pie(dataframe, val_col, name_col, title_str, col_target, use_map=False):
        clean_df = dataframe[dataframe[val_col] > 0]
        if not clean_df.empty:
            fig = px.pie(clean_df, values=val_col, names=name_col, title=title_str, 
                         color=name_col if use_map else None,
                         color_discrete_map=COLOR_MAP_DIS if use_map else None)
            fig.update_traces(textinfo='percent+label')
            col_target.plotly_chart(fig, use_container_width=True)

    st.subheader("📌 Análisis por Disciplina")
    d1, d2, d3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Disciplina', "Stock Dass / Dis", d1, True)
    safe_pie(df, 'Sell out Total', 'Disciplina', "Sell Out / Dis", d2, True)
    safe_pie(df, 'Stock Clientes', 'Disciplina', "Stock Cliente / Dis", d3, True)

    st.subheader("🏷️ Análisis por Franja")
    f1, f2, f3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'FRANJA_PRECIO', "Stock Dass / Franja", f1)
    safe_pie(df, 'Sell out Total', 'FRANJA_PRECIO', "Sell Out / Franja", f2)
    safe_pie(df, 'Stock Clientes', 'FRANJA_PRECIO', "Stock Cliente / Franja", f3)

    # --- 7. TABLA FINAL ---
    st.divider()
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    st.dataframe(
        df[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']]
        .sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
