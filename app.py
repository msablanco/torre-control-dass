import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v8.9", layout="wide")

# --- 1. COLORES FIJOS (v7.9) ---
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
    # --- 2. MAESTRO DE PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    # Asegurar columnas
    if 'Disciplina' not in df_ma.columns: df_ma['Disciplina'] = 'OTRO'
    df_ma['Disciplina'] = df_ma['Disciplina'].fillna('OTRO').str.upper()
    pos_fr = ['FRANJA_PRECIO', 'FRANJA', 'SEGMENTO']
    col_fr = next((c for c in df_ma.columns if c.upper() in pos_fr), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_fr].fillna('SIN CATEGORIA').str.upper()

    # --- 3. PROCESAMIENTO SELL OUT (Independiente para evitar duplicados) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
    so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
    so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
    so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')

    # --- 4. PROCESAMIENTO STOCK (Lógica de Fechas Auditoría) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
    stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
    stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
    stk_raw['Cliente_limpio'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

    # --- 5. FILTROS SIDEBAR (Recuperados) ---
    st.sidebar.header("🔍 Filtros de Gestión")
    clis_all = sorted(list(set(so_raw['Cliente'].dropna().unique().tolist() + stk_raw['Cliente'].dropna().unique().tolist())))
    f_cli = st.sidebar.multiselect("🤝 Clientes", [c for c in clis_all if "DASS" not in str(c)])
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique().tolist()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(df_ma['FRANJA_PRECIO'].unique().tolist()))
    sku_search = st.sidebar.text_input("📦 Buscar SKU")

    # --- 6. APLICACIÓN DE FILTROS Y CÁLCULO DE TOTALES ---
    so_work = so_raw[so_raw['Cliente'].isin(f_cli)] if f_cli else so_raw
    stk_work = stk_raw[stk_raw['Cliente_limpio'].isin(f_cli)] if f_cli else stk_raw

    # Totales Sell Out (Garantiza los 457.517)
    so_sku_total = so_work.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
    so_mensual = so_work.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
    max_3m = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})

    # Totales Stock Cliente (Foto 15/12)
    f_stk_cli = pd.to_datetime('2025-12-15')
    stk_c = stk_work[(~stk_work['Cliente_limpio'].str.contains('DASS')) & (stk_work['Fecha_dt'] == f_stk_cli)]
    if stk_c.empty: stk_c = stk_work[~stk_work['Cliente_limpio'].str.contains('DASS')].sort_values('Fecha_dt').groupby(['SKU','Cliente']).tail(1)
    stk_c_sum = stk_c.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

    # Totales Stock Dass (Foto 05/02)
    f_stk_dass = pd.to_datetime('2026-02-05')
    stk_d = stk_raw[(stk_raw['Cliente_limpio'].str.contains('DASS')) & (stk_raw['Fecha_dt'] <= f_stk_dass)]
    stk_d_sum = stk_d.sort_values('Fecha_dt').groupby('SKU').tail(1)[['SKU', 'Cant']].rename(columns={'Cant': 'Stock Dass'})

    # --- 7. MERGE FINAL ---
    df = df_ma.merge(so_sku_total, on='SKU', how='left').merge(max_3m, on='SKU', how='left')
    df = df.merge(stk_c_sum, on='SKU', how='left').merge(stk_d_sum, on='SKU', how='left').fillna(0)

    # Filtros extra
    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
    if sku_search: df = df[df['SKU'].str.contains(sku_search.upper())]

    # --- 8. DASHBOARD VISUAL (Recuperado) ---
    st.title("📊 Torre de Control Dass v8.9")
    
    # KPIs SUPERIORES
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out Total", f"{df['Sell out Total'].sum():,.0f}")
    k2.metric("Stock Cliente (15/12)", f"{df['Stock Clientes'].sum():,.0f}")
    k3.metric("Stock Dass (05/02)", f"{df['Stock Dass'].sum():,.0f}")
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

    st.subheader("📌 Participación por Disciplina")
    d1, d2, d3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Disciplina', "Stock Dass", d1, True)
    safe_pie(df, 'Sell out Total', 'Disciplina', "Sell Out Total", d2, True)
    safe_pie(df, 'Stock Clientes', 'Disciplina', "Stock Cliente", d3, True)

    # --- 9. TABLA DETALLE (v8.1) ---
    st.divider()
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    cols_t = ['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']
    
    st.dataframe(
        df[cols_t].sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
