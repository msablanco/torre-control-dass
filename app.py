import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v7.9", layout="wide")

# --- [DICCIONARIO DE COLORES Y CARGA DE DATOS MANTENIDOS] ---
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
    # --- 1. MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        pos_fr = ['FRANJA_PRECIO', 'FRANJA', 'SEGMENTO']
        col_fr = next((c for c in df_ma.columns if c.upper() in pos_fr), None)
        df_ma['FRANJA_PRECIO'] = df_ma[col_fr].fillna('SIN CATEGORIA').astype(str).str.upper() if col_fr else 'SIN CATEGORIA'
        df_ma['Disciplina'] = df_ma.get('Disciplina', pd.Series(['OTRO']*len(df_ma))).fillna('OTRO').astype(str).str.upper()

    # --- 2. SELL OUT (Cálculo MOS) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_stats = pd.DataFrame(columns=['SKU', 'Sell out Total', 'Max_Mensual_3M', 'Cliente'])
    
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        
        mensual = so_raw.groupby(['SKU', 'Cliente', 'MesAnio'])['Cant'].sum().reset_index()
        max_3m = mensual.groupby(['SKU', 'Cliente'])['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        so_total = so_raw.groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        so_stats = so_total.merge(max_3m, on=['SKU', 'Cliente'], how='left')

    # --- 3. STOCK (Última Foto) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    st_dass_grp = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    st_cli_grp = pd.DataFrame(columns=['SKU', 'Stock Clientes', 'Cliente'])
    
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_stk'] = stk_raw['Cliente'].fillna('').astype(str).str.upper().str.strip()
        
        mask_dass = stk_raw['Cliente_stk'].str.contains('DASS', na=False)
        st_dass_grp = stk_raw[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        
        stk_cli = stk_raw[~mask_dass].sort_values('Fecha_dt')
        st_cli_grp = stk_cli.groupby(['SKU', 'Cliente'])['Cant'].last().reset_index().rename(columns={'Cant': 'Stock Clientes'})

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    clis_all = sorted(list(set(so_stats['Cliente'].dropna().unique().tolist() + st_cli_grp['Cliente'].dropna().unique().tolist())))
    f_cli = st.sidebar.multiselect("Seleccionar Cliente", [c for c in clis_all if str(c) not in ['DASS', '0', 'nan']])
    
    if f_cli:
        so_stats = so_stats[so_stats['Cliente'].isin(f_cli)]
        st_cli_grp = st_cli_grp[st_cli_grp['Cliente'].isin(f_cli)]

    # --- 5. MERGE TOTAL ---
    so_sum = so_stats.groupby('SKU')[['Sell out Total', 'Max_Mensual_3M']].sum().reset_index()
    st_cli_sum = st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index()
    df = df_ma.merge(st_dass_grp, on='SKU', how='left').merge(so_sum, on='SKU', how='left').merge(st_cli_sum, on='SKU', how='left').fillna(0)

    # --- 6. VISUALIZACIÓN ---
    st.title("📊 Torre de Control Dass v7.9")

    def safe_pie(dataframe, val_col, name_col, title_str, col_target, use_map=False):
        clean_df = dataframe[dataframe[val_col] > 0]
        if not clean_df.empty:
            fig = px.pie(clean_df, values=val_col, names=name_col, title=title_str, 
                         color=name_col if use_map else None,
                         color_discrete_map=COLOR_MAP_DIS if use_map else None)
            fig.update_traces(textinfo='percent+label')
            col_target.plotly_chart(fig, use_container_width=True)
        else:
            col_target.warning(f"Sin datos: {title_str}")

    st.subheader("📌 Análisis por Disciplina")
    d1, d2, d3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Disciplina', "Stock Dass", d1, True)
    safe_pie(df, 'Sell out Total', 'Disciplina', "Sell Out Total", d2, True)
    safe_pie(df, 'Stock Clientes', 'Disciplina', "Stock Cliente", d3, True)

    st.subheader("🏆 Análisis por Franja Comercial")
    f1, f2, f3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'FRANJA_PRECIO', "Stock Dass / Franja", f1, False)
    safe_pie(df, 'Sell out Total', 'FRANJA_PRECIO', "Sell Out / Franja", f2, False)
    safe_pie(df, 'Stock Clientes', 'FRANJA_PRECIO', "Stock Cliente / Franja", f3, False)

    # --- 7. TABLA EJECUTIVA (MOS Umbral 3) ---
    st.divider()
    st.subheader("🏆 Resumen Ejecutivo: MOS (Umbral 3 meses)")
    
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    
    cols_tabla = ['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']
    df_ver = df[(df['Sell out Total'] > 0) | (df['Stock Clientes'] > 0) | (df['Stock Dass'] > 0)]

    st.dataframe(
        df_ver[cols_tabla].sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True
    )
    st.caption("💡 **MOS (Months on Hand):** Rojo (>3 meses) es sobrestock, Verde (0-1 mes) es rotación alta.")

else:
    st.error("Error al cargar datos.")
