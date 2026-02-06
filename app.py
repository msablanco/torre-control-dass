import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v8.4", layout="wide")

# --- 1. CONFIGURACIÓN DE COLORES ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131',
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700',
    'TENIS': '#FFD700', 'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513',
    'FOOTBALL': '#000000', 'FUTBOL': '#000000'
}

# --- 2. FUNCIÓN DE CARGA DE DRIVE ---
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
    # --- 3. PROCESAMIENTO MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        pos_fr = ['FRANJA_PRECIO', 'FRANJA', 'SEGMENTO']
        col_fr = next((c for c in df_ma.columns if c.upper() in pos_fr), None)
        df_ma['FRANJA_PRECIO'] = df_ma[col_fr].fillna('SIN CATEGORIA').astype(str).str.upper() if col_fr else 'SIN CATEGORIA'
        df_ma['Disciplina'] = df_ma.get('Disciplina', pd.Series(['OTRO']*len(df_ma))).fillna('OTRO').astype(str).str.upper()

    # --- 4. SELL OUT (Lógica de Suma Mensual para Max 3M) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_stats = pd.DataFrame()
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        
        # 1. Suma por Mes y SKU (Sumamos todos los clientes de ese mes)
        so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        # 2. El máximo de esas sumas mensuales
        max_3m = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        # 3. Sell Out Total por SKU y Cliente (para filtros)
        so_total = so_raw.groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        so_stats = so_total.merge(max_3m, on='SKU', how='left')

    # --- 5. STOCK (Última Foto) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    st_dass_grp = pd.DataFrame()
    st_cli_grp = pd.DataFrame()
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_stk'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
        
        mask_dass = stk_raw['Cliente_stk'].str.contains('DASS', na=False)
        st_dass_grp = stk_raw[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        
        # Última foto de stock por Cliente y SKU
        stk_cli_ult = stk_raw[~mask_dass].sort_values('Fecha_dt').groupby(['SKU', 'Cliente'])['Cant'].last().reset_index()
        st_cli_grp = stk_cli_ult.rename(columns={'Cant': 'Stock Clientes'})

    # --- 6. FILTROS SIDEBAR ---
    st.sidebar.header("🔍 Filtros de Gestión")
    clis_all = sorted(list(set(so_stats['Cliente'].dropna().unique().tolist() + st_cli_grp['Cliente'].dropna().unique().tolist()))) if not so_stats.empty else []
    f_cli = st.sidebar.multiselect("Clientes", [c for c in clis_all if str(c) not in ['DASS', '0', 'nan']])
    f_dis = st.sidebar.multiselect("Disciplinas", sorted(df_ma['Disciplina'].unique().tolist()))
    f_fra = st.sidebar.multiselect("Franjas", sorted(df_ma['FRANJA_PRECIO'].unique().tolist()))
    sku_search = st.sidebar.text_input("Buscar SKU")

    # --- 7. MERGE Y FILTRADO FINAL ---
    if f_cli:
        so_stats = so_stats[so_stats['Cliente'].isin(f_cli)]
        st_cli_grp = st_cli_grp[st_cli_grp['Cliente'].isin(f_cli)]

    so_sum = so_stats.groupby('SKU')[['Sell out Total', 'Max_Mensual_3M']].sum().reset_index() if not so_stats.empty else pd.DataFrame(columns=['SKU','Sell out Total','Max_Mensual_3M'])
    st_cli_sum = st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index() if not st_cli_grp.empty else pd.DataFrame(columns=['SKU','Stock Clientes'])
    
    df = df_ma.merge(st_dass_grp, on='SKU', how='left').merge(so_sum, on='SKU', how='left').merge(st_cli_sum, on='SKU', how='left').fillna(0)

    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
    if sku_search: df = df[df['SKU'].str.contains(sku_search.upper())]

    # --- 8. VISUALIZACIÓN ---
    st.title("📊 Torre de Control Dass v8.4")

    # Gráficos (Mantenidos de v7.9-v8.3)
    def safe_pie(dataframe, val_col, name_col, title_str, col_target, use_map=False):
        clean_df = dataframe[dataframe[val_col] > 0]
        if not clean_df.empty:
            fig = px.pie(clean_df, values=val_col, names=name_col, title=title_str, 
                         color=name_col if use_map else None,
                         color_discrete_map=COLOR_MAP_DIS if use_map else None)
            fig.update_traces(textinfo='percent+label')
            col_target.plotly_chart(fig, use_container_width=True)

    st.subheader("📌 Participación por Disciplina y Franja")
    c1, c2, c3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Disciplina', "Stock Dass", c1, True)
    safe_pie(df, 'Sell out Total', 'Disciplina', "Sell Out Total", c2, True)
    safe_pie(df, 'Stock Clientes', 'Disciplina', "Stock Cliente", c3, True)

    # --- 9. SUBTOTALES Y TABLA ---
    st.divider()
    df_ver = df[(df['Sell out Total'] > 0) | (df['Stock Clientes'] > 0) | (df['Stock Dass'] > 0)].copy()
    df_ver['MOS'] = np.where(df_ver['Max_Mensual_3M'] > 0, df_ver['Stock Clientes'] / df_ver['Max_Mensual_3M'], 0)

    # Fila de Totales Dinámica
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Sell Out Total", f"{df_ver['Sell out Total'].sum():,.0f}")
    t2.metric("Max Mensual (Pico)", f"{df_ver['Max_Mensual_3M'].sum():,.0f}")
    t3.metric("Stock Cliente", f"{df_ver['Stock Clientes'].sum():,.0f}")
    t4.metric("MOS Global", f"{(df_ver['Stock Clientes'].sum() / df_ver['Max_Mensual_3M'].sum() if df_ver['Max_Mensual_3M'].sum() > 0 else 0):.1f}")
    t5.metric("Stock Dass Disp.", f"{df_ver['Stock Dass'].sum():,.0f}")

    st.dataframe(
        df_ver[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']]
        .sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
else:
    st.error("Error crítico: No se cargaron los datos. Verifica secretos de Streamlit.")
