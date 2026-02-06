import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CARGA Y CONFIGURACIÓN ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000'
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
            df.columns = df.columns.str.strip().str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. MAESTRO PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        df_ma['Disciplina'] = df_ma.get('DISCIPLINA', 'OTRO').fillna('OTRO').astype(str).str.upper()
        df_ma['FRANJA_PRECIO'] = df_ma.get('FRANJA_PRECIO', 'SIN CAT').fillna('SIN CAT').astype(str).str.upper()
        df_ma['Descripcion'] = df_ma.get('DESCRIPCION', '').fillna('').astype(str).str.upper()

    # --- 3. LIMPIEZA UNIFICADA ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        # Estandarizar SKU
        if 'SKU' in df.columns:
            df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Cantidades
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        # Fechas
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Identificación de CLIENTE (Específico para Sell In y Sell Out)
        if 'CLIENTE' in df.columns:
            df['Emprendimiento'] = df['CLIENTE'].fillna('DESCONOCIDO').astype(str).str.strip().str.upper()
        elif 'EMPRENDIMIENTO' in df.columns:
            df['Emprendimiento'] = df['EMPRENDIMIENTO'].fillna('DESCONOCIDO').astype(str).str.strip().str.upper()
        else:
            df['Emprendimiento'] = 'WHOLESALE'
            
        return df

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 4. FILTROS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros de Control")
    f_search = st.sidebar.text_input("🎯 Busca SKU / Descripcion").upper()
    
    meses_dis = sorted(list(set(so_raw['Mes'].dropna())), reverse=True) if not so_raw.empty else []
    f_mes = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_dis)
    
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    st.sidebar.divider()
    st.sidebar.subheader("👥 Selección de Clientes")
    
    # Filtro dinámico basado en columna CLIENTE de Sell Out
    clientes_so_list = sorted(so_raw['Emprendimiento'].unique()) if not so_raw.empty else []
    f_so_clientes = st.sidebar.multiselect("📉 Clientes Sell Out (Col. E)", clientes_so_list, default=clientes_so_list)
    
    # Filtro dinámico basado en columna CLIENTE de Sell In
    clientes_si_list = sorted(si_raw['Emprendimiento'].unique()) if not si_raw.empty else []
    f_si_clientes = st.sidebar.multiselect("📈 Clientes Sell In", clientes_si_list, default=clientes_si_list)

    def apply_filters(df, type_df=None, filter_month=True):
        if df is None or df.empty: return df
        
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')
        
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_search: 
            temp = temp[temp['SKU'].str.contains(f_search, na=False) | 
                        temp['Descripcion'].str.contains(f_search, na=False)]
        
        if filter_month and f_mes != "Todos": 
            temp = temp[temp['Mes'] == f_mes]
        
        # Aplicación de filtros por cliente según el archivo
        if type_df == 'SO' and f_so_clientes:
            temp = temp[temp['Emprendimiento'].isin(f_so_clientes)]
        elif type_df == 'SI' and f_si_clientes:
            temp = temp[temp['Emprendimiento'].isin(f_si_clientes)]
            
        return temp

    so_f = apply_filters(so_raw, type_df='SO')
    si_f = apply_filters(si_raw, type_df='SI')
    stk_f = apply_filters(stk_raw)

    # --- 5. LÓGICA DE SEGMENTACIÓN ---
    max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

    # Función para filtrar sectores en los gráficos
    def get_sector(df, keywords):
        if df is None or df.empty: return pd.DataFrame()
        mask = df['Emprendimiento'].str.contains('|'.join(keywords), na=False)
        return df[mask]

    # --- 6. INTERFAZ Y KPIs ---
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out Total", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Stock Dass", f"{get_sector(stk_snap, ['DASS', 'CENTRAL'])['Cant'].sum():,.0f}")
    k3.metric("Stock Clientes", f"{get_sector(stk_snap, ['WHOLESALE', 'CLIENTE'])['Cant'].sum():,.0f}")
    k4.metric("Stock Retail", f"{get_sector(stk_snap, ['RETAIL', 'TIENDA'])['Cant'].sum():,.0f}")
    k5.metric("Stock E-com", f"{get_sector(stk_snap, ['E-COM', 'DIGITAL'])['Cant'].sum():,.0f}")

    # --- BLOQUE 1: DISCIPLINAS ---
    st.subheader("📌 Análisis por Disciplina")
    
    def safe_pie(df, title):
        if df is not None and not df.empty and df['Cant'].sum() > 0:
            return px.pie(df.groupby('Disciplina')['Cant'].sum().reset_index(), 
                          values='Cant', names='Disciplina', title=title, 
                          color_discrete_map=COLOR_MAP_DIS)
        return None

    row1 = st.columns(4)
    figs_r1 = [
        (get_sector(stk_snap, ['DASS', 'CENTRAL']), "Stock Dass"),
        (get_sector(so_f, ['WHOLESALE', 'CLIENTE']), "Sell Out Wholesale"),
        (get_sector(so_f, ['RETAIL', 'TIENDA']), "Sell Out Retail"),
        (get_sector(so_f, ['E-COM', 'DIGITAL']), "Sell Out E-com")
    ]

    for i, (df_sector, title) in enumerate(figs_r1):
        fig = safe_pie(df_sector, title)
        if fig: row1[i].plotly_chart(fig, use_container_width=True)
        else: row1[i].info(f"{title}: Sin datos")

    row2 = st.columns(4)
    figs_r2 = [
        (get_sector(stk_snap, ['WHOLESALE', 'CLIENTE']), "Stock Clientes"),
        (get_sector(stk_snap, ['RETAIL', 'TIENDA']), "Stock Retail"),
        (get_sector(stk_snap, ['E-COM', 'DIGITAL']), "Stock E-com")
    ]

    for i, (df_sector, title) in enumerate(figs_r2):
        fig = safe_pie(df_sector, title)
        if fig: row2[i].plotly_chart(fig, use_container_width=True)
        else: row2[i].info(f"{title}: Sin datos")
    
    if not si_f.empty:
        fig_si = px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), 
                        x='Mes', y='Cant', color='Disciplina', 
                        title="Sell In por Mes", color_discrete_map=COLOR_MAP_DIS)
        row2[3].plotly_chart(fig_si, use_container_width=True)

    # --- 7. LÍNEA DE TIEMPO ---
    st.divider()
    st.subheader("📈 Evolución: Sell Out vs Stock Clientes")
    so_evol = get_sector(apply_filters(so_raw, type_df='SO', filter_month=False), ['WHOLESALE', 'CLIENTE']).groupby('Mes')['Cant'].sum().reset_index()
    stk_evol = get_sector(apply_filters(stk_raw, filter_month=False), ['WHOLESALE', 'CLIENTE']).groupby('Mes')['Cant'].sum().reset_index()
    
    fig_evol = go.Figure()
    fig_evol.add_trace(go.Scatter(x=so_evol['Mes'], y=so_evol['Cant'], name='SELL OUT CLIENTES', line=dict(color='#FF3131', width=4)))
    fig_evol.add_trace(go.Scatter(x=stk_evol['Mes'], y=stk_evol['Cant'], name='STOCK CLIENTES', line=dict(color='#0055A4', width=4, dash='dash')))
    fig_evol.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_evol, use_container_width=True)

    # --- 8. TABLA DE DETALLE ---
    st.subheader("📋 Tabla de Información Detallada")
    if not so_f.empty or not stk_f.empty:
        # Agrupación por Cliente (Emprendimiento) para la tabla final
        df_so_t = so_f.groupby(['Mes', 'SKU', 'Emprendimiento'])['Cant'].sum().unstack(fill_value=0).add_prefix('Venta ').reset_index()
        df_stk_t = stk_f.groupby(['Mes', 'SKU', 'Emprendimiento'])['Cant'].sum().unstack(fill_value=0).add_prefix('Stock ').reset_index()
        df_final = df_so_t.merge(df_stk_t, on=['Mes', 'SKU'], how='outer').fillna(0)
        df_final = df_final.merge(df_ma[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left')
        st.dataframe(df_final, use_container_width=True)
