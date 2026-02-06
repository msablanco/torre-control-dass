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
        
        # Estandarizar SKU y Cantidad
        if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        # Fechas
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Nueva Lógica de Columnas: Emprendimiento y Cliente
        df['Emprendimiento'] = df.get('EMPRENDIMIENTO', 'WHOLESALE').fillna('WHOLESALE').astype(str).str.strip().str.upper()
        df['Cliente'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.strip().str.upper()
            
        return df

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 4. FILTROS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    f_search = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    
    meses_dis = sorted(list(set(so_raw['Mes'].dropna())), reverse=True) if not so_raw.empty else []
    f_mes = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_dis)
    
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    st.sidebar.divider()
    
    # Filtro de Emprendimiento (Unificado de todos los archivos)
    opciones_emp = sorted(list(set(so_raw['Emprendimiento'].unique()) | 
                               set(si_raw['Emprendimiento'].unique()) | 
                               set(stk_raw['Emprendimiento'].unique())))
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", opciones_emp, default=opciones_emp)

    # Filtros de Clientes (Específicos por archivo como pediste)
    clientes_so = sorted(so_raw['Cliente'].unique()) if not so_raw.empty else []
    f_so_cli = st.sidebar.multiselect("📉 Clientes Sell Out", clientes_so, default=clientes_so)
    
    clientes_si = sorted(si_raw['Cliente'].unique()) if not si_raw.empty else []
    f_si_cli = st.sidebar.multiselect("📈 Clientes Sell In", clientes_si, default=clientes_si)

    def apply_filters(df, type_df=None, filter_month=True):
        if df is None or df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')
        
        # Filtros comunes
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_search: temp = temp[temp['SKU'].str.contains(f_search, na=False) | temp['Descripcion'].str.contains(f_search, na=False)]
        if filter_month and f_mes != "Todos": temp = temp[temp['Mes'] == f_mes]
        
        # Filtro de Emprendimiento (Afecta a todos)
        if f_emp: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        
        # Filtros específicos de Cliente
        if type_df == 'SO' and f_so_cli: temp = temp[temp['Cliente'].isin(f_so_cli)]
        if type_df == 'SI' and f_si_cli: temp = temp[temp['Cliente'].isin(f_si_cli)]
            
        return temp

    so_f = apply_filters(so_raw, type_df='SO')
    si_f = apply_filters(si_raw, type_df='SI')
    stk_f = apply_filters(stk_raw)

    # --- 5. LÓGICA DE SEGMENTACIÓN ---
    max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

    def get_sector(df, emp_val):
        if df is None or df.empty: return pd.DataFrame()
        return df[df['Emprendimiento'] == emp_val]

    # --- 6. INTERFAZ Y KPIs ---
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out Total", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Stock Dass", f"{get_sector(stk_snap, 'DASS CENTRAL')['Cant'].sum():,.0f}")
    k3.metric("Stock Clientes", f"{get_sector(stk_snap, 'WHOLESALE')['Cant'].sum():,.0f}")
    k4.metric("Stock Retail", f"{get_sector(stk_snap, 'RETAIL')['Cant'].sum():,.0f}")
    k5.metric("Stock E-com", f"{get_sector(stk_snap, 'E-COM')['Cant'].sum():,.0f}")

    # --- BLOQUE 1: DISCIPLINAS ---
    st.subheader("📌 Análisis por Disciplina")
    
    def safe_pie(df, title):
        if not df.empty and df['Cant'].sum() > 0:
            fig = px.pie(df.groupby('Disciplina')['Cant'].sum().reset_index(), 
                         values='Cant', names='Disciplina', title=title, 
                         color_discrete_map=COLOR_MAP_DIS)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            return fig
        return None

    # Fila 1: Stock Dass y Sell Out por canal
    row1 = st.columns(4)
    sects_r1 = [
        (get_sector(stk_snap, 'DASS CENTRAL'), "Stock Dass"),
        (get_sector(so_f, 'WHOLESALE'), "Sell Out Wholesale"),
        (get_sector(so_f, 'RETAIL'), "Sell Out Retail"),
        (get_sector(so_f, 'E-COM'), "Sell Out E-com")
    ]

    for i, (d, t) in enumerate(sects_r1):
        fig = safe_pie(d, t)
        if fig: row1[i].plotly_chart(fig, use_container_width=True)
        else: row1[i].info(f"{t}: Sin datos")

    # Fila 2: Stock por canal y Sell In
    row2 = st.columns(4)
    sects_r2 = [
        (get_sector(stk_snap, 'WHOLESALE'), "Stock Clientes"),
        (get_sector(stk_snap, 'RETAIL'), "Stock Retail"),
        (get_sector(stk_snap, 'E-COM'), "Stock E-com")
    ]

    for i, (d, t) in enumerate(sects_r2):
        fig = safe_pie(d, t)
        if fig: row2[i].plotly_chart(fig, use_container_width=True)
        else: row2[i].info(f"{t}: Sin datos")
    
    if not si_f.empty:
        fig_si = px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), 
                        x='Mes', y='Cant', color='Disciplina', 
                        title="Sell In por Mes", color_discrete_map=COLOR_MAP_DIS)
        row2[3].plotly_chart(fig_si, use_container_width=True)

    # --- 7. LÍNEA DE TIEMPO ---
    st.divider()
    st.subheader("📈 Evolución: Sell Out vs Stock Clientes (Wholesale)")
    so_evol = get_sector(apply_filters(so_raw, type_df='SO', filter_month=False), 'WHOLESALE').groupby('Mes')['Cant'].sum().reset_index()
    stk_evol = get_sector(apply_filters(stk_raw, filter_month=False), 'WHOLESALE').groupby('Mes')['Cant'].sum().reset_index()
    
    fig_evol = go.Figure()
    fig_evol.add_trace(go.Scatter(x=so_evol['Mes'], y=so_evol['Cant'], name='SELL OUT', line=dict(color='#FF3131', width=4)))
    fig_evol.add_trace(go.Scatter(x=stk_evol['Mes'], y=stk_evol['Cant'], name='STOCK', line=dict(color='#0055A4', width=4, dash='dash')))
    fig_evol.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_evol, use_container_width=True)

    # --- 8. TABLA DE DETALLE ---
    st.subheader("📋 Tabla de Información Detallada")
    if not so_f.empty or not stk_f.empty:
        df_so_t = so_f.groupby(['Mes', 'SKU', 'Emprendimiento', 'Cliente'])['Cant'].sum().unstack(fill_value=0).add_prefix('Venta ').reset_index()
        df_stk_t = stk_f.groupby(['Mes', 'SKU', 'Emprendimiento', 'Cliente'])['Cant'].sum().unstack(fill_value=0).add_prefix('Stock ').reset_index()
        df_final = df_so_t.merge(df_stk_t, on=['Mes', 'SKU', 'Emprendimiento', 'Cliente'], how='outer').fillna(0)
        df_final = df_final.merge(df_ma[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left')
        st.dataframe(df_final, use_container_width=True)
