import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.34", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL ---
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
    # --- 2. MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        col_f = next((c for c in df_ma.columns if 'FRANJA' in c or 'PRECIO' in c), 'FRANJA_PRECIO')
        df_ma['Disciplina'] = df_ma.get('DISCIPLINA', 'OTRO').fillna('OTRO').astype(str).str.upper()
        df_ma['FRANJA_PRECIO'] = df_ma.get(col_f, 'SIN CAT').fillna('SIN CAT').astype(str).str.upper()
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma.get('DESCRIPCION', '').astype(str).str.upper()

    # --- 3. LIMPIEZA CON DETECCIÓN FLEXIBLE ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Normalizamos la columna para que la búsqueda sea fácil
        c_emp = next((c for c in df.columns if 'EMPRENDIMIENTO' in c), None)
        if c_emp:
            df['Emprendimiento_Raw'] = df[c_emp].fillna('').astype(str).str.upper()
        else:
            df['Emprendimiento_Raw'] = 'WHOLESALE' # Fallback
            
        return df

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    # Filtro de emprendimiento basado en lo que hay en el CSV
    opciones_emp = sorted(stk_raw['Emprendimiento_Raw'].unique()) if not stk_raw.empty else []
    f_emp = st.sidebar.multiselect("🏢 Filtrar Emprendimiento", opciones_emp)

    def apply_filters(df, filter_month=True, ignore_emp=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Busqueda']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if not ignore_emp and f_emp: temp = temp[temp['Emprendimiento_Raw'].isin(f_emp)]
        return temp

    so_f = apply_filters(so_raw)
    stk_f_base = apply_filters(stk_raw, ignore_emp=True) 

    # --- 5. LÓGICA DE SEGMENTACIÓN ---
    # Usamos .str.contains para que sea infalible
    max_date = stk_f_base['Fecha_dt'].max() if not stk_f_base.empty else None
    stk_snap = stk_f_base[stk_f_base['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

    df_dass = stk_snap[stk_snap['Emprendimiento_Raw'].str.contains('DASS|CENTRAL', na=False)]
    df_whole = stk_snap[stk_snap['Emprendimiento_Raw'].str.contains('WHOLESALE|CLIENTE', na=False)]
    df_retail = stk_snap[stk_snap['Emprendimiento_Raw'].str.contains('RETAIL', na=False)]
    df_ecom = stk_snap[stk_snap['Emprendimiento_Raw'].str.contains('E-COM|ECOMM', na=False)]

    # --- 6. INTERFAZ ---
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Stock Dass", f"{df_dass['Cant'].sum():,.0f}")
    k3.metric("Stock Clientes", f"{df_whole['Cant'].sum():,.0f}")
    k4.metric("Retail", f"{df_retail['Cant'].sum():,.0f}")
    k5.metric("E-com", f"{df_ecom['Cant'].sum():,.0f}")

    st.divider()
    
    # GRÁFICOS DE DISCIPLINA (4 columnas)
    st.subheader("📌 Distribución por Disciplina")
    col1, col2, col3, col4 = st.columns(4)
    col1.plotly_chart(px.pie(df_dass.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    col2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Venta Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    col3.plotly_chart(px.pie(df_whole.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Clientes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    col4.plotly_chart(px.bar(apply_filters(si_raw).groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In Mes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # GRÁFICOS DE FRANJA (4 columnas)
    st.subheader("💰 Análisis por Franja de Precio")
    fcol1, fcol2, fcol3, fcol4 = st.columns(4)
    fcol1.plotly_chart(px.pie(df_dass.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Dass (F)"), use_container_width=True)
    fcol2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out (F)"), use_container_width=True)
    fcol3.plotly_chart(px.pie(df_whole.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Clientes (F)"), use_container_width=True)
    fcol4.plotly_chart(px.bar(apply_filters(si_raw).groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In Franja"), use_container_width=True)

    # LÍNEA DE TIEMPO
    st.divider()
    st.subheader("📈 Línea de Tiempo de Stocks")
    stk_h_raw = apply_filters(stk_raw, filter_month=False)
    so_h = apply_filters(so_raw, filter_month=False).groupby('Mes')['Cant'].sum().reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='VENTA TOTAL', line=dict(color='#0055A4', width=3)))
    
    # Dibujamos cada emprendimiento que exista en el archivo
    for emp in sorted(stk_h_raw['Emprendimiento_Raw'].unique()):
        df_e = stk_h_raw[stk_h_raw['Emprendimiento_Raw'] == emp].groupby('Mes')['Cant'].sum().reset_index()
        fig.add_trace(go.Scatter(x=df_e['Mes'], y=df_e['Cant'], name=f"Stock {emp}"))
    
    st.plotly_chart(fig, use_container_width=True)
