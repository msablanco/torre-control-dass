import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.27", layout="wide")

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

    # --- 3. LIMPIEZA ROBUSTA ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        # Buscar SKU
        c_sku = next((c for c in df.columns if 'SKU' in c), 'SKU')
        df['SKU'] = df[c_sku].astype(str).str.strip().str.upper()
        
        # Buscar Cantidad
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT', 'SUMA'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        # Buscar Fecha
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # BUSCADOR DE EMPRENDIMIENTO / CANAL / GRUPO
        c_emp = next((c for c in df.columns if any(x in c for x in ['EMPRENDIMIENTO', 'GRUPO', 'CANAL', 'CLIENTE'])), None)
        if c_emp:
            df['Emprendimiento'] = df[c_emp].fillna('OTROS').astype(str).str.upper().str.strip()
        else:
            df['Emprendimiento'] = 'WHOLESALE'
            
        return df

    so_f = clean_df('Sell_out')
    si_f = clean_df('Sell_in')
    stk_f = clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_f['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    
    # Filtro de Emprendimiento (Detecta Wholesale, Dass Central, etc.)
    lista_emp = sorted(stk_f['Emprendimiento'].unique()) if not stk_f.empty else []
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", lista_emp)

    def apply_filters(df, filter_month=True):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if f_emp and 'Emprendimiento' in temp.columns: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        return temp

    so_filt = apply_filters(so_f)
    stk_filt = apply_filters(stk_f)

    # --- 5. INTERFAZ ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Inteligencia"])

    with tab_control:
        max_date = stk_filt['Fecha_dt'].max() if not stk_filt.empty else None
        stk_snap = stk_filt[stk_filt['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

        # SEPARACIÓN CLAVE
        df_dass = stk_snap[stk_snap['Emprendimiento'].str.contains('DASS|CENTRAL', na=False)]
        df_whole = stk_snap[stk_snap['Emprendimiento'].str.contains('WHOLESALE|MAYORISTA|CLIENTE', na=False)]

        k1, k2, k3 = st.columns(3)
        k1.metric("Sell Out (Venta)", f"{so_filt['Cant'].sum():,.0f}")
        k2.metric("Stock Dass Central", f"{df_dass['Cant'].sum():,.0f}")
        k3.metric("Stock Wholesale", f"{df_whole['Cant'].sum():,.0f}")

        # Gráficos
        st.subheader("📌 Análisis por Disciplina")
        c1, c2, c3 = st.columns(3)
        c1.plotly_chart(px.pie(df_dass.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Central", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c2.plotly_chart(px.pie(so_filt.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Venta Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        
        # El gráfico de Wholesale ahora usa una búsqueda parcial por si se llama "Wholesale " o "Wholesale-Asics"
        if not df_whole.empty:
            c3.plotly_chart(px.pie(df_whole.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Wholesale", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        else:
            c3.error("Dato 'WHOLESALE' no detectado en columna Emprendimiento/Canal")

        # Línea de Tiempo
        st.divider()
        st.subheader("📈 Evolución de Stocks y Ventas")
        stk_h = apply_filters(stk_f, False).groupby(['Mes', 'Emprendimiento'])['Cant'].sum().reset_index()
        so_h = apply_filters(so_f, False).groupby('Mes')['Cant'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='Venta Sell Out', line=dict(color='#0055A4', width=4)))
        
        for emp in stk_h['Emprendimiento'].unique():
            df_e = stk_h[stk_h['Emprendimiento'] == emp]
            fig.add_trace(go.Scatter(x=df_e['Mes'], y=df_e['Cant'], name=f"Stock {emp}"))
        
        st.plotly_chart(fig, use_container_width=True)
