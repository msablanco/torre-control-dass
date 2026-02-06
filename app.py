import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.35", layout="wide")

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

    # --- 3. LIMPIEZA ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Emprendimiento'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Mapeo según tus reglas
        if 'EMPRENDIMIENTO' in df.columns:
            df['Emprendimiento'] = df['EMPRENDIMIENTO'].fillna('WHOLESALE').astype(str).str.upper().str.strip()
        else:
            df['Emprendimiento'] = 'WHOLESALE'
        return df

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    search_query = st.sidebar.text_input("🎯 Buscar SKU / Desc.").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))
    opciones_emp = sorted(stk_raw['Emprendimiento'].unique()) if not stk_raw.empty else []
    f_emp = st.sidebar.multiselect("🏢 Filtrar Emprendimiento (Gráfico Líneas)", opciones_emp)

    def apply_filters(df, filter_month=True, ignore_emp=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Busqueda']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if search_query: temp = temp[temp['Busqueda'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if not ignore_emp and f_emp and 'Emprendimiento' in temp.columns:
            temp = temp[temp['Emprendimiento'].isin(f_emp)]
        return temp

    so_f = apply_filters(so_raw)
    si_f = apply_filters(si_raw)
    stk_f_fixed = apply_filters(stk_raw, ignore_emp=True) 

    # --- 5. INTERFAZ ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Inteligencia"])

    with tab_control:
        max_date = stk_f_fixed['Fecha_dt'].max() if not stk_f_fixed.empty else None
        stk_snap = stk_f_fixed[stk_f_fixed['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()
        
        # Segmentación Estricta
        df_dass = stk_snap[stk_snap['Emprendimiento'].str.contains('DASS CENTRAL', na=False)]
        df_whole = stk_snap[stk_snap['Emprendimiento'].str.contains('WHOLESALE', na=False)]
        df_retail = stk_snap[stk_snap['Emprendimiento'].str.contains('RETAIL', na=False)]
        df_ecom = stk_snap[stk_snap['Emprendimiento'].str.contains('E-COM', na=False)]

        # KPIs superiores
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Venta Sell Out", f"{so_f['Cant'].sum():,.0f}")
        k2.metric("Sell In Total", f"{si_f['Cant'].sum():,.0f}")
        k3.metric("Stk Dass", f"{df_dass['Cant'].sum():,.0f}")
        k4.metric("Stk Clientes", f"{df_whole['Cant'].sum():,.0f}")
        k5.metric("Retail", f"{df_retail['Cant'].sum():,.0f}")
        k6.metric("E-com", f"{df_ecom['Cant'].sum():,.0f}")

        # --- SECCIÓN DISCIPLINAS ---
        st.subheader("📌 Análisis por Disciplina")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 1.5])
        c1.plotly_chart(px.pie(df_dass.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c3.plotly_chart(px.pie(df_whole.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Clientes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c4.plotly_chart(px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Evolución Sell In", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # --- SECCIÓN FRANJAS ---
        st.subheader("💰 Análisis por Franja")
        f1, f2, f3, f4 = st.columns([1, 1, 1, 1.5])
        f1.plotly_chart(px.pie(df_dass.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Dass (F)"), use_container_width=True)
        f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out (F)"), use_container_width=True)
        f3.plotly_chart(px.pie(df_whole.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Clientes (F)"), use_container_width=True)
        f4.plotly_chart(px.bar(si_f.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In Franja"), use_container_width=True)

        # --- LÍNEA DE TIEMPO ---
        st.divider()
        st.subheader("📈 Evolución Histórica Operativa")
        stk_h_data = apply_filters(stk_raw, filter_month=False, ignore_emp=False)
        stk_h = stk_h_data.groupby(['Mes', 'Emprendimiento'])['Cant'].sum().reset_index()
        so_h = apply_filters(so_raw, filter_month=False).groupby('Mes')['Cant'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='VENTA TOTAL', line=dict(color='#0055A4', width=4)))
        map_nombres = {'DASS CENTRAL': 'Stock Dass', 'WHOLESALE': 'Stock Clientes', 'RETAIL': 'Stock Retail Tiendas', 'E-COM': 'Stock E-com Tienda'}
        for emp in sorted(stk_h['Emprendimiento'].unique()):
            df_e = stk_h[stk_h['Emprendimiento'] == emp]
            fig.add_trace(go.Scatter(x=df_e['Mes'], y=df_e['Cant'], name=map_nombres.get(emp, f"Stock {emp}"), mode='lines+markers'))
        st.plotly_chart(fig, use_container_width=True)

    with tab_intel:
        st.header("🚨 Inteligencia de SKUs")
        df_ranking = so_f.groupby(['SKU', 'Disciplina', 'FRANJA_PRECIO'])['Cant'].sum().reset_index().sort_values('Cant', ascending=False)
        st.dataframe(df_ranking, use_container_width=True)
