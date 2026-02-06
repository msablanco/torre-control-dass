import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.29", layout="wide")

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
        df_ma['Descripcion'] = df_ma.get('DESCRIPCION', '').fillna('').astype(str).str.upper()
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. LIMPIEZA CON REGLAS DE NEGOCIO ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Normalización de Emprendimiento
        if 'EMPRENDIMIENTO' in df.columns:
            df['Emprendimiento'] = df['EMPRENDIMIENTO'].fillna('OTROS').astype(str).str.strip().str.upper()
        else:
            df['Emprendimiento'] = 'OTROS'
        return df

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros Dashboard")
    search_query = st.sidebar.text_input("🎯 Buscar SKU / Desc.").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    # Filtro Multiselección de Emprendimiento
    opciones_emp = sorted(stk_raw['Emprendimiento'].unique()) if not stk_raw.empty else []
    f_emp = st.sidebar.multiselect("🏢 Seleccionar Emprendimiento", opciones_emp)

    def apply_filters(df, filter_month=True):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Busqueda']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if search_query: temp = temp[temp['Busqueda'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if f_emp and 'Emprendimiento' in temp.columns: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        return temp

    so_f, si_f, stk_f = apply_filters(so_raw), apply_filters(si_raw), apply_filters(stk_raw)

    # --- 5. VISUALIZACIÓN ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Inteligencia"])

    with tab_control:
        # Snapshot Stock Actual
        max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
        stk_snap = stk_f[stk_f['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()
        
        # KPIs según tus Reglas
        df_dass = stk_snap[stk_snap['Emprendimiento'] == 'DASS CENTRAL']
        df_whole = stk_snap[stk_snap['Emprendimiento'] == 'WHOLESALE']
        df_retail = stk_snap[stk_snap['Emprendimiento'] == 'RETAIL']
        df_ecom = stk_snap[stk_snap['Emprendimiento'] == 'E-COM']

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Sell Out", f"{so_f['Cant'].sum():,.0f}")
        k2.metric("Stock Dass", f"{df_dass['Cant'].sum():,.0f}")
        k3.metric("Stock Clientes", f"{df_whole['Cant'].sum():,.0f}")
        k4.metric("Retail Tiendas", f"{df_retail['Cant'].sum():,.0f}")
        k5.metric("E-com Tienda", f"{df_ecom['Cant'].sum():,.0f}")

        # --- FILA GRÁFICOS: DISCIPLINAS ---
        st.subheader("📌 Distribución de Stock por Disciplina")
        g1, g2, g3, g4 = st.columns(4)
        
        # Stock Dass
        g1.plotly_chart(px.pie(df_dass.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        # Sell Out (Venta)
        g2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Venta Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        # Stock Clientes (Wholesale)
        g3.plotly_chart(px.pie(df_whole.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Clientes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        # Sell In (Mensual)
        g4.plotly_chart(px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In por Mes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # --- LÍNEA DE TIEMPO (MAESTRA) ---
        st.divider()
        st.subheader("📈 Evolución del Stock por Canal")
        
        stk_h = apply_filters(stk_raw, False).groupby(['Mes', 'Emprendimiento'])['Cant'].sum().reset_index()
        so_h = apply_filters(so_raw, False).groupby('Mes')['Cant'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='VENTA TOTAL', line=dict(color='#0055A4', width=4)))
        
        # Diccionario de Nombres para el Gráfico
        map_nombres = {
            'DASS CENTRAL': 'Stock Dass',
            'WHOLESALE': 'Stock Clientes',
            'RETAIL': 'Stock Retail Tiendas',
            'E-COM': 'Stock E-com Tienda'
        }

        for emp in stk_h['Emprendimiento'].unique():
            df_e = stk_h[stk_h['Emprendimiento'] == emp]
            nombre_grafico = map_nombres.get(emp, f"Stock {emp}")
            fig.add_trace(go.Scatter(x=df_e['Mes'], y=df_e['Cant'], name=nombre_grafico, mode='lines+markers'))
            
        fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)
