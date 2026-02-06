import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.24", layout="wide")

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
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. MAESTRO (Mapeo de Atributos) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        col_f = next((c for c in df_ma.columns if 'FRANJA' in c.upper() or 'PRECIO' in c.upper()), 'FRANJA_PRECIO')
        df_ma['Disciplina'] = df_ma.get('Disciplina', 'OTRO').fillna('OTRO').astype(str).str.upper().str.strip()
        df_ma['FRANJA_PRECIO'] = df_ma.get(col_f, 'SIN CAT').fillna('SIN CAT').astype(str).str.upper().str.strip()
        df_ma['Descripcion'] = df_ma.get('Descripcion', 'SIN DESCRIPCIÓN').fillna('SIN DESCRIPCIÓN').astype(str).str.upper()
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. LIMPIEZA DE TRANSACCIONES ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        c_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNID', 'CANT'])), 'Cant')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        c_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'MES'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Columna Emprendimiento (Crucial para Stock)
        if 'EMPRENDIMIENTO' in df.columns:
            df['Emprendimiento'] = df['EMPRENDIMIENTO'].fillna('OTROS').astype(str).str.upper().str.strip()
        else:
            df['Emprendimiento'] = 'CLIENTES' # Default si no existe la columna todavía
            
        return df

    so_f = clean_df('Sell_out')
    si_f = clean_df('Sell_in')
    stk_f = clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_f['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    # Filtro dinámico de Emprendimiento (Para ver solo Clientes, Retail, etc.)
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", sorted(stk_f['Emprendimiento'].unique()) if not stk_f.empty else [])

    def apply_filters(df, is_stk=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion', 'Busqueda']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if f_periodo != "Todos" and 'Mes' in temp.columns: temp = temp[temp['Mes'] == f_periodo]
        if f_emp and 'Emprendimiento' in temp.columns: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        return temp

    so_filt = apply_filters(so_f)
    si_filt = apply_filters(si_f)
    stk_filt = apply_filters(stk_f)

    # --- 5. VISUALIZACIÓN ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Abastecimiento"])

    with tab_control:
        # Foto Stock
        max_date = stk_filt['Fecha_dt'].max() if not stk_filt.empty else None
        stk_snap = stk_filt[stk_filt['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

        # Separación por Emprendimiento
        df_central = stk_snap[stk_snap['Emprendimiento'] == 'DASS CENTRAL']
        df_clientes = stk_snap[stk_snap['Emprendimiento'] == 'CLIENTES']
        df_retail = stk_snap[stk_snap['Emprendimiento'].isin(['RETAIL', 'E-COM'])]

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Sell Out", f"{so_filt['Cant'].sum():,.0f}")
        k2.metric("Stock Central", f"{df_central['Cant'].sum():,.0f}")
        k3.metric("Stock Clientes", f"{df_clientes['Cant'].sum():,.0f}")
        k4.metric("Stock Propio (DTC)", f"{df_retail['Cant'].sum():,.0f}")

        # --- FILA 1: DISCIPLINAS ---
        st.subheader("📌 Stock y Venta por Disciplina")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        c1.plotly_chart(px.pie(df_central.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Dass Central", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c2.plotly_chart(px.pie(so_filt.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c3.plotly_chart(px.pie(df_clientes.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Clientes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c4.plotly_chart(px.bar(si_filt.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In Mensual", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # --- FILA 2: FRANJAS ---
        st.subheader("💰 Stock y Venta por Franja")
        f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
        f1.plotly_chart(px.pie(df_central.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Dass Central"), use_container_width=True)
        f2.plotly_chart(px.pie(so_filt.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out"), use_container_width=True)
        f3.plotly_chart(px.pie(df_clientes.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Clientes"), use_container_width=True)
        f4.plotly_chart(px.bar(si_filt.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In por Franja"), use_container_width=True)

        # --- LÍNEA DE TIEMPO ---
        st.divider()
        st.subheader("📈 Evolución de Stocks y Salida")
        # Agrupar histórico por mes y emprendimiento
        stk_h = stk_filt.groupby(['Mes', 'Emprendimiento'])['Cant'].sum().reset_index()
        so_h = so_filt.groupby('Mes')['Cant'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='Venta (Sell Out)', line=dict(color='#0055A4', width=4)))
        for emp in stk_h['Emprendimiento'].unique():
            df_e = stk_h[stk_h['Emprendimiento'] == emp]
            fig.add_trace(go.Scatter(x=df_e['Mes'], y=df_e['Cant'], name=f"Stock {emp}"))
        st.plotly_chart(fig, use_container_width=True)

    with tab_intel:
        st.header("🎯 Sugerencia de Reposición para Clientes")
        # Aquí filtramos la lógica solo para abastecer el canal Clientes
        st.info("Cálculo basado en reponer el stock necesario para cubrir la venta máxima de Clientes.")
        # (Lógica de tabla de abastecimiento...)
