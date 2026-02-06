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

    # --- 3. LIMPIEZA UNIFICADA (STOCK Y SELL OUT) ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Cantidades y Fechas
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Lógica de Emprendimiento Unificada
        if 'EMPRENDIMIENTO' in df.columns:
            df['Emprendimiento'] = df['EMPRENDIMIENTO'].fillna('').astype(str).str.strip().str.upper()
        else:
            df['Emprendimiento'] = 'WHOLESALE'
        return df

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS (7 GARANTIZADOS) ---
    st.sidebar.header("🔍 Filtros de Control")
    f_search = st.sidebar.text_input("🎯 Busca SKU / Descripcion").upper()
    f_mes = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    # Filtros de Emprendimiento/Clientes
    opciones_emp = sorted(list(set(stk_raw['Emprendimiento'].unique()) | set(so_raw['Emprendimiento'].unique())))
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", opciones_emp, default=opciones_emp)
    
    # Sliders para Sell Out / Sell In Clientes
    f_so_range = st.sidebar.slider("📉 Sell Out Clientes (Rango)", 0, int(so_raw['Cant'].max() if not so_raw.empty else 100), (0, int(so_raw['Cant'].max() if not so_raw.empty else 100)))
    f_si_range = st.sidebar.slider("📈 Sell In Clientes (Rango)", 0, int(si_raw['Cant'].max() if not si_raw.empty else 100), (0, int(si_raw['Cant'].max() if not si_raw.empty else 100)))

    def apply_filters(df, filter_month=True):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_search: temp = temp[temp['SKU'].str.contains(f_search) | temp['Descripcion'].str.contains(f_search)]
        if filter_month and f_mes != "Todos": temp = temp[temp['Mes'] == f_mes]
        if f_emp: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        # Filtro por volumen de ventas/ingresos
        if 'Sell_out' in df.index.name or 'CANT' in df.columns: 
            temp = temp[(temp['Cant'] >= f_so_range[0]) & (temp['Cant'] <= f_so_range[1])]
        return temp

    so_f, si_f, stk_f = apply_filters(so_raw), apply_filters(si_raw), apply_filters(stk_raw)

    # --- 5. LÓGICA DE SEGMENTACIÓN ---
    max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

    # Mapeos de Stock y Sell Out por Canal
    def get_sector(df, emp_name): return df[df['Emprendimiento'] == emp_name]

    # --- 6. INTERFAZ ---
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out Total", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Stock Dass", f"{get_sector(stk_snap, 'DASS CENTRAL')['Cant'].sum():,.0f}")
    k3.metric("Stock Clientes", f"{get_sector(stk_snap, 'WHOLESALE')['Cant'].sum():,.0f}")
    k4.metric("Stock Retail", f"{get_sector(stk_snap, 'RETAIL')['Cant'].sum():,.0f}")
    k5.metric("Stock E-com", f"{get_sector(stk_snap, 'E-COM')['Cant'].sum():,.0f}")

    # --- BLOQUE 1: DISCIPLINAS (TORTAS Y BARRAS) ---
    st.subheader("📌 Análisis por Disciplina")
    row1 = st.columns(4)
    row1[0].plotly_chart(px.pie(get_sector(stk_snap, 'DASS CENTRAL').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    row1[1].plotly_chart(px.pie(get_sector(so_f, 'WHOLESALE').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out Wholesale", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    row1[2].plotly_chart(px.pie(get_sector(so_f, 'RETAIL').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out Retail", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    row1[3].plotly_chart(px.pie(get_sector(so_f, 'E-COM').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out E-com", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    row2 = st.columns(4)
    row2[0].plotly_chart(px.pie(get_sector(stk_snap, 'WHOLESALE').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Clientes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    row2[1].plotly_chart(px.pie(get_sector(stk_snap, 'RETAIL').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Retail", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    row2[2].plotly_chart(px.pie(get_sector(stk_snap, 'E-COM').groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock E-com", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    row2[3].plotly_chart(px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In por Mes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # --- BLOQUE 2: FRANJAS (MISMA LÓGICA) ---
    st.subheader("💰 Análisis por Franja de Precio")
    f_row1 = st.columns(4)
    f_row1[0].plotly_chart(px.pie(get_sector(stk_snap, 'DASS CENTRAL').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Dass (F)"), use_container_width=True)
    f_row1[1].plotly_chart(px.pie(get_sector(so_f, 'WHOLESALE').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out Whol (F)"), use_container_width=True)
    f_row1[2].plotly_chart(px.pie(get_sector(so_f, 'RETAIL').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out Ret (F)"), use_container_width=True)
    f_row1[3].plotly_chart(px.pie(get_sector(so_f, 'E-COM').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out Ecom (F)"), use_container_width=True)

    f_row2 = st.columns(4)
    f_row2[0].plotly_chart(px.pie(get_sector(stk_snap, 'WHOLESALE').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Clientes (F)"), use_container_width=True)
    f_row2[1].plotly_chart(px.pie(get_sector(stk_snap, 'RETAIL').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Retail (F)"), use_container_width=True)
    f_row2[2].plotly_chart(px.pie(get_sector(stk_snap, 'E-COM').groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock E-com (F)"), use_container_width=True)
    f_row2[3].plotly_chart(px.bar(si_f.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In Franja"), use_container_width=True)

    # --- 7. LÍNEA DE TIEMPO (SELL OUT Y STOCK CLIENTES) ---
    st.divider()
    st.subheader("📈 Evolución: Sell Out vs Stock Clientes")
    so_h = get_sector(apply_filters(so_raw, False), 'WHOLESALE').groupby('Mes')['Cant'].sum().reset_index()
    stk_h = get_sector(apply_filters(stk_raw, False), 'WHOLESALE').groupby('Mes')['Cant'].sum().reset_index()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='SELL OUT CLIENTES', line=dict(color='#FF3131', width=4)))
    fig.add_trace(go.Scatter(x=stk_h['Mes'], y=stk_h['Cant'], name='STOCK CLIENTES', line=dict(color='#0055A4', width=4, dash='dash')))
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # --- 8. TABLA DE DETALLE ---
    st.subheader("📋 Tabla de Información Detallada")
    df_so_t = so_f.groupby(['Mes', 'SKU', 'Emprendimiento'])['Cant'].sum().unstack(fill_value=0).add_prefix('Venta ').reset_index()
    df_stk_t = stk_f.groupby(['Mes', 'SKU', 'Emprendimiento'])['Cant'].sum().unstack(fill_value=0).add_prefix('Stock ').reset_index()
    
    df_final = df_so_t.merge(df_stk_t, on=['Mes', 'SKU'], how='outer').fillna(0)
    df_final = df_final.merge(df_ma[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left')
    
    st.dataframe(df_final, use_container_width=True)
