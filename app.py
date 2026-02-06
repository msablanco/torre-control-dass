import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.5", layout="wide")

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
    # --- 2. MAESTRO (PROCESAMIENTO SEGURO) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        
        # Blindaje contra columnas inexistentes
        columnas_esperadas = {
            'Disciplina': 'OTRO',
            'FRANJA_PRECIO': 'SIN CAT',
            'Descripcion': 'SIN DESCRIPCION'
        }
        for col, default in columnas_esperadas.items():
            if col not in df_ma.columns:
                df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. TRANSACCIONES ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        # Detección flexible de columna cantidad
        col_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'Cant')
        df['Cant'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        # Detección flexible de fecha
        col_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        df['Cliente_up'] = df.get('Cliente', '').fillna('').astype(str).str.upper()
        return df[['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up']]

    so_all = clean_df('Sell_out')
    si_all = clean_df('Sell_in')
    stk_all = clean_df('Stock')

    # --- 4. SIDEBAR & FILTROS ATÓMICOS ---
    st.sidebar.header("🔍 Filtros de Tablero")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Nombre").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + sorted(list(set(so_all['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))

    # Filtrar SKUs válidos primero para velocidad
    m_filt = df_ma.copy()
    if f_dis: m_filt = m_filt[m_filt['Disciplina'].isin(f_dis)]
    if f_fra: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_fra)]
    if search_query: m_filt = m_filt[m_filt['Busqueda'].str.contains(search_query, na=False)]
    skus_ok = set(m_filt['SKU'])

    def apply_final_filters(df, filter_month=True):
        if df.empty: return df
        temp = df[df['SKU'].isin(skus_ok)]
        if filter_month and f_periodo != "Todos":
            temp = temp[temp['Mes'] == f_periodo]
        return temp.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')

    so_f = apply_final_filters(so_all)
    si_f = apply_final_filters(si_all)
    stk_f = apply_final_filters(stk_all)

    # --- 5. DASHBOARD ---
    st.title("📊 Torre de Control Dass v11.5")
    
    # Snapshot de Stock (Última fecha disponible filtrada)
    max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_date] if max_date else pd.DataFrame()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['Cant'].sum():,.0f}")
    k3.metric("Stock Dass", f"{stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)]['Cant'].sum():,.0f}")
    k4.metric("Stock Cliente", f"{stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)]['Cant'].sum():,.0f}")

    # --- 6. GRÁFICOS DE ANÁLISIS ---
    def render_row(title, so, si, stk, g_col):
        st.subheader(title)
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        if not stk.empty:
            c1.plotly_chart(px.pie(stk[stk['Cliente_up'].str.contains('DASS', na=False)].groupby(g_col)['Cant'].sum().reset_index(), values='Cant', names=g_col, title="Stock Dass", color=g_col, color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
            c3.plotly_chart(px.pie(stk[~stk['Cliente_up'].str.contains('DASS', na=False)].groupby(g_col)['Cant'].sum().reset_index(), values='Cant', names=g_col, title="Stock Cliente", color=g_col, color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        if not so.empty:
            c2.plotly_chart(px.pie(so.groupby(g_col)['Cant'].sum().reset_index(), values='Cant', names=g_col, title="Sell Out", color=g_col, color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        if not si.empty:
            c4.plotly_chart(px.bar(si.groupby(['Mes', g_col])['Cant'].sum().reset_index(), x='Mes', y='Cant', color=g_col, title="Sell In Evolución", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    render_row("📌 Análisis por Disciplina", so_f, si_f, stk_snap, 'Disciplina')

    # --- 7. LÍNEA DE TIEMPO 4D (MULTIVARIABLE) ---
    st.divider()
    st.subheader("📈 Evolución Histórica: Ventas, Ingresos y Stocks")
    
    # Datos históricos ignorando el filtro de mes pero respetando los filtros de producto/disciplina
    so_h = apply_final_filters(so_all, False).groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out'})
    si_h = apply_final_filters(si_all, False).groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In'})
    stk_h_raw = apply_final_filters(stk_all, False)
    
    sd_h = stk_h_raw[stk_h_raw['Cliente_up'].str.contains('DASS', na=False)].groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
    sc_h = stk_h_raw[~stk_h_raw['Cliente_up'].str.contains('DASS', na=False)].groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Cliente'})

    # Unión de datos para el gráfico
    df_h = so_h.merge(si_h, on='Mes', how='outer').merge(sd_h, on='Mes', how='outer').merge(sc_h, on='Mes', how='outer').fillna(0).sort_values('Mes')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Sell Out'], name='Sell Out (Ventas)', line=dict(color='#0055A4', width=4)))
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Sell In'], name='Sell In (Ingresos)', line=dict(color='#FF3131', width=3, dash='dot')))
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Stock Dass'], name='Stock en Dass', line=dict(color='#00A693', width=2)))
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Stock Cliente'], name='Stock en Cliente', line=dict(color='#FFD700', width=2)))
    
    fig.update_layout(height=500, template="plotly_white", hovermode="x unified", legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"))
    st.plotly_chart(fig, use_container_width=True)

    # --- 8. TABLA DE DETALLE ---
    st.divider()
    st.subheader("📋 Detalle de Productos")
    t_final = so_f.groupby(['SKU', 'Descripcion', 'Disciplina']).agg({'Cant': 'sum'}).reset_index().sort_values('Cant', ascending=False)
    st.dataframe(t_final, use_container_width=True, hide_index=True)
