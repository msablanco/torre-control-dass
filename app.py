import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(
    page_title="Performance & Inteligencia: Fila Calzado",
    page_icon="logo_fila.png",
    layout="wide"
)

# --- 1. CONFIGURACIONES VISUALES Y MAPEO ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3'
}
COLOR_MAP_FRA = {
    'PINNACLE': '#4B0082', 'BEST': '#1E90FF', 'BETTER': '#32CD32', 
    'GOOD': '#FF8C00', 'CORE': '#696969', 'SIN CATEGORIA': '#D3D3D3'
}

@st.cache_data(ttl=600)
def load_all_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        results = service.files().list(q=f"'{folder_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
        found_dfs = {}
        for item in results.get('files', []):
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.upper()
            found_dfs[item['name'].replace('.csv', '')] = df
        return found_dfs
    except: return {}

data = load_all_data()

# --- 2. ENCABEZADO Y SIDEBAR ---
try: st.sidebar.image("logo_fila.png", use_container_width=True)
except: pass

st.sidebar.header("🔍 Filtros de Inteligencia")

col_logo, col_title = st.columns([1, 6])
with col_logo:
    try: st.image("logo_fila.png", width=120)
    except: pass
with col_title:
    st.title("Performance & Inteligencia: Fila Calzado")
    st.markdown("### Torre de Control de Inventario y Ventas")

if data:
    # --- 3. PROCESAMIENTO DE DATOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma['DISCIPLINA'] = df_ma['DISCIPLINA'].fillna('SIN CATEGORIA').str.upper()
        df_ma['FRANJA_PRECIO'] = df_ma['FRANJA_PRECIO'].fillna('SIN CATEGORIA').str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION'].fillna('').str.upper()
    else:
        df_ma = pd.DataFrame(columns=['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA'])

    def process_sales(name, cant_idx):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'EMPRENDIMIENTO', 'FECHA_DT', 'MES'])
        res = pd.DataFrame()
        res['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        res['CANT'] = pd.to_numeric(df.iloc[:, cant_idx], errors='coerce').fillna(0)
        res['EMPRENDIMIENTO'] = df.iloc[:, 4].astype(str).str.strip().str.upper()
        col_f = next((c for c in df.columns if 'FECHA' in c), df.columns[0])
        res['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    def process_stock():
        df = data.get('Stock', pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'FECHA_DT', 'EMPRENDIMIENTO', 'MES'])
        res = pd.DataFrame()
        res['SKU'] = df.iloc[:, 0].astype(str).str.strip().str.upper()
        res['CANT'] = pd.to_numeric(df.iloc[:, 1], errors='coerce').fillna(0)
        res['FECHA_DT'] = pd.to_datetime(df.iloc[:, 4], dayfirst=True, errors='coerce')
        res['EMPRENDIMIENTO'] = df.iloc[:, 5].astype(str).str.strip().str.upper()
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    si_raw = process_sales('Sell_in', 3)
    so_raw = process_sales('Sell_out', 2)
    stk_raw = process_stock()

    # --- 4. FILTROS ---
    search_sku = st.sidebar.text_input("🎯 Buscar SKU / Modelo").upper()
    canales_op = ["WHOLESALE", "E-COM", "RETAIL"]
    f_emp = st.sidebar.multiselect("🚀 Emprendimiento (Canal)", canales_op, default=canales_op)
    
    meses_op = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True)
    f_mes = st.sidebar.selectbox("📅 Período de Venta", ["Todos"] + meses_op)
    
    dis_op = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()])
    f_dis = st.sidebar.multiselect("👟 Disciplinas", dis_op)
    
    fra_op = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()])
    f_fra = st.sidebar.multiselect("💰 Franjas de Precio", fra_op)

    def apply_filters(df, is_stock=False):
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'EMPRENDIMIENTO', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA', 'MES'])
        temp = df.copy()
        if f_emp:
            pattern = '|'.join(f_emp)
            temp = temp[temp['EMPRENDIMIENTO'].str.contains(pattern, na=False)]
        if not is_stock and f_mes != "Todos":
            temp = temp[temp['MES'] == f_mes]
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        temp[['DISCIPLINA', 'FRANJA_PRECIO']] = temp[['DISCIPLINA', 'FRANJA_PRECIO']].fillna('SIN CATEGORIA')
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_sku: temp = temp[temp['BUSQUEDA'].str.contains(search_sku, na=False)]
        return temp

    so_f = apply_filters(so_raw)
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw, is_stock=True)

    # --- 5. KPIs ---
    dass_raw = stk_raw[stk_raw['EMPRENDIMIENTO'].str.contains('DASS', na=False)]
    snap_dass = dass_raw[dass_raw['FECHA_DT'] == dass_raw['FECHA_DT'].max()] if not dass_raw.empty else pd.DataFrame()
    if not snap_dass.empty:
        snap_dass = snap_dass.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO']], on='SKU', how='left').fillna('SIN CATEGORIA')

    df_cli = stk_f[~stk_f['EMPRENDIMIENTO'].str.contains('DASS', na=False)]
    snap_wh = df_cli[df_cli['FECHA_DT'] == df_cli['FECHA_DT'].max()] if not df_cli.empty else pd.DataFrame()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📈 Sell Out Total", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("📦 Sell In Total", f"{si_f['CANT'].sum():,.0f}")
    k3.metric("🏢 Stock Dass", f"{snap_dass['CANT'].sum():,.0f}" if not snap_dass.empty else "0")
    k4.metric("🤝 Stock Cliente Filtrado", f"{snap_wh['CANT'].sum():,.0f}" if not snap_wh.empty else "0")

    # --- 6. GRÁFICOS: DISTRIBUCIÓN POR DISCIPLINA ---
    st.divider()
    st.subheader("📊 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        if not snap_dass.empty: st.plotly_chart(px.pie(snap_dass.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        # Mantenemos el gráfico aunque el filtro lo vacíe
        df_so_pie = so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index()
        if not df_so_pie.empty: st.plotly_chart(px.pie(df_so_pie, values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if not snap_wh.empty: st.plotly_chart(px.pie(snap_wh.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        if not si_f.empty:
            fig_si_mes = px.bar(si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index(), x='MES', y='CANT', color='DISCIPLINA', title="Sell In por Mes", color_discrete_map=COLOR_MAP_DIS, text='CANT')
            fig_si_mes.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            st.plotly_chart(fig_si_mes, use_container_width=True)

    # --- 7. GRÁFICOS: FRANJA Y CANAL ---
    st.divider()
    st.subheader("💰 Segmentación por Franja y Canal")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    with f1:
        df_so_fra = so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index()
        if not df_so_fra.empty: st.plotly_chart(px.pie(df_so_fra, values='CANT', names='FRANJA_PRECIO', title="Venta (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f2:
        df_so_emp = so_f.groupby('EMPRENDIMIENTO')['CANT'].sum().reset_index()
        if not df_so_emp.empty: st.plotly_chart(px.bar(df_so_emp, x='EMPRENDIMIENTO', y='CANT', title="Venta por Canal", color_discrete_sequence=['#0055A4'], text='CANT'), use_container_width=True)
    with f3:
        if not snap_wh.empty: st.plotly_chart(px.bar(snap_wh.groupby('EMPRENDIMIENTO')['CANT'].sum().reset_index(), x='EMPRENDIMIENTO', y='CANT', title="Stock por Canal", color_discrete_sequence=['#FFD700'], text='CANT'), use_container_width=True)
    with f4:
        if not si_f.empty:
            fig_si_fra = px.bar(si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index(), x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In por Franja", color_discrete_map=COLOR_MAP_FRA, text='CANT')
            fig_si_fra.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            st.plotly_chart(fig_si_fra, use_container_width=True)

    # --- 8. EVOLUCIÓN TEMPORAL ---
    st.divider()
    st.subheader("📈 Evolución Temporal del Negocio")
    h_so = so_f.groupby('MES')['CANT'].sum().reset_index(name='SO')
    h_si = si_f.groupby('MES')['CANT'].sum().reset_index(name='SI')
    h_sd = stk_raw[stk_raw['EMPRENDIMIENTO'].str.contains('DASS')].groupby('MES')['CANT'].sum().reset_index(name='SD')
    h_sc = stk_f[~stk_f['EMPRENDIMIENTO'].str.contains('DASS')].groupby('MES')['CANT'].sum().reset_index(name='SC')
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SO'], name='Sell Out', line=dict(color='#0055A4', width=3)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SI'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SD'], name='Stock Dass', line=dict(color='#00A693')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SC'], name='Stock Cliente', line=dict(color='#FFD700')))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 9. MATRIZ SKU ---
    st.divider()
    st.subheader("📋 Matriz de Inteligencia por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_sd = snap_dass.groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass') if not snap_dass.empty else pd.DataFrame(columns=['SKU', 'Stock Dass'])
    t_sc = snap_wh.groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente') if not snap_wh.empty else pd.DataFrame(columns=['SKU', 'Stock Cliente'])
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='outer').merge(t_si, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_sc, on='SKU', how='left').fillna(0)
    mask = (df_final['Sell Out'] > 0) | (df_final['Sell In'] > 0) | (df_final['Stock Dass'] > 0) | (df_final['Stock Cliente'] > 0)
    st.dataframe(df_final[mask].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("Error al conectar con Drive. Revisa st.secrets.")
