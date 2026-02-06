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
    page_icon="logo_fila.png", # <--- Esto pone el logo en la pestaña del navegador
    layout="wide"
)
if data:
    # --- BARRA LATERAL CON LOGO ---
    try:
        st.sidebar.image("logo_fila.png", use_container_width=True)
    except:
        st.sidebar.warning("Logo no encontrado. Subí 'logo_fila.png' a GitHub.")

    st.sidebar.header("🔍 Inteligencia de Datos")
    # ... (Tus filtros de siempre)

    # --- TÍTULO PRINCIPAL CON LOGO AL LADO ---
    col_l, col_t = st.columns([1, 5])
    with col_l:
        try:
            st.image("logo_fila.png", width=120)
        except:
            pass
    with col_t:
        st.title("Performance & Inteligencia: Fila Calzado")
# --- 1. CONFIGURACIÓN VISUAL ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3', 'OTRO': '#E5E5E5'
}
COLOR_MAP_FRA = {
    'PINNACLE': '#4B0082', 'BEST': '#1E90FF', 'BETTER': '#32CD32', 
    'GOOD': '#FF8C00', 'CORE': '#696969', 'SIN CATEGORIA': '#D3D3D3'
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
        df_ma['DISCIPLINA'] = df_ma['DISCIPLINA'].fillna('SIN CATEGORIA').astype(str).str.upper()
        df_ma['FRANJA_PRECIO'] = df_ma['FRANJA_PRECIO'].fillna('SIN CATEGORIA').astype(str).str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION'].fillna('').astype(str).str.upper()

    # --- 3. LÓGICA DE STOCK (ESTRICTA E+F) ---
    def process_stock():
        df = data.get('Stock', pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        res = pd.DataFrame()
        try:
            # A=SKU, B=CANT, E=FECHA, F=CLIENTE
            res['SKU'] = df.iloc[:, 0].astype(str).str.strip().str.upper()
            res['CANT'] = pd.to_numeric(df.iloc[:, 1], errors='coerce').fillna(0)
            res['FECHA_DT'] = pd.to_datetime(df.iloc[:, 4], dayfirst=True, errors='coerce')
            res['CLIENTE_UP'] = df.iloc[:, 5].astype(str).str.strip().str.upper()
            res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
            return res
        except: return pd.DataFrame()

    # --- 4. VENTAS (INTOUCHABLE) ---
    def process_sales(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        res = pd.DataFrame()
        res['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_c = next((c for c in df.columns if 'CANT' in c or 'UNID' in c), df.columns[1])
        res['CANT'] = pd.to_numeric(df[col_c], errors='coerce').fillna(0)
        col_f = next((c for c in df.columns if 'FECHA' in c), df.columns[0])
        res['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    stk_raw = process_stock()
    so_raw = process_sales('Sell_out')
    si_raw = process_sales('Sell_in')

    # --- 5. FILTROS EN BARRA LATERAL ---
    st.sidebar.header("🔍 Filtros de Control")
    search_sku = st.sidebar.text_input("🎯 SKU o Descripción").upper()
    
    meses_op = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True) if not so_raw.empty else []
    f_mes = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + meses_op)
    
    dis_op = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()]) if not df_ma.empty else []
    f_dis = st.sidebar.multiselect("👟 Disciplina", dis_op)
    
    fra_op = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()]) if not df_ma.empty else []
    f_fra = st.sidebar.multiselect("💰 Franja de Precio", fra_op)

    def apply_filters(df, is_stock=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        temp[['DISCIPLINA', 'FRANJA_PRECIO']] = temp[['DISCIPLINA', 'FRANJA_PRECIO']].fillna('SIN CATEGORIA')
        
        if search_sku:
            temp = temp[temp['BUSQUEDA'].str.contains(search_sku, na=False)]
        if f_dis:
            temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra:
            temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if not is_stock and f_mes != "Todos":
            temp = temp[temp['MES'] == f_mes]
        return temp

    stk_f = apply_filters(stk_raw, is_stock=True)
    so_f = apply_filters(so_raw)
    si_f = apply_filters(si_raw)

    # --- 6. MÉTRICAS ---
    st.title("📊 Performance & Inteligencia: Fila Calzado")
    
    # Stock Dass (Última fecha de DASS)
    df_dass = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]
    max_d_dass = df_dass['FECHA_DT'].max()
    snap_dass = df_dass[df_dass['FECHA_DT'] == max_d_dass]
    
    # Stock Cliente (Última fecha de WHOLESALE)
    df_wh = stk_f[stk_f['CLIENTE_UP'].str.contains('WHOLESALE', na=False)]
    max_d_wh = df_wh['FECHA_DT'].max()
    snap_wh = df_wh[df_wh['FECHA_DT'] == max_d_wh]
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['CANT'].sum():,.0f}")
    k3.metric("Stock Dass", f"{snap_dass['CANT'].sum():,.0f}")
    k4.metric("Stock Cliente", f"{snap_wh['CANT'].sum():,.0f}")

    # --- 7. GRÁFICOS ---
    st.divider()
    st.subheader("📌 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        st.plotly_chart(px.pie(snap_dass.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if not snap_wh.empty:
            st.plotly_chart(px.pie(snap_wh.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        st.plotly_chart(px.bar(si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index(), x='MES', y='CANT', color='DISCIPLINA', title="Sell In Historico", color_discrete_map=COLOR_MAP_DIS, text_auto='.2s'), use_container_width=True)

    st.subheader("💰 Análisis por Franja")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    with f1:
        st.plotly_chart(px.pie(snap_dass.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f2:
        st.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f3:
        if not snap_wh.empty:
            st.plotly_chart(px.pie(snap_wh.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f4:
        st.plotly_chart(px.bar(si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index(), x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In por Franja", color_discrete_map=COLOR_MAP_FRA, text_auto='.2s'), use_container_width=True)

    # --- 8. HISTÓRICO (CORREGIDO NAMEERROR) ---
    st.divider()
    st.subheader("📈 Evolución Temporal")
    h_so = apply_filters(so_raw, is_stock=False).groupby('MES')['CANT'].sum().reset_index(name='SO')
    h_si = apply_filters(si_raw, is_stock=False).groupby('MES')['CANT'].sum().reset_index(name='SI')
    h_stk = apply_filters(stk_raw, is_stock=True)
    h_sd = h_stk[h_stk['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index(name='SD')
    h_sc = h_stk[h_stk['CLIENTE_UP'].str.contains('WHOLESALE', na=False)].groupby('MES')['CANT'].sum().reset_index(name='SC')
    
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SO'], name='Sell Out', line=dict(color='#0055A4', width=3)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SI'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SD'], name='Stock Dass', line=dict(color='#00A693')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SC'], name='Stock Cliente', line=dict(color='#FFD700')))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 9. TABLA DE DATOS ---
    st.divider()
    st.subheader("📋 Detalle por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_sd = snap_dass.groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_sc = snap_wh.groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_si, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_sc, on='SKU', how='left').fillna(0)
    
    mask = (df_final['Sell Out'] > 0) | (df_final['Sell In'] > 0) | (df_final['Stock Dass'] > 0) | (df_final['Stock Cliente'] > 0)
    st.dataframe(df_final[mask].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("No se detectaron archivos en Drive.")

