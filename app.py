import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Turevi En Tan", layout="wide")

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
    # --- 2. MAESTRO (Sin cambios) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION'].fillna('')

    # --- 3. LÓGICA DE STOCK AJUSTADA ---
    def process_stock():
        df = data.get('Stock', pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        res = pd.DataFrame()
        try:
            # Col A(0)=SKU, B(1)=CANT, E(4)=FECHA, F(5)=CLIENTE
            res['SKU'] = df.iloc[:, 0].astype(str).str.strip().str.upper()
            res['CANT'] = pd.to_numeric(df.iloc[:, 1], errors='coerce').fillna(0)
            res['FECHA_DT'] = pd.to_datetime(df.iloc[:, 4], dayfirst=True, errors='coerce')
            res['CLIENTE_UP'] = df.iloc[:, 5].astype(str).str.strip().str.upper()
            res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
            return res
        except Exception:
            st.error("El archivo de Stock no tiene el formato esperado (Columnas B, E, F)")
            return pd.DataFrame()

    # --- 4. VENTAS (INTOUCHABLE - COMO ESTABA) ---
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

    # --- 5. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    meses = sorted(so_raw['MES'].dropna().unique(), reverse=True) if not so_raw.empty else []
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses)
    
    def enrich_and_filter(df, is_stock=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        temp[['DISCIPLINA', 'FRANJA_PRECIO']] = temp[['DISCIPLINA', 'FRANJA_PRECIO']].fillna('SIN CATEGORIA')
        if search_query:
            temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if not is_stock and f_periodo != "Todos":
            temp = temp[temp['MES'] == f_periodo]
        return temp

    stk_f = enrich_and_filter(stk_raw, is_stock=True)
    so_f = enrich_and_filter(so_raw)
    si_f = enrich_and_filter(si_raw)

    st.title("📊 Torre de Control Dass v11.38")

    # --- 6. CÁLCULO DE STOCK (LOGICA DE FECHA MÁXIMA SEGÚN FILA) ---
    # Stock Dass: registros que contienen 'DASS' en Col F
    df_dass_total = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]
    max_date_dass = df_dass_total['FECHA_DT'].max()
    snap_dass = df_dass_total[df_dass_total['FECHA_DT'] == max_date_dass]
    
    # Stock Cliente: registros que contienen 'WHOLESALE' en Col F
    df_wh_total = stk_f[stk_f['CLIENTE_UP'].str.contains('WHOLESALE', na=False)]
    max_date_wh = df_wh_total['FECHA_DT'].max()
    snap_wh = df_wh_total[df_wh_total['FECHA_DT'] == max_date_wh]
    
    v_stk_dass = snap_dass['CANT'].sum()
    v_stk_wh = snap_wh['CANT'].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['CANT'].sum():,.0f}")
    k3.metric("Stock Dass", f"{v_stk_dass:,.0f}")
    k4.metric("Stock Cliente", f"{v_stk_wh:,.0f}")

    # --- 7. GRÁFICOS RESTAURADOS ---
    st.divider()
    st.subheader("📌 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        st.plotly_chart(px.pie(snap_dass.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if v_stk_wh > 0:
            st.plotly_chart(px.pie(snap_wh.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        df_si_m = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
        st.plotly_chart(px.bar(df_si_m, x='MES', y='CANT', color='DISCIPLINA', title="Sell In por Disciplina", color_discrete_map=COLOR_MAP_DIS, text_auto='.2s'), use_container_width=True)

    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    with f1:
        st.plotly_chart(px.pie(snap_dass.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f2:
        st.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f3:
        if v_stk_wh > 0:
            st.plotly_chart(px.pie(snap_wh.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f4:
        df_si_f = si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index()
        st.plotly_chart(px.bar(df_si_f, x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In por Franja", color_discrete_map=COLOR_MAP_FRA, text_auto='.2s'), use_container_width=True)

    # --- 8. EVOLUCIÓN HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica")
    h_so = enrich_and_filter(so_raw).groupby('MES')['CANT'].sum().reset_index(name='SO')
    h_si = enrich_and_filter(si_raw).groupby('MES')['CANT'].sum().reset_index(name='SI')
    h_stk = enrich_and_filter(stk_raw, is_stock=True)
    h_sd = h_stk[h_stk['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index(name='SD')
    h_sc = h_stk[h_stk['CLIENTE_UP'].str.contains('WHOLESALE', na=False)].groupby('MES')['CANT'].sum().reset_index(name='SC')
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SO'], name='Sell Out', line=dict(color='#0055A4', width=3)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SI'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SD'], name='Stock Dass', line=dict(color='#00A693')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SC'], name='Stock Cliente', line=dict(color='#FFD700')))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 9. TABLA DE DETALLE ---
    st.divider()
    st.subheader("📋 Detalle de Inventario y Ventas por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_sd = snap_dass.groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_sc = snap_wh.groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_res = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_si, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_sc, on='SKU', how='left').fillna(0)
    st.dataframe(df_res[df_res[['Sell Out', 'Sell In', 'Stock Dass', 'Stock Cliente']].sum(axis=1) > 0].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("No se detectaron archivos.")
