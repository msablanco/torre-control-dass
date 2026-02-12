import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL (MAPAS DE COLORES CONSISTENTES) ---
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

# --- 2. CARGA DE DATOS ---
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
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_data()

if data:
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col, default in {'DISCIPLINA': 'SIN CATEGORIA', 'FRANJA_PRECIO': 'SIN CATEGORIA', 'DESCRIPCION': 'SIN DESCRIPCION'}.items():
            if col not in df_ma.columns: df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'MES', 'FECHA_DT', 'CLIENTE_UP'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'CANT')
        df['CANT'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), 'FECHA')
        df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.upper()
        return df

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    meses_op = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True) if not so_raw.empty else []
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_op)
    opts_dis = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_dis = st.sidebar.multiselect("👟 Disciplinas", opts_dis)
    opts_fra = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_fra = st.sidebar.multiselect("💰 Franjas", opts_fra)
    f_cli_so = st.sidebar.multiselect("👤 Cliente SO", sorted(so_raw['CLIENTE_UP'].unique()) if not so_raw.empty else [])
    f_cli_si = st.sidebar.multiselect("📦 Cliente SI", sorted(si_raw['CLIENTE_UP'].unique()) if not si_raw.empty else [])
    selected_clients = set(f_cli_so) | set(f_cli_si)

    def apply_logic(df, filter_month=True):
        if df.empty: return df
        temp = df.copy()
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        for c in ['DISCIPLINA', 'FRANJA_PRECIO']: temp[c] = temp[c].fillna('SIN CATEGORIA')
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: 
            temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False) | temp['SKU'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos":
            temp = temp[temp['MES'] == f_periodo]
        if selected_clients:
            temp = temp[temp['CLIENTE_UP'].isin(selected_clients)]
        return temp

    so_f, si_f, stk_f = apply_logic(so_raw), apply_logic(si_raw), apply_logic(stk_raw)

    st.title("📊 Torre de Control Dass v11.38")
    max_date = stk_f['FECHA_DT'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['FECHA_DT'] == max_date] if max_date else pd.DataFrame()
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['CANT'].sum():,.0f}")
    val_d = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k3.metric("Stock Dass", f"{val_d:,.0f}")
    val_c = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k4.metric("Stock Cliente", f"{val_c:,.0f}")

    st.divider()
    st.subheader("📌 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    
    if val_d > 0:
        c1.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not so_f.empty:
        c2.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if val_c > 0:
        c3.plotly_chart(px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    
    if not si_f.empty:
        # Gráfico de barras con unidades reales y etiquetas de %
        df_bar_dis = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
        fig_bar_dis = px.bar(df_bar_dis, x='MES', y='CANT', color='DISCIPLINA', title="Sell In por Disciplina (con % Mix)", 
                             color_discrete_map=COLOR_MAP_DIS, text_auto='.2s')
        # Calculamos el porcentaje relativo a cada mes
        fig_bar_dis.update_traces(textposition='inside')
        # Esta línea permite que al pasar el mouse se vea el % relativo al grupo (mes)
        fig_bar_dis.update_layout(barmode='stack', yaxis_title="Unidades")
        c4.plotly_chart(fig_bar_dis, use_container_width=True)

    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    
    if val_d > 0:
        f1.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    if not so_f.empty:
        f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    if val_c > 0:
        f3.plotly_chart(px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    
    if not si_f.empty:
        df_bar_fra = si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index()
        fig_bar_fra = px.bar(df_bar_fra, x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In por Franja (con % Mix)", 
                             color_discrete_map=COLOR_MAP_FRA, text_auto='.2s')
        fig_bar_fra.update_traces(textposition='inside')
        fig_bar_fra.update_layout(barmode='stack', yaxis_title="Unidades")
        f4.plotly_chart(fig_bar_fra, use_container_width=True)

    st.divider()
    st.subheader("📈 Evolución Histórica Comparativa")
    h_so = apply_logic(so_raw, False).groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Sell Out'})
    h_si = apply_logic(si_raw, False).groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Sell In'})
    stk_h_all = apply_logic(stk_raw, False)
    h_sd = stk_h_all[stk_h_all['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Stock Dass'})
    h_sc = stk_h_all[~stk_h_all['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Stock Cliente'})
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Dass'], name='Stock Dass', line=dict(color='#00A693', width=2)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Cliente'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
    st.plotly_chart(fig_h, use_container_width=True)

    st.divider()
    st.subheader("📋 Detalle de Inventario y Ventas por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_stk_d = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_stk_c = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').merge(t_stk_d, on='SKU', how='left').merge(t_si, on='SKU', how='left').fillna(0)
    df_final = df_final[(df_final['Sell Out'] > 0) | (df_final['Stock Cliente'] > 0) | (df_final['Stock Dass'] > 0) | (df_final['Sell In'] > 0)]
    st.dataframe(df_final.sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)
else:
    st.error("No se detectaron archivos.")

