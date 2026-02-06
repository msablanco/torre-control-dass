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

# --- 1. CONFIGURACIÓN VISUAL (MAPAS DE COLORES) ---
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
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str, header=None)
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. PROCESAMIENTO MAESTRO ---
    df_ma_raw = data.get('Maestro_Productos', pd.DataFrame())
    df_ma = pd.DataFrame()
    if not df_ma_raw.empty:
        df_ma = df_ma_raw.copy()
        df_ma.columns = df_ma.iloc[0].str.upper()
        df_ma = df_ma[1:].reset_index(drop=True)
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']:
            if col in df_ma.columns:
                df_ma[col] = df_ma[col].fillna('SIN CATEGORIA').astype(str).str.upper()
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    # --- 3. LIMPIEZA GENÉRICA (STOCK USA COL E PARA FECHA) ---
    def clean_generic(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        res = pd.DataFrame()
        # Col A(0)=SKU, B(1)=CANT, E(4)=FECHA, F(5)=CLIENTE
        res['SKU'] = df.iloc[1:, 0].astype(str).str.strip().str.upper()
        res['CANT'] = pd.to_numeric(df.iloc[1:, 1], errors='coerce').fillna(0)
        res['FECHA_DT'] = pd.to_datetime(df.iloc[1:, 4], dayfirst=True, errors='coerce')
        res['CLIENTE_UP'] = df.iloc[1:, 5].astype(str).str.strip().str.upper()
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    so_raw = clean_generic('Sell_out')
    si_raw = clean_generic('Sell_in')
    stk_raw = clean_generic('Stock')

    # --- 4. FILTROS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Descripción").upper()
    
    meses_op = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True) if not so_raw.empty else []
    f_periodo = st.sidebar.selectbox("📅 Período (Mes)", ["Todos"] + meses_op)
    
    opts_dis = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_dis = st.sidebar.multiselect("👟 Disciplinas", opts_dis)
    
    opts_fra = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_fra = st.sidebar.multiselect("💰 Franjas de Precio", opts_fra)

    def apply_logic(df, filter_month=True):
        if df.empty: return df
        temp = df.copy()
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        for c in ['DISCIPLINA', 'FRANJA_PRECIO']: temp[c] = temp[c].fillna('SIN CATEGORIA')
        
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: 
            temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos":
            temp = temp[temp['MES'] == f_periodo]
        return temp

    so_f = apply_logic(so_raw)
    si_f = apply_logic(si_raw)
    stk_filtered = apply_logic(stk_raw)

    # --- 5. LÓGICA DE STOCK (FECHA MÁXIMA POR GRUPO) ---
    st.title("📊 Torre de Control Dass v11.38")
    
    # Stock Dass: Contiene "DASS" en Col F, fecha max de esos registros
    df_dass = stk_filtered[stk_filtered['CLIENTE_UP'].str.contains('DASS', na=False)]
    max_date_dass = df_dass['FECHA_DT'].max() if not df_dass.empty else None
    stk_dass_snap = df_dass[df_dass['FECHA_DT'] == max_date_dass] if max_date_dass else pd.DataFrame()
    val_stk_dass = stk_dass_snap['CANT'].sum()

    # Stock Cliente: Contiene "WHOLESALE" en Col F, fecha max de esos registros
    df_wh = stk_filtered[stk_filtered['CLIENTE_UP'].str.contains('WHOLESALE', na=False)]
    max_date_wh = df_wh['FECHA_DT'].max() if not df_wh.empty else None
    stk_wh_snap = df_wh[df_wh['FECHA_DT'] == max_date_wh] if max_date_wh else pd.DataFrame()
    val_stk_cli = stk_wh_snap['CANT'].sum()

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['CANT'].sum():,.0f}")
    k3.metric("Stock Dass", f"{val_stk_dass:,.0f}")
    k4.metric("Stock Cliente", f"{val_stk_cli:,.0f}")

    # --- 6. GRÁFICOS POR DISCIPLINA ---
    st.divider()
    st.subheader("📌 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    
    with c1:
        if not stk_dass_snap.empty:
            st.plotly_chart(px.pie(stk_dass_snap.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        if not so_f.empty:
            st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if not stk_wh_snap.empty:
            st.plotly_chart(px.pie(stk_wh_snap.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        if not si_f.empty:
            df_bar = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
            fig = px.bar(df_bar, x='MES', y='CANT', color='DISCIPLINA', title="Sell In por Disciplina", color_discrete_map=COLOR_MAP_DIS, text_auto='.2s')
            fig.update_layout(barmode='stack')
            st.plotly_chart(fig, use_container_width=True)

    # --- 7. GRÁFICOS POR FRANJA ---
    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    with f1:
        if not stk_dass_snap.empty:
            st.plotly_chart(px.pie(stk_dass_snap.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f2:
        if not so_f.empty:
            st.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f3:
        if not stk_wh_snap.empty:
            st.plotly_chart(px.pie(stk_wh_snap.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f4:
        if not si_f.empty:
            df_si_fra = si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index()
            fig_f = px.bar(df_si_fra, x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In por Franja", color_discrete_map=COLOR_MAP_FRA, text_auto='.2s')
            st.plotly_chart(fig_f, use_container_width=True)

    # --- 8. EVOLUCIÓN HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica Comparativa")
    # Para el histórico usamos apply_logic sin filtro de mes (filter_month=False)
    h_so = apply_logic(so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SO')
    h_si = apply_logic(si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SI')
    
    stk_h_all = apply_logic(stk_raw, False)
    h_sd = stk_h_all[stk_h_all['CLIENTE_UP'].str.contains('DASS')].groupby('MES')['CANT'].sum().reset_index(name='SD')
    h_sc = stk_h_all[stk_h_all['CLIENTE_UP'].str.contains('WHOLESALE')].groupby('MES')['CANT'].sum().reset_index(name='SC')
    
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SO'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SI'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SD'], name='Stock Dass', line=dict(color='#00A693', width=2)))
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['SC'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
    st.plotly_chart(fig_line, use_container_width=True)

    # --- 9. TABLA DE DETALLE ---
    st.divider()
    st.subheader("📋 Detalle de Inventario y Ventas por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_sd = stk_dass_snap.groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_sc = stk_wh_snap.groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_si, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_sc, on='SKU', how='left').fillna(0)
    
    # Mostrar solo registros con alguna actividad
    mask = (df_final['Sell Out'] > 0) | (df_final['Sell In'] > 0) | (df_final['Stock Dass'] > 0) | (df_final['Stock Cliente'] > 0)
    st.dataframe(df_final[mask].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("No se detectaron archivos en la carpeta de Google Drive configurada.")
