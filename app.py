import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Performance & Inteligencia => Fila Calzado", layout="wide")

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
    # --- 3. PROCESAMIENTO ---
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

    # --- 4. STOCK SNAPSHOT ---
    if not stk_raw.empty:
        max_date_stk = stk_raw['FECHA_DT'].max()
        stk_snap = stk_raw[stk_raw['FECHA_DT'] == max_date_stk].copy()
        stk_snap = stk_snap.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        for c in ['DISCIPLINA', 'FRANJA_PRECIO']: stk_snap[c] = stk_snap[c].fillna('SIN CATEGORIA')
    else:
        stk_snap = pd.DataFrame()

    # --- 5. FILTROS SIDEBAR ---
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

    so_f, si_f = apply_logic(so_raw), apply_logic(si_raw)

    # --- 6. KPIs ---
    st.title("📊 Torre de Control Dass v12.1 (Restaurada)")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out (Filtro)", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In (Filtro)", f"{si_f['CANT'].sum():,.0f}")
    val_d = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k3.metric("Stock Dass (Actual)", f"{val_d:,.0f}")
    val_c = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k4.metric("Stock Cliente (Actual)", f"{val_c:,.0f}")

    # --- 7. ANÁLISIS VISUAL POR SECCIONES ---
    st.divider()
    st.subheader("📌 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        if val_d > 0: st.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        if not so_f.empty: st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if val_c > 0: st.plotly_chart(px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        if not si_f.empty:
            df_bar_dis = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
            st.plotly_chart(px.bar(df_bar_dis, x='MES', y='CANT', color='DISCIPLINA', title="Sell In por Disciplina (Mix)", color_discrete_map=COLOR_MAP_DIS, text_auto='.2s'), use_container_width=True)

    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    with f1:
        if val_d > 0: st.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f2:
        if not so_f.empty: st.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f3:
        if val_c > 0: st.plotly_chart(px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f4:
        if not si_f.empty:
            df_bar_fra = si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index()
            st.plotly_chart(px.bar(df_bar_fra, x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In por Franja (Mix)", color_discrete_map=COLOR_MAP_FRA, text_auto='.2s'), use_container_width=True)

    # --- 8. EVOLUCIÓN HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica Comparativa")
    h_so = apply_logic(so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
    h_si = apply_logic(si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell In')
    h_sd = apply_logic(stk_raw, False)[stk_raw['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index(name='Stock Dass')
    h_sc = apply_logic(stk_raw, False)[~stk_raw['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index(name='Stock Cliente')
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Dass'], name='Stock Dass', line=dict(color='#00A693', width=2)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Cliente'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 9. RANKINGS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings")
    mes_actual = st.selectbox("Periodo Reciente (A)", meses_op, index=0)
    mes_anterior = st.selectbox("Periodo Anterior (B)", meses_op, index=min(1, len(meses_op)-1))
    
    def get_rank_df(mes):
        return so_raw[so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index().assign(Puesto=lambda x: x['CANT'].rank(ascending=False, method='min'))

    rk_a, rk_b = get_rank_df(mes_actual), get_rank_df(mes_anterior)
    df_rank = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Puesto', 'CANT']], on='SKU', how='inner').merge(rk_b[['SKU', 'Puesto']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Puesto_B': 999})
    df_rank['Salto'] = df_rank['Puesto_B'] - df_rank['Puesto_A']
    st.dataframe(df_rank.sort_values('Puesto_A').head(10), use_container_width=True, hide_index=True)

    # --- 10. EXPLORADOR TÁCTICO ---
    st.divider()
    st.subheader("👟 Explorador Táctico")
    dis_sel = st.selectbox("Selecciona Disciplina:", sorted(df_rank['DISCIPLINA'].unique()))
    df_dis_sel = df_rank[df_rank['DISCIPLINA'] == dis_sel].copy()
    st.dataframe(df_dis_sel.sort_values('CANT', ascending=False).head(10), use_container_width=True)

    # --- 11. ALERTA DE QUIEBRE (MOS) ---
    st.divider()
    st.subheader("🚨 Alerta de Quiebre (MOS)")
    t_stk_all = stk_snap.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')
    df_mos = df_rank.merge(t_stk_all, on='SKU', how='left').fillna(0)
    df_mos['MOS'] = (df_mos['Stock_Total'] / df_mos['CANT']).replace([float('inf')], 0)
    
    def semaforo(row):
        if row['Salto'] >= 5 and row['MOS'] < 1 and row['CANT'] > 0: return '🔴 CRÍTICO'
        if row['Salto'] > 0 and row['MOS'] < 2 and row['CANT'] > 0: return '🟡 ADVERTENCIA'
        return '🟢 OK'
    
    df_mos['Estado'] = df_mos.apply(semaforo, axis=1)
    st.dataframe(df_mos[df_mos['Estado'] != '🟢 OK'].sort_values('MOS'), use_container_width=True)
    st.plotly_chart(px.scatter(df_mos[df_mos['CANT'] > 0], x='Salto', y='MOS', size='CANT', color='Estado', hover_name='DESCRIPCION', color_discrete_map={'🔴 CRÍTICO': '#ff4b4b', '🟡 ADVERTENCIA': '#ffa500', '🟢 OK': '#28a745'}), use_container_width=True)

else:
    st.error("Sin conexión a Drive.")
