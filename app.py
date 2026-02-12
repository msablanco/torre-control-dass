import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
import datetime

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
    # --- 3. PROCESAMIENTO MAESTRO ---
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

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')
    ingresos_raw = clean_df('ingresos')

    # --- 4. FILTROS SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    meses_op = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes de Análisis", meses_op if meses_op else ["S/D"])
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo").upper()
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    st.sidebar.subheader("Filtros Clientes")
    f_cli_so = st.sidebar.multiselect("👤 Sell Out Clientes", sorted(so_raw['CLIENTE_UP'].unique()))
    f_cli_si = st.sidebar.multiselect("📦 Sell In Clientes", sorted(si_raw['CLIENTE_UP'].unique()))
    f_emp = st.sidebar.multiselect("🏬 Emprendimiento (Stock)", sorted(stk_raw['CLIENTE_UP'].unique()))

    def apply_logic(df, filter_month=True, tipo=None):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if filter_month: temp = temp[temp['MES'] == f_periodo]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        
        if tipo == 'SO' and f_cli_so: temp = temp[temp['CLIENTE_UP'].isin(f_cli_so)]
        if tipo == 'SI' and f_cli_si: temp = temp[temp['CLIENTE_UP'].isin(f_cli_si)]
        if tipo == 'STK' and f_emp: temp = temp[temp['CLIENTE_UP'].isin(f_emp)]
        return temp

    so_f = apply_logic(so_raw, True, 'SO')
    si_f = apply_logic(si_raw, True, 'SI')
    stk_f = apply_logic(stk_raw, True, 'STK')

    # --- 5. LÓGICA DE FUTUROS INGRESOS (CORREGIDA: INDEPENDIENTE DEL MES) ---
    hoy_actual = pd.Timestamp(datetime.date.today()).replace(day=1)
    if not ingresos_raw.empty:
        # Los ingresos NO se filtran por f_periodo, se filtran por fecha >= hoy
        df_ing_base = ingresos_raw.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'BUSQUEDA']], on='SKU', how='left')
        if f_dis: df_ing_base = df_ing_base[df_ing_base['DISCIPLINA'].isin(f_dis)]
        if f_fra: df_ing_base = df_ing_base[df_ing_base['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: df_ing_base = df_ing_base[df_ing_base['BUSQUEDA'].str.contains(search_query, na=False)]
        
        t_futuro = df_ing_base[df_ing_base['FECHA_DT'] >= hoy_actual].groupby('SKU')['CANT'].sum().reset_index(name='Futuros_Ingresos')
    else:
        t_futuro = pd.DataFrame(columns=['SKU', 'Futuros_Ingresos'])

    # --- 6. LÍNEA DE TIEMPO ---
    st.title(f"📊 Dashboard Performance - {f_periodo}")
    st.subheader("📈 Evolución Histórica")
    h_so = apply_logic(so_raw, False, 'SO').groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
    h_si = apply_logic(si_raw, False, 'SI').groupby('MES')['CANT'].sum().reset_index(name='Sell In')
    h_stk = apply_logic(stk_raw, False, 'STK').groupby(['MES', 'CLIENTE_UP'])['CANT'].sum().reset_index()
    h_sd = h_stk[h_stk['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index(name='Stock Dass')
    h_sc = h_stk[~h_stk['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index(name='Stock Cliente')
    df_hist = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=df_hist['MES'], y=df_hist['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_line.add_trace(go.Scatter(x=df_hist['MES'], y=df_hist['Sell In'], name='Sell In', line=dict(color='#FF3131', width=2, dash='dot')))
    fig_line.add_trace(go.Bar(x=df_hist['MES'], y=df_hist['Stock Dass'], name='Stock Dass', marker_color='#00A693', opacity=0.5))
    fig_line.add_trace(go.Bar(x=df_hist['MES'], y=df_hist['Stock Cliente'], name='Stock Cliente', marker_color='#FFD700', opacity=0.5))
    st.plotly_chart(fig_line, use_container_width=True)

    # --- 7. GRÁFICOS DE TORTA ---
    st.divider()
    st.subheader("👟 Análisis por Disciplina")
    c1, c2, c3 = st.columns(3)
    with c1: st.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2: st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3: st.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3 = st.columns(3)
    with f1: st.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f2: st.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with f3: st.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)

    # --- 8. PRECALCULO DE RANKINGS (Para evitar el NameError) ---
    m_ant_periodo = meses_op[min(1, len(meses_op)-1)]
    rk_a = so_raw[so_raw['MES'] == f_periodo].groupby('SKU')['CANT'].sum().reset_index().assign(P_A=lambda x: x['CANT'].rank(ascending=False))
    rk_b = so_raw[so_raw['MES'] == m_ant_periodo].groupby('SKU')['CANT'].sum().reset_index().assign(P_B=lambda x: x['CANT'].rank(ascending=False))

    # --- 9. TABLA DETALLE COMPLETA POR SKU ---
    st.divider()
    st.subheader("📋 Detalle SKU: Stock, Venta e Ingresos Futuros")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_In')
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Clientes')

    df_detalle = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left') \
        .merge(t_stk_c, on='SKU', how='left') \
        .merge(t_stk_d, on='SKU', how='left') \
        .merge(t_si, on='SKU', how='left') \
        .merge(t_futuro, on='SKU', how='left').fillna(0)
    
    df_detalle['Rotacion_Meses'] = (df_detalle['Stock_Clientes'] / df_detalle['Sell_Out']).replace([float('inf')], 0).fillna(0)
    st.dataframe(df_detalle.sort_values('Sell_Out', ascending=False), use_container_width=True, hide_index=True)

  # --- 10. DETALLE POR SKU (CORREGIDO) ---
st.divider()
st.subheader("📋 Detalle por SKU")

# Agrupamos los ingresos por SKU para sumarlos
t_ingresos = ingresos_raw.groupby('SKU')['CANT'].sum().reset_index(name='Futuros_Ingresos')

t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
t_stk_d = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
t_stk_c = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')

# Unimos t_ingresos a la cadena de merges
df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left') \
    .merge(t_stk_c, on='SKU', how='left') \
    .merge(t_stk_d, on='SKU', how='left') \
    .merge(t_si, on='SKU', how='left') \
    .merge(t_ingresos, on='SKU', how='left').fillna(0) # <-- Aquí se integra el dato

# Mostrar la tabla
st.dataframe(df_final.sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)
    # --- 11. RANKINGS Y TENDENCIAS ---
    st.divider()
    st.subheader("🏆 Rankings y Saltos de Posición")
    col_sel1, col_sel2 = st.columns(2)
    with col_sel1: m_act = st.selectbox("Periodo Reciente (A)", meses_op, index=0, key="act")
    with col_sel2: m_ant = st.selectbox("Periodo Anterior (B)", meses_op, index=min(1, len(meses_op)-1), key="ant")

    rk_a_dyn = so_raw[so_raw['MES'] == m_act].groupby('SKU')['CANT'].sum().reset_index().assign(P_A=lambda x: x['CANT'].rank(ascending=False, method='min'))
    rk_b_dyn = so_raw[so_raw['MES'] == m_ant].groupby('SKU')['CANT'].sum().reset_index().assign(P_B=lambda x: x['CANT'].rank(ascending=False, method='min'))
    
    df_rank = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a_dyn[['SKU', 'P_A', 'CANT']], on='SKU', how='inner')
    df_rank = df_rank.merge(rk_b_dyn[['SKU', 'P_B']], on='SKU', how='left').fillna({'P_B': 999})
    df_rank['Salto'] = df_rank['P_B'] - df_rank['P_A']
    df_rank = df_rank.merge(t_futuro, on='SKU', how='left').fillna(0)

    top_actual = df_rank.sort_values('P_A').head(10).copy()
    top_actual['Evolución'] = top_actual['Salto'].apply(lambda val: "🆕 Nuevo" if val > 500 else (f"⬆️ +{int(val)}" if val > 0 else (f"⬇️ {int(val)}" if val < 0 else "➡️ =")))
    st.dataframe(top_actual[['P_A', 'SKU', 'DESCRIPCION', 'CANT', 'Evolución', 'Futuros_Ingresos']], use_container_width=True, hide_index=True)

    # --- 12. EXPLORADOR TÁCTICO ---
    st.divider()
    st.subheader("👟 Explorador Táctico por Disciplina")
    disciplina_select = st.selectbox("Seleccioná una Disciplina:", sorted(df_rank['DISCIPLINA'].unique()))
    df_rank_dis = df_rank[df_rank['DISCIPLINA'] == disciplina_select].copy()
    df_rank_dis['Pos_Categoría'] = df_rank_dis['CANT'].rank(ascending=False, method='min')

    col_l1, col_l2 = st.columns([2, 1])
    with col_l1:
        df_dis_show = df_rank_dis.sort_values('Pos_Categoría').head(10).copy()
        df_dis_show['Evolución'] = df_dis_show['Salto'].apply(lambda x: "🔥 Nuevo" if x > 500 else (f"🔼 +{int(x)}" if x > 0 else (f"🔽 {int(x)}" if x < 0 else "⏺️ =")))
        st.dataframe(df_dis_show[['Pos_Categoría', 'SKU', 'DESCRIPCION', 'CANT', 'Evolución', 'Futuros_Ingresos']], use_container_width=True, hide_index=True)
    with col_l2:
        st.metric(f"Total Venta {disciplina_select}", f"{df_rank_dis['CANT'].sum():,.0f}")
        st.metric(f"Total Ingresos Futuros", f"{df_rank_dis['Futuros_Ingresos'].sum():,.0f}")

  



















