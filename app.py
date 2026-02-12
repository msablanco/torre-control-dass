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
        
        # Limpieza Crítica de SKU para el match
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Identificar cantidad
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'CANT')
        df['CANT'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        
        # Identificar fecha
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO', 'ETA'])), 'FECHA')
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
    
    st.sidebar.subheader("Filtros Específicos")
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

    # --- 5. LÓGICA DE INGRESOS FUTUROS (CORREGIDA) ---
    if not ingresos_raw.empty:
        # Los ingresos NO se filtran por el mes del sidebar
        df_ing_base = ingresos_raw.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'BUSQUEDA']], on='SKU', how='left')
        if f_dis: df_ing_base = df_ing_base[df_ing_base['DISCIPLINA'].isin(f_dis)]
        if f_fra: df_ing_base = df_ing_base[df_ing_base['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: df_ing_base = df_ing_base[df_ing_base['BUSQUEDA'].str.contains(search_query, na=False)]
        t_futuro = df_ing_base.groupby('SKU')['CANT'].sum().reset_index(name='Ingresos_Futuros')
    else:
        t_futuro = pd.DataFrame(columns=['SKU', 'Ingresos_Futuros'])

    # --- 6. LÍNEA DE TIEMPO HISTÓRICA ---
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

    # --- 7. ANÁLISIS POR TORTAS ---
    st.divider()
    c1, c2, c3 = st.columns(3)
    with c1: st.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2: st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3: st.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # --- 8. TABLA DETALLE CON SEMÁFORO E INGRESOS ---
    st.divider()
    st.subheader("📋 Detalle de Inventario, Ventas e Ingresos")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_Out')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Cli')
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    
    df_det = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left') \
        .merge(t_stk_c, on='SKU', how='left') \
        .merge(t_stk_d, on='SKU', how='left') \
        .merge(t_futuro, on='SKU', how='left').fillna(0)
    
    df_det['Cobertura'] = (df_det['Stock_Cli'] / df_det['Sell_Out']).replace([float('inf')], 99).fillna(0)

    def color_semaforo(val):
        if val == 0: return ''
        if val <= 1.5: return 'background-color: #FFB3B3' # Rojo: Crítico
        if val <= 3.5: return 'background-color: #B3FFB3' # Verde: Saludable
        return 'background-color: #FFFFB3' # Amarillo: Exceso

    st.dataframe(df_det.style.applymap(color_semaforo, subset=['Cobertura']), use_container_width=True, hide_index=True)

    # --- 9. RANKINGS Y COMPARATIVO PERIODOS ---
    st.divider()
    st.subheader("🏆 Comparativa de Rankings (Periodo A vs B)")
    col_a, col_b = st.columns(2)
    with col_a: m_act = st.selectbox("Mes Actual (A)", meses_op, index=0)
    with col_b: m_ant = st.selectbox("Mes Anterior (B)", meses_op, index=min(1, len(meses_op)-1))

    def get_rank(mes):
        df_r = so_raw[so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df_r['Pos'] = df_r['CANT'].rank(ascending=False, method='min')
        return df_r

    rk_a = get_rank(m_act)
    rk_b = get_rank(m_ant)
    
    df_rk = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
    df_rk = df_rk.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Pos_B': 999})
    df_rk['Salto'] = df_rk['Pos_B'] - df_rk['Pos_A']
    
    st.write(f"Top 10 Rendimiento en {m_act}")
    st.dataframe(df_rk.sort_values('Pos_A').head(10), use_container_width=True, hide_index=True)

    # --- 10. EXPLORADOR POR DISCIPLINA (PUNTO 13) ---
    st.divider()
    st.subheader("👟 Análisis Táctico por Disciplina")
    dis_list = sorted(df_ma['DISCIPLINA'].unique())
    disciplina_select = st.selectbox("Profundizar en:", dis_list)
    
    df_final_dis = df_rk[df_rk['DISCIPLINA'] == disciplina_select].copy()
    df_final_dis = df_final_dis.merge(t_futuro, on='SKU', how='left').fillna(0)
    
    c_m1, c_m2, c_m3 = st.columns(3)
    c_m1.metric(f"Venta {disciplina_select}", f"{df_final_dis['CANT'].sum():,.0f}")
    c_m2.metric("SKUs con Venta", len(df_final_dis[df_final_dis['CANT']>0]))
    c_m3.metric("Ingresos Futuros", f"{df_final_dis['Ingresos_Futuros'].sum():,.0f}")
    
    st.dataframe(df_final_dis.sort_values('CANT', ascending=False), use_container_width=True, hide_index=True)# --- 11. RANKINGS Y SALTOS DE POSICIÓN DINÁMICOS ---
    st.divider()
    st.subheader("🏆 11. Análisis de Tendencias y Rankings")
    
    col_sel1, col_sel2 = st.columns(2)
    with col_sel1: 
        m_act_rank = st.selectbox("Seleccionar Mes Reciente (A):", meses_op, index=0, key="rank_a")
    with col_sel2: 
        m_ant_rank = st.selectbox("Seleccionar Mes de Contraste (B):", meses_op, index=min(1, len(meses_op)-1), key="rank_b")

    def calcular_ranking(mes):
        df_res = so_raw[so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df_res['Pos'] = df_res['CANT'].rank(ascending=False, method='min')
        return df_res

    rk_reciente = calcular_ranking(m_act_rank)
    rk_anterior = calcular_ranking(m_ant_rank)
    
    df_tendencia = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_reciente[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
    df_tendencia = df_tendencia.merge(rk_anterior[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Pos_B': 999})
    
    # Cálculo del salto: si bajó el número de posición, subió en el ranking
    df_tendencia['Evolucion_Posicion'] = df_tendencia['Pos_B'] - df_tendencia['Pos_A']
    
    # Unir con ingresos para ver si lo que está subiendo tiene backup
    df_tendencia = df_tendencia.merge(t_futuro, on='SKU', how='left').fillna(0)

    st.write(f"Mostrando los 15 productos con mayor venta en {m_act_rank} y su comparativa con {m_ant_rank}:")
    
    def format_evolucion(val):
        if val > 500: return "🆕 Nuevo"
        if val > 0: return f"⬆️ +{int(val)}"
        if val < 0: return f"⬇️ {int(val)}"
        return "➡️ ="

    df_tendencia['Status'] = df_tendencia['Evolucion_Posicion'].apply(format_evolucion)
    
    st.dataframe(
        df_tendencia.sort_values('Pos_A').head(15)[['Pos_A', 'SKU', 'DESCRIPCION', 'CANT', 'Status', 'Ingresos_Futuros']], 
        use_container_width=True, 
        hide_index=True
    )

    # --- 12. EXPLORADOR TÁCTICO PROFUNDO ---
    st.divider()
    st.subheader("👟 12. Explorador Táctico por Disciplina")
    
    disciplina_tactica = st.selectbox("Seleccioná disciplina para análisis de stock e ingresos:", sorted(df_ma['DISCIPLINA'].unique()), key="tactico_dis")
    
    df_tactico = df_det[df_det['DISCIPLINA'] == disciplina_tactica].copy()
    
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        st.metric("Venta Mes (SO)", f"{df_tactico['Sell_Out'].sum():,.0f}")
    with col_t2:
        st.metric("Stock en Clientes", f"{df_tactico['Stock_Cli'].sum():,.0f}")
    with col_t3:
        st.metric("Ingresos Pendientes", f"{df_tactico['Ingresos_Futuros'].sum():,.0f}", delta_color="normal")

    st.write(f"Prioridades de reposición para {disciplina_tactica} (Ordenado por Venta):")
    st.dataframe(
        df_tactico.sort_values('Sell_Out', ascending=False),
        use_container_width=True,
        hide_index=True
    )

    # --- 13. RESUMEN DE COBERTURA CRÍTICA ---
    st.divider()
    st.subheader("⚠️ 13. SKUs en Quiebre o Riesgo (Cobertura < 1 mes)")
    
    # Filtramos la tabla de detalle ya calculada en el punto 8
    df_riesgo = df_det[(df_det['Cobertura'] < 1) & (df_det['Sell_Out'] > 0)].copy()
    
    if not df_riesgo.empty:
        st.warning(f"Se detectaron {len(df_riesgo)} SKUs que se quedarán sin stock en menos de 30 días.")
        st.dataframe(
            df_riesgo.sort_values('Sell_Out', ascending=False)[['SKU', 'DESCRIPCION', 'Sell_Out', 'Stock_Cli', 'Cobertura', 'Ingresos_Futuros']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.success("No se detectan SKUs con venta activa en situación crítica de stock.")
