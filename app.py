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
    # --- 3. PROCESAMIENTO INICIAL ---
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

    # --- 4. FUNCIÓN STOCK POR PERIODO ---
    def get_stk_por_periodo(df_stk, periodo_seleccionado):
        if df_stk.empty: return df_stk
        if periodo_seleccionado != "Todos":
            df_p = df_stk[df_stk['MES'] == periodo_seleccionado].copy()
            if not df_p.empty:
                max_d = df_p['FECHA_DT'].max()
                return df_p[df_p['FECHA_DT'] == max_d]
        max_h = df_stk['FECHA_DT'].max()
        return df_stk[df_stk['FECHA_DT'] == max_h]

    # --- 5. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    meses_op = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_op)
    
    opts_dis = sorted(df_ma['DISCIPLINA'].unique()) if not df_ma.empty else []
    f_dis = st.sidebar.multiselect("👟 Disciplinas", opts_dis)
    
    f_cli_so = st.sidebar.multiselect("👤 Cliente SO", sorted(so_raw['CLIENTE_UP'].unique()) if not so_raw.empty else [])
    selected_clients = set(f_cli_so)

    # --- 6. LÓGICA DE FILTRADO ---
    stk_snapshot = get_stk_por_periodo(stk_raw, f_periodo)

    def apply_logic(df, filter_month=True, is_stock=False):
        if df.empty: return df
        temp = df.copy()
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if search_query:
            temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if not is_stock and filter_month and f_periodo != "Todos":
            temp = temp[temp['MES'] == f_periodo]
        if selected_clients:
            temp = temp[temp['CLIENTE_UP'].isin(selected_clients)]
        return temp

    so_f = apply_logic(so_raw)
    si_f = apply_logic(si_raw)
    stk_f = apply_logic(stk_snapshot, is_stock=True)

    val_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum()
    val_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum()

    # --- 7. DASHBOARD VISUAL ---
    st.divider()
    st.subheader("📌 Análisis de Mix y Stock")
    c1, c2, c3 = st.columns(3)
    if val_d > 0:
        c1.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not so_f.empty:
        c2.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if val_c > 0:
        c3.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # --- 10. DETALLE SKU ---
    st.divider()
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(t_so, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').merge(t_stk_d, on='SKU', how='left').fillna(0)
    st.subheader("📋 Detalle por SKU")
    st.dataframe(df_final[df_final['Sell Out'] > 0].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

    # --- 11. RANKINGS & ALERTA ---
    if len(meses_op) >= 2:
        st.divider()
        m_actual = f_periodo if f_periodo != "Todos" else meses_op[0]
        idx_ant = meses_op.index(m_actual) + 1 if (m_actual in meses_op and meses_op.index(m_actual)+1 < len(meses_op)) else 0
        m_anterior = meses_op[idx_ant]

        rk_a = so_raw[so_raw['MES'] == m_actual].groupby('SKU')['CANT'].sum().reset_index()
        rk_b = so_raw[so_raw['MES'] == m_anterior].groupby('SKU')['CANT'].sum().reset_index()
        rk_a['Pos'] = rk_a['CANT'].rank(ascending=False, method='min')
        rk_b['Pos'] = rk_b['CANT'].rank(ascending=False, method='min')

        df_rank = df_ma[['SKU', 'DESCRIPCION']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
        df_rank = df_rank.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Pos_B': 999})
        df_rank['Salto'] = df_rank['Pos_B'] - df_rank['Pos_A']

        st.subheader(f"🚨 Alerta de Quiebre (Basado en {m_actual})")
        df_alerta = df_rank.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
        df_alerta['Stock_Total'] = df_alerta['Stock Dass'] + df_alerta['Stock Cliente']
        df_alerta['MOS'] = df_alerta.apply(lambda x: x['Stock_Total']/x['CANT'] if x['CANT'] > 0 else 0, axis=1)

        def semaforo(r):
            if r['CANT'] == 0: return '🟢 OK'
            return '🔴 CRÍTICO' if r['MOS'] < 1 else ('🟡 ALERTA' if r['MOS'] < 2 else '🟢 OK')
        
        df_alerta['Estado'] = df_alerta.apply(semaforo, axis=1)
        st.dataframe(df_alerta[df_alerta['Estado'] != '🟢 OK'].sort_values('MOS'), use_container_width=True)

else:
    st.error("No se detectaron archivos en Google Drive.")

    # --- 8. ANÁLISIS POR FRANJA ---
    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    
    if val_d > 0:
        f1.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                               values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    if not so_f.empty:
        f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                               values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    if val_c > 0:
        f3.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                               values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    
    if not si_f.empty:
        df_bar_fra = si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index()
        fig_bar_fra = px.bar(df_bar_fra, x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In (Mix)", color_discrete_map=COLOR_MAP_FRA, text_auto='.2s')
        f4.plotly_chart(fig_bar_fra, use_container_width=True)

    # --- 9. EVOLUCIÓN HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica Comparativa")
    h_so = apply_logic(so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
    h_si = apply_logic(si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell In')
    
    df_h = h_so.merge(h_si, on='MES', how='outer').fillna(0).sort_values('MES')
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    st.plotly_chart(fig_h, use_container_width=True)

# --- 10. DETALLE SKU ---
    st.divider()
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(t_so, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').merge(t_stk_d, on='SKU', how='left').fillna(0)
    st.subheader("📋 Detalle por SKU")
    st.dataframe(df_final[df_final['Sell Out'] > 0].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

    # --- 11. RANKINGS & ALERTA ---
    if len(meses_op) >= 2:
        st.divider()
        m_actual = f_periodo if f_periodo != "Todos" else meses_op[0]
        idx_ant = meses_op.index(m_actual) + 1 if (m_actual in meses_op and meses_op.index(m_actual)+1 < len(meses_op)) else 0
        m_anterior = meses_op[idx_ant]

        rk_a = so_raw[so_raw['MES'] == m_actual].groupby('SKU')['CANT'].sum().reset_index()
        rk_b = so_raw[so_raw['MES'] == m_anterior].groupby('SKU')['CANT'].sum().reset_index()
        rk_a['Pos'] = rk_a['CANT'].rank(ascending=False, method='min')
        rk_b['Pos'] = rk_b['CANT'].rank(ascending=False, method='min')

        df_rank = df_ma[['SKU', 'DESCRIPCION']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
        df_rank = df_rank.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Pos_B': 999})
        df_rank['Salto'] = df_rank['Pos_B'] - df_rank['Pos_A']

        st.subheader(f"🚨 Alerta de Quiebre (Basado en {m_actual})")
        df_alerta = df_rank.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
        df_alerta['Stock_Total'] = df_alerta['Stock Dass'] + df_alerta['Stock Cliente']
        df_alerta['MOS'] = df_alerta.apply(lambda x: x['Stock_Total']/x['CANT'] if x['CANT'] > 0 else 0, axis=1)

        def semaforo(r):
            if r['CANT'] == 0: return '🟢 OK'
            return '🔴 CRÍTICO' if r['MOS'] < 1 else ('🟡 ALERTA' if r['MOS'] < 2 else '🟢 OK')
        
        df_alerta['Estado'] = df_alerta.apply(semaforo, axis=1)
        st.dataframe(df_alerta[df_alerta['Estado'] != '🟢 OK'].sort_values('MOS'), use_container_width=True)

else:
    st.error("No se detectaron archivos en Google Drive.")
 # --- 12. RANKING DE PRODUCTOS Y TENDENCIAS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings y Tendencias")
    
    col_sel1, col_sel2 = st.columns(2)
    with col_sel1:
        mes_actual = st.selectbox("Mes de Comparación (A)", meses_disponibles, index=0, key='ma')
    with col_sel2:
        mes_anterior = st.selectbox("Mes Base (B)", meses_disponibles, index=min(1, len(meses_disponibles)-1), key='mb')

    # Lógica de Ranking
    def obtener_ranking(mes):
        df_mes = df_so_raw[df_so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df_mes['Posicion'] = df_mes['CANT'].rank(ascending=False, method='min')
        return df_mes

    rk_a = obtener_ranking(mes_actual)
    rk_b = obtener_ranking(mes_anterior)

    df_tendencia = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Posicion', 'CANT']], on='SKU', how='inner')
    df_tendencia = df_tendencia.merge(rk_b[['SKU', 'Posicion']], on='SKU', how='left', suffixes=('_A', '_B'))
    df_tendencia['Posicion_B'] = df_tendencia['Posicion_B'].fillna(999) # Si no existía, puesto 999
    df_tendencia['Salto'] = df_tendencia['Posicion_B'] - df_tendencia['Posicion_A']

    # Visualización Ranking Top 10
    st.subheader(f"Top 10 Productos con Mayor Venta en {mes_actual}")
    top_10 = df_tendencia.sort_values('Posicion_A').head(10).copy()
    
    def format_salto(val):
        if val > 500: return "🆕 Nuevo"
        if val > 0: return f"⬆️ +{int(val)}"
        if val < 0: return f"⬇️ {int(val)}"
        return "➡️ ="

    top_10['Tendencia'] = top_10['Salto'].apply(format_salto)
    st.dataframe(top_10[['Posicion_A', 'SKU', 'DESCRIPCION', 'CANT', 'Tendencia']].rename(columns={'Posicion_A': 'Puesto', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)
        
        # --- 13. ALERTA DE QUIEBRE REPARADA ---
        st.divider()
        st.subheader(f"🚨 Alerta de Quiebre (Basado en {m_actual})")
        
        # Unimos Rankings con Stock Recalculado
        df_alerta = df_rank.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
        df_alerta['Stock_Total'] = df_alerta['Stock Dass'] + df_alerta['Stock Cliente']
        
        # Evitar división por cero en MOS
        df_alerta['MOS_Proyectado'] = df_alerta.apply(lambda r: r['Stock_Total'] / r['CANT'] if r['CANT'] > 0 else 0, axis=1)

        def definir_semaforo(row):
            if row['CANT'] == 0: return '🟢 OK: Sin Venta'
            if row['MOS_Proyectado'] < 1.0: return '🔴 CRÍTICO: < 1 Mes'
            if row['MOS_Proyectado'] < 2.0: return '🟡 ADVERTENCIA: < 2 Meses'
            return '🟢 OK: Stock Suficiente'

        df_alerta['Estado'] = df_alerta.apply(definir_semaforo, axis=1)
        df_riesgo = df_alerta[df_alerta['Estado'].str.contains('🔴|🟡')].sort_values('MOS_Proyectado')

        if not df_riesgo.empty:
            st.warning(f"Se detectaron {len(df_riesgo)} SKUs con stock insuficiente para la velocidad de venta de {m_actual}.")
            st.dataframe(df_riesgo[['Estado', 'SKU', 'DESCRIPCION', 'CANT', 'Stock_Total', 'MOS_Proyectado']]
                         .rename(columns={'CANT': 'Venta Mes', 'Stock_Total': 'Stock Disp.', 'MOS_Proyectado': 'Meses Cobertura'}), 
                         use_container_width=True, hide_index=True)
        
        # Mapa Visual de Quiebre
        fig_mos = px.scatter(df_alerta[df_alerta['CANT'] > 0], x='Salto', y='MOS_Proyectado', 
                             size='CANT', color='Estado', hover_name='DESCRIPCION',
                             title="Velocidad (Salto Ranking) vs Cobertura (MOS)",
                             color_discrete_map={'🔴 CRÍTICO: < 1 Mes': '#ff4b4b', '🟡 ADVERTENCIA: < 2 Meses': '#ffa500', '🟢 OK: Stock Suficiente': '#28a745', '🟢 OK: Sin Venta': '#28a745'})
        st.plotly_chart(fig_mos, use_container_width=True)




















