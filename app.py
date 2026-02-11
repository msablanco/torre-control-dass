import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Performance & Inteligencia => Dass Calzado", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL (COLORES) ---
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
def load_data_from_drive():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'",
            fields="files(id, name)"
        ).execute()
        items = results.get('files', [])
        
        dfs = {}
        for item in items:
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            file_name = item['name'].replace('.csv', '')
            dfs[file_name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_data_from_drive()

if data:
    # --- 3. PROCESAMIENTO MAESTRO ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO']:
            df_maestro[col] = df_maestro.get(col, 'SIN CATEGORIA').fillna('SIN CATEGORIA').astype(str).str.upper()
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 4. LIMPIEZA TRANSACCIONAL ---
    def limpiar_transaccional(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS'])), None)
        df['CANT'] = pd.to_numeric(df[col_cant], errors='coerce').fillna(0) if col_cant else 0
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), None)
        if col_fecha:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        df['CLIENTE_UP'] = df['CLIENTE'].fillna('S/D').astype(str).str.upper() if 'CLIENTE' in df.columns else 'S/D'
        return df

    df_so_raw = limpiar_transaccional('Sell_out')
    df_si_raw = limpiar_transaccional('Sell_in')
    df_stk_raw = limpiar_transaccional('Stock')
    df_ing_raw = limpiar_transaccional('Ingresos')

    # Snapshot Stock
    if not df_stk_raw.empty:
        max_fecha = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == max_fecha].copy()
        df_stk_snap = df_stk_snap.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO']], on='SKU', how='left')
    else:
        df_stk_snap = pd.DataFrame()

    # --- 5. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo", "").upper()
    meses_dis = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    mes_filtro = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + meses_dis)

    f_emprendimiento = st.sidebar.multiselect("🏬 Emprendimiento (Canal)", sorted(df_maestro['EMPRENDIMIENTO'].unique()))
    f_disciplina = st.sidebar.multiselect("👟 Disciplina", sorted(df_maestro['DISCIPLINA'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_maestro['FRANJA_PRECIO'].unique()))
    f_clientes = st.sidebar.multiselect("👤 Clientes", sorted(list(set(df_so_raw['CLIENTE_UP'].unique()) | set(df_si_raw['CLIENTE_UP'].unique()))))

    def filtrar_df(df, filtrar_mes=True):
        if df.empty: return df
        temp = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO', 'BUSQUEDA']], on='SKU', how='left')
        if f_emprendimiento: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emprendimiento)]
        if f_disciplina: temp = temp[temp['DISCIPLINA'].isin(f_disciplina)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if f_clientes: temp = temp[temp['CLIENTE_UP'].isin(f_clientes)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if filtrar_mes and mes_filtro != "Todos": temp = temp[temp['MES'] == mes_filtro]
        return temp

    df_so_f = filtrar_df(df_so_raw)
    df_si_f = filtrar_df(df_si_raw)
    df_stk_f = filtrar_df(df_stk_snap, filtrar_mes=False)

    # --- 6. VISUALIZACIÓN ---
    st.title("📊 Torre de Control: Sell Out & Abastecimiento")
    
    # MIX DISCIPLINA Y EVOLUCIÓN
    st.subheader("👟 Mix por Disciplina y Flujo")
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        st.plotly_chart(px.pie(df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Mix Venta", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        st.plotly_chart(px.pie(df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Mix Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        ev_so = filtrar_df(df_so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SO')
        ev_si = filtrar_df(df_si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SI')
        ev_m = ev_so.merge(ev_si, on='MES', how='outer').fillna(0).sort_values('MES')
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ev_m['MES'], y=ev_m['SO'], name='Sell Out', line=dict(color='#0055A4', width=4)))
        fig.add_trace(go.Scatter(x=ev_m['MES'], y=ev_m['SI'], name='Sell In', line=dict(color='#FF3131', width=3)))
        st.plotly_chart(fig, use_container_width=True)

    # MIX FRANJA DE PRECIO
    st.divider()
    st.subheader("💰 Análisis por Franja de Precio")
    cf1, cf2, cf3 = st.columns(3)
    with cf1:
        st.plotly_chart(px.pie(df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Mix Venta/Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with cf2:
        st.plotly_chart(px.pie(df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Mix Stock/Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with cf3:
        st.plotly_chart(px.bar(df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index().sort_values('CANT'), x='CANT', y='FRANJA_PRECIO', orientation='h', title="Volumen Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)

    # --- 7. RANKINGS Y TENDENCIAS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings y Tendencias")
    ca, cb = st.columns(2)
    m_act = ca.selectbox("Mes A", meses_dis, index=0)
    m_ant = cb.selectbox("Mes B", meses_dis, index=min(1, len(meses_dis)-1))

    def get_rk(m):
        df = df_so_raw[df_so_raw['MES'] == m].groupby('SKU')['CANT'].sum().reset_index()
        df['Pos'] = df['CANT'].rank(ascending=False, method='min')
        return df

    rk_a, rk_b = get_rk(m_act), get_rk(m_ant)
    df_trend = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
    df_trend = df_trend.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna(999)
    df_trend['Salto'] = df_trend['Pos_B'] - df_trend['Pos_A']
    
    st.subheader(f"Top 10 en {m_act}")
    top10 = df_trend.sort_values('Pos_A').head(10).copy()
    top10['Tendencia'] = top10['Salto'].apply(lambda x: f"⬆️ +{int(x)}" if x > 0 and x < 500 else (f"⬇️ {int(x)}" if x < 0 else "🆕 Nuevo" if x >= 500 else "➡️ ="))
    st.dataframe(top10[['Pos_A', 'SKU', 'DESCRIPCION', 'CANT', 'Tendencia']], use_container_width=True, hide_index=True)

    # --- 8. ALERTAS MOS ---
    st.divider()
    st.header("🚨 Alerta de Cobertura (MOS)")
    stk_d = df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stk')
    df_al = df_trend.merge(stk_d, on='SKU', how='left').fillna(0)
    df_al['MOS'] = (df_al['Stk'] / df_al['CANT']).replace([float('inf'), -float('inf')], 0).fillna(0)
    
    def semaforo(r):
        if r['Salto'] > 0 and r['MOS'] < 1 and r['CANT'] > 0: return '🔴 CRÍTICO'
        if r['Salto'] > 0 and r['MOS'] < 2 and r['CANT'] > 0: return '🟡 RIESGO'
        return '🟢 OK'
    
    df_al['Estado'] = df_al.apply(semaforo, axis=1)
    st.plotly_chart(px.scatter(df_al[df_al['CANT']>0], x='Salto', y='MOS', size='CANT', color='Estado', hover_name='DESCRIPCION', color_discrete_map={'🔴 CRÍTICO': '#ff4b4b', '🟡 RIESGO': '#ffa500', '🟢 OK': '#28a745'}), use_container_width=True)

    st.subheader("Lista de Reposición Crítica")
    st.dataframe(df_al[df_al['Estado'] != '🟢 OK'].sort_values('Salto', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("Error al cargar datos desde Drive.")
