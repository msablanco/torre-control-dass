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

# --- 2. CARGA DE DATOS DESDE DRIVE ---
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
        st.error(f"Error de conexión con Google Drive: {e}")
        return {}

data = load_data()

if data:
    # --- 3. PROCESAMIENTO DE MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']:
            df_ma[col] = df_ma.get(col, 'SIN CATEGORIA').fillna('SIN CATEGORIA').astype(str).str.upper()
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    # --- 4. LIMPIEZA DE TRANSACCIONALES ---
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

    # --- 5. FILTROS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Descripción").upper()
    
    meses_disponibles = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes Principal de Análisis", meses_disponibles if meses_disponibles else ["S/D"])
    
    disciplinas_op = sorted(df_ma['DISCIPLINA'].unique()) if not df_ma.empty else []
    f_dis = st.sidebar.multiselect("👟 Disciplina", disciplinas_op)
    
    franjas_op = sorted(df_ma['FRANJA_PRECIO'].unique()) if not df_ma.empty else []
    f_fra = st.sidebar.multiselect("💰 Franja de Precio", franjas_op)

    # --- 6. LÓGICA DE FILTRADO DINÁMICO ---
    def aplicar_filtros(df, por_mes=True):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if por_mes and f_periodo != "S/D": temp = temp[temp['MES'] == f_periodo]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        return temp

    so_f = aplicar_filtros(so_raw)
    si_f = aplicar_filtros(si_raw)
    stk_snapshot = stk_raw[stk_raw['FECHA_DT'] == stk_raw['FECHA_DT'].max()]
    stk_f = aplicar_filtros(stk_snapshot, por_mes=False)

    # --- 7. KPIs ---
    st.title("🚀 Torre de Control - Fila Calzado")
    c_kpi1, c_kpi2, c_kpi3, c_kpi4 = st.columns(4)
    c_kpi1.metric("Sell Out (Pares)", f"{int(so_f['CANT'].sum()):,}")
    c_kpi2.metric("Sell In (Pares)", f"{int(si_f['CANT'].sum()):,}")
    c_kpi3.metric("Stock Disp.", f"{int(stk_f['CANT'].sum()):,}")
    c_kpi4.metric("SKUs con Venta", len(so_f['SKU'].unique()))

    # --- 8. DASHBOARD VISUAL ---
    st.divider()
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        st.subheader("Mix Sell Out por Disciplina")
        fig_so = px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                        values='CANT', names='DISCIPLINA', color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
        st.plotly_chart(fig_so, use_container_width=True)

    with col_g2:
        st.subheader("Evolución de Sell In vs Sell Out")
        so_hist = aplicar_filtros(so_raw, por_mes=False).groupby('MES')['CANT'].sum().reset_index()
        si_hist = aplicar_filtros(si_raw, por_mes=False).groupby('MES')['CANT'].sum().reset_index()
        fig_evol = go.Figure()
        fig_evol.add_trace(go.Scatter(x=so_hist['MES'], y=so_hist['CANT'], name='Sell Out', line=dict(color='#0055A4', width=3)))
        fig_evol.add_trace(go.Bar(x=si_hist['MES'], y=si_hist['CANT'], name='Sell In', marker_color='#D3D3D3', opacity=0.5))
        st.plotly_chart(fig_evol, use_container_width=True)

    # --- 9. INTELIGENCIA DE RANKINGS Y TENDENCIAS ---
    if len(meses_disponibles) >= 2:
        st.divider()
        st.header("🏆 Inteligencia de Rankings y Tendencias")
        
        m_actual = f_periodo
        idx_ant = meses_disponibles.index(m_actual) + 1 if meses_disponibles.index(m_actual) + 1 < len(meses_disponibles) else meses_disponibles[-1]
        m_anterior = meses_disponibles[idx_ant]

        def get_ranking(mes):
            df = so_raw[so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
            df['Posicion'] = df['CANT'].rank(ascending=False, method='min')
            return df

        rk_a = get_ranking(m_actual)
        rk_b = get_ranking(m_anterior)

        df_tend = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Posicion', 'CANT']], on='SKU', how='inner')
        df_tend = df_tend.merge(rk_b[['SKU', 'Posicion']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Posicion_B': 999})
        df_tend['Salto'] = df_tend['Posicion_B'] - df_tend['Posicion_A']

        st.subheader(f"Top 10 Performance: {m_actual} vs {m_anterior}")
        top_10 = df_tend.sort_values('Posicion_A').head(10).copy()
        
        def format_salto(v):
            if v > 500: return "🆕 Nuevo"
            return f"⬆️ +{int(v)}" if v > 0 else (f"⬇️ {int(v)}" if v < 0 else "➡️ =")
        
        top_10['Tendencia'] = top_10['Salto'].apply(format_salto)
        st.dataframe(top_10[['Posicion_A', 'SKU', 'DESCRIPCION', 'CANT', 'Tendencia']].rename(columns={'Posicion_A': 'Puesto', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)

        # --- 10. ALERTA DE QUIEBRE (MOS) ---
        st.divider()
        st.subheader(f"🚨 Alerta de Quiebre (Basado en {m_actual})")
        
        t_stk_total = stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')
        df_alerta = df_tend.merge(t_stk_total, on='SKU', how='left').fillna(0)
        df_alerta['MOS_Proyectado'] = df_alerta.apply(lambda r: r['Stock_Total'] / r['CANT'] if r['CANT'] > 0 else 0, axis=1)

        def definir_semaforo(row):
            if row['CANT'] == 0: return '🟢 OK: Sin Venta'
            if row['MOS_Proyectado'] < 1.0: return '🔴 CRÍTICO: < 1 Mes'
            if row['MOS_Proyectado'] < 2.0: return '🟡 ADVERTENCIA: < 2 Meses'
            return '🟢 OK: Stock Suficiente'

        df_alerta['Estado'] = df_alerta.apply(definir_semaforo, axis=1)
        df_riesgo = df_alerta[df_alerta['Estado'].str.contains('🔴|🟡')].sort_values(['Salto', 'MOS_Proyectado'], ascending=[False, True])

        if not df_riesgo.empty:
            st.warning(f"Se detectaron {len(df_riesgo)} SKUs en riesgo.")
            st.dataframe(df_riesgo[['Estado', 'SKU', 'DESCRIPCION', 'CANT', 'Stock_Total', 'MOS_Proyectado']].rename(columns={'CANT': 'Venta Mes', 'MOS_Proyectado': 'Meses Cobertura'}), use_container_width=True, hide_index=True)
            # Botón de descarga
            csv = df_riesgo.to_csv(index=False).encode('utf-8')
            st.download_button(label="📥 Descargar Lista de Reposición", data=csv, file_name=f'reposicion_{m_actual}.csv', mime='text/csv')

        st.plotly_chart(px.scatter(df_alerta[df_alerta['CANT'] > 0], x='Salto', y='MOS_Proyectado', size='CANT', color='Estado', hover_name='DESCRIPCION',
                                   title="Velocidad (Salto) vs Cobertura (MOS)",
                                   color_discrete_map={'🔴 CRÍTICO: < 1 Mes': '#FF4B4B', '🟡 ADVERTENCIA: < 2 Meses': '#FFA500', '🟢 OK: Stock Suficiente': '#28A745', '🟢 OK: Sin Venta': '#28A745'}), 
                        use_container_width=True)

    # --- 11. DETALLE GENERAL ---
    st.divider()
    st.subheader("📋 Detalle General de Movimientos")
    st.dataframe(so_f, use_container_width=True, hide_index=True)

else:
    st.error("No se detectaron archivos válidos en la carpeta de Drive configurada.")
