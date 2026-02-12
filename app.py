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
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']:
            df_ma[col] = df_ma.get(col, 'SIN CATEGORIA').fillna('SIN CATEGORIA').astype(str).str.upper()
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'MES', 'FECHA_DT', 'CLIENTE_UP'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'CANT')
        df['CANT'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MOVIMIENTO'])), 'FECHA')
        df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.upper()
        return df

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 4. FILTROS SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU o Descripción").upper()
    meses_op = sorted(list(set(so_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes Principal", meses_op if meses_op else ["S/D"])
    
    # Filtros multiselect
    disciplinas_op = sorted(df_ma['DISCIPLINA'].unique())
    f_dis = st.sidebar.multiselect("👟 Disciplina", disciplinas_op)
    
    franjas_op = sorted(df_ma['FRANJA_PRECIO'].unique())
    f_fra = st.sidebar.multiselect("💰 Franja de Precio", franjas_op)

    # --- 5. LÓGICA DE FILTRADO ---
    def filtrar(df, por_mes=True):
        if df.empty: return df
        res = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'BUSQUEDA']], on='SKU', how='left')
        if por_mes and f_periodo != "S/D": res = res[res['MES'] == f_periodo]
        if search_query: res = res[res['BUSQUEDA'].str.contains(search_query, na=False)]
        if f_dis: res = res[res['DISCIPLINA'].isin(f_dis)]
        if f_fra: res = res[res['FRANJA_PRECIO'].isin(f_fra)]
        return res

    so_f = filtrar(so_raw)
    si_f = filtrar(si_raw)
    stk_snapshot = stk_raw[stk_raw['FECHA_DT'] == stk_raw['FECHA_DT'].max()]
    stk_f = filtrar(stk_snapshot, por_mes=False)

    # --- 6. KPIs Y MÉTRICAS ---
    st.title("🚀 Torre de Control - Performance & Inteligencia")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out (Pares)", f"{int(so_f['CANT'].sum()):,}")
    k2.metric("Sell In (Pares)", f"{int(si_f['CANT'].sum()):,}")
    k3.metric("Stock Disp.", f"{int(stk_f['CANT'].sum()):,}")
    k4.metric("SKUs Activos", len(so_f['SKU'].unique()))

    # --- 7. DASHBOARD VISUAL (GRÁFICOS) ---
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Mix Sell Out por Disciplina")
        fig_so = px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                        values='CANT', names='DISCIPLINA', color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
        st.plotly_chart(fig_so, use_container_width=True)
    
    with c2:
        st.subheader("Evolución de Ventas (Timeline)")
        df_time = filtrar(so_raw, por_mes=False).groupby('MES')['CANT'].sum().reset_index()
        fig_line = px.line(df_time, x='MES', y='CANT', markers=True, line_shape='spline')
        st.plotly_chart(fig_line, use_container_width=True)

    # --- 8. INTELIGENCIA DE RANKINGS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings")
    if len(meses_op) >= 2:
        m_act = f_periodo
        m_ant = meses_op[meses_op.index(m_act)+1] if meses_op.index(m_act)+1 < len(meses_op) else meses_op[-1]
        
        def get_rank(mes):
            df = so_raw[so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
            df['Pos'] = df['CANT'].rank(ascending=False, method='min')
            return df

        rk_a, rk_b = get_rank(m_act), get_rank(m_ant)
        df_rk = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
        df_rk = df_rk.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Pos_B': 999})
        df_rk['Salto'] = df_rk['Pos_B'] - df_rk['Pos_A']

        st.subheader(f"Top 10 Performance: {m_act} vs {m_ant}")
        st.dataframe(df_rk.sort_values('Pos_A').head(10), use_container_width=True, hide_index=True)

        # --- 9. ALERTA DE QUIEBRE (REPARADA) ---
        st.divider()
        st.subheader("🚨 Alerta de Quiebre (Velocidad vs Cobertura)")
        t_stk = stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')
        df_q = df_rk.merge(t_stk, on='SKU', how='left').fillna(0)
        df_q['MOS'] = df_q.apply(lambda x: x['Stock_Total'] / x['CANT'] if x['CANT'] > 0 else 0, axis=1)
        
        def semaforo(r):
            if r['CANT'] == 0: return '🟢 OK'
            if r['MOS'] < 1.0: return '🔴 CRÍTICO'
            if r['MOS'] < 2.0: return '🟡 ADVERTENCIA'
            return '🟢 OK'
        
        df_q['Estado'] = df_q.apply(semaforo, axis=1)
        df_riesgo = df_q[df_q['Estado'] != '🟢 OK'].sort_values('MOS')
        
        if not df_riesgo.empty:
            st.dataframe(df_riesgo[['Estado', 'SKU', 'DESCRIPCION', 'CANT', 'Stock_Total', 'MOS']], use_container_width=True, hide_index=True)

        st.plotly_chart(px.scatter(df_q[df_q['CANT']>0], x='Salto', y='MOS', size='CANT', color='Estado', 
                                   hover_name='DESCRIPCION', color_discrete_map={'🔴 CRÍTICO': '#FF4B4B', '🟡 ADVERTENCIA': '#FFA500', '🟢 OK': '#28A745'}), 
                        use_container_width=True)

    # --- 10. DETALLE SKU FINAL ---
    st.divider()
    st.subheader("📋 Detalle General")
    st.dataframe(so_f, use_container_width=True, hide_index=True)

else:
    st.error("No se detectaron datos. Revisa la carpeta de Drive.")
