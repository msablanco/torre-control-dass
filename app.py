import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.express as px
import plotly.graph_objects as go

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
    # --- 3. PROCESAMIENTO Y LIMPIEZA ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col in ['DISCIPLINA', 'DESCRIPCION']:
            df_ma[col] = df_ma.get(col, 'S/D').fillna('S/D').astype(str).str.upper()
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

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. SIDEBAR (FILTROS) ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Descripción").upper()
    meses_op = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes Principal", meses_op if meses_op else ["S/D"])
    
    # Filtro de Clientes
    clientes_op = sorted(so_raw['CLIENTE_UP'].unique())
    f_clientes = st.sidebar.multiselect("👤 Filtrar Clientes", clientes_op)

    # Lógica de Filtrado Dinámico
    def apply_filters(df, filter_month=True):
        if df.empty: return df
        temp = df.copy()
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'BUSQUEDA']], on='SKU', how='left')
        if search_query:
            temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if f_clientes:
            temp = temp[temp['CLIENTE_UP'].isin(f_clientes)]
        if filter_month and f_periodo != "S/D":
            temp = temp[temp['MES'] == f_periodo]
        return temp

    so_f = apply_filters(so_raw)
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw[stk_raw['FECHA_DT'] == stk_raw['FECHA_DT'].max()])

    # --- 5. DASHBOARD VISUAL (GRÁFICOS) ---
    st.subheader(f"📊 Análisis de Mix - {f_periodo}")
    c1, c2, c3 = st.columns(3)
    
    with c1:
        if not so_f.empty:
            st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                                   values='CANT', names='DISCIPLINA', title="Mix Sell Out", 
                                   color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        if not stk_f.empty:
            st.plotly_chart(px.pie(stk_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                                   values='CANT', names='DISCIPLINA', title="Mix Stock Actual", 
                                   color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if not si_f.empty:
            df_evol = si_raw.groupby('MES')['CANT'].sum().reset_index()
            st.plotly_chart(px.line(df_evol, x='MES', y='CANT', title="Evolución Sell In (Timeline)"), use_container_width=True)

    # --- 6. RANKINGS E INTELIGENCIA ---
    if len(meses_op) >= 2:
        st.divider()
        st.header("🏆 Rankings y Tendencias")
        m_ant = meses_op[meses_op.index(f_periodo) + 1] if f_periodo in meses_op and meses_op.index(f_periodo)+1 < len(meses_op) else meses_op[-1]
        
        def get_rank(mes):
            return so_raw[so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index().assign(Pos=lambda x: x['CANT'].rank(ascending=False, method='min'))

        rk_a, rk_b = get_rank(f_periodo), get_rank(m_ant)
        df_rank = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
        df_rank = df_rank.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna({'Pos_B': 999})
        df_rank['Salto'] = df_rank['Pos_B'] - df_rank['Pos_A']

        st.subheader(f"Top 10 Productos vs Mes Anterior ({m_ant})")
        st.dataframe(df_rank.sort_values('Pos_A').head(10), use_container_width=True, hide_index=True)

        # --- 7. ALERTA DE QUIEBRE ---
        st.divider()
        st.subheader("🚨 Alerta de Quiebre (MOS)")
        t_stk = stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')
        df_q = df_rank.merge(t_stk, on='SKU', how='left').fillna(0)
        df_q['MOS'] = df_q.apply(lambda x: x['Stock_Total'] / x['CANT'] if x['CANT'] > 0 else 0, axis=1)
        
        df_q['Estado'] = df_q['MOS'].apply(lambda x: '🔴 CRÍTICO' if x < 1 else ('🟡 RIESGO' if x < 2 else '🟢 OK'))
        st.dataframe(df_q[df_q['Estado'] != '🟢 OK'].sort_values('MOS'), use_container_width=True, hide_index=True)

    # --- 8. TABLA DE DETALLE FINAL ---
    st.divider()
    st.subheader("📋 Detalle Completo SKU")
    st.dataframe(apply_filters(so_raw, filter_month=False), use_container_width=True)

else:
    st.error("No se detectaron datos en Drive. Revisa los archivos .csv")
