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
    'SPORTSWEAR': '#0055A4', 'HERITAGE': '#00A693', 'TRAINING': '#FF3131', 
    'RUNNING': '#87CEEB', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
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

# --- 3. FLUJO PRINCIPAL DE LA APP ---
if data:
    # Procesamiento Maestro de Productos
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

    # --- 4. SIDEBAR (FILTROS QUE ACTUALIZAN TODO) ---
    st.sidebar.header("🔍 Filtros de Negocio")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Descripción").upper()
    
    meses_op = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes Principal", meses_op if meses_op else ["S/D"])
    
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))
    f_cli_so = st.sidebar.multiselect("👤 Cliente Sell Out", sorted(so_raw['CLIENTE_UP'].unique()))
    f_cli_si = st.sidebar.multiselect("📦 Cliente Sell In", sorted(si_raw['CLIENTE_UP'].unique()))

    # --- 5. MOTOR DE FILTRADO ---
    def apply_filters(df, tipo=None):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        temp = temp[temp['MES'] == f_periodo] # Este filtro actualiza los gráficos marcados
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if tipo == 'SO' and f_cli_so: temp = temp[temp['CLIENTE_UP'].isin(f_cli_so)]
        if tipo == 'SI' and f_cli_si: temp = temp[temp['CLIENTE_UP'].isin(f_cli_si)]
        return temp

    so_f = apply_filters(so_raw, 'SO')
    si_f = apply_filters(si_raw, 'SI')
    stk_f = apply_filters(stk_raw)

    # --- 6. KPIs Y GRÁFICOS (ACTUALIZADOS POR MES) ---
    st.title(f"📊 Dashboard Performance - {f_periodo}")
    
    # KPIs Rápidos
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{int(so_f['CANT'].sum()):,}")
    k2.metric("Sell In", f"{int(si_f['CANT'].sum()):,}")
    stk_d_val = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum()
    k3.metric("Stock Dass", f"{int(stk_d_val):,}")
    stk_c_val = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum()
    k4.metric("Stock Cliente", f"{int(stk_c_val):,}")

    st.divider()
    
    # Los 3 Gráficos de Torta que marcaste en la imagen
    c1, c2, c3 = st.columns(3)
    with c1:
        df_p_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index()
        st.plotly_chart(px.pie(df_p_d, values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        df_p_so = so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index()
        st.plotly_chart(px.pie(df_p_so, values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        # Gráfico Stock Cliente (Actualizado con f_periodo)
        df_p_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index()
        st.plotly_chart(px.pie(df_p_c, values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # --- 7. ALERTA DE QUIEBRE (NAMEERROR REPARADO) ---
    st.divider()
    st.subheader("🚨 Alerta de Quiebre (MOS)")
    
    # Preparamos las tablas de stock para el merge
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Cliente')
    
    df_v = so_f.groupby(['SKU', 'DESCRIPCION'])['CANT'].sum().reset_index(name='Venta_Mes')
    df_alerta = df_v.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
    
    # Mostrar tabla de alertas
    st.dataframe(df_alerta.sort_values('Venta_Mes', ascending=False), use_container_width=True, hide_index=True)

else:
    # Este else está alineado perfectamente con el 'if data:' inicial
    st.error("No se detectaron archivos en Google Drive.")

    # --- 8. RANKINGS Y ERROR FIX (NameError) ---
    st.divider()
    # Definimos las variables que faltaban para evitar el error de la imagen
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Cliente')
    
    # Lógica de Ranking (Simplificada para asegurar funcionamiento)
    df_rank = so_f.groupby(['SKU', 'DESCRIPCION'])['CANT'].sum().reset_index(name='Venta_Actual')
    
    # Unimos todo para la Alerta de Quiebre
    df_alerta = df_rank.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
    
    st.subheader("🚨 Alerta de Quiebre: Velocidad vs Cobertura")
    st.dataframe(df_alerta.sort_values('Venta_Actual', ascending=False).head(10), use_container_width=True)

else:
    st.error("No se pudieron cargar los datos de Drive.")

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





