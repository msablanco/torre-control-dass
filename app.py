import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# --- 1. CONFIGURACIÓN DE ENTORNO Y ESTILOS ---
st.set_page_config(page_title="Performance & BI - Dass Calzado", layout="wide")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 24px; color: #0055A4; }
    .main { background-color: #f8f9fa; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DICCIONARIOS DE IDENTIDAD VISUAL (ESTABLECIDOS) ---
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

# --- 3. INSTRUCCIONES DE CONEXIÓN (GOOGLE DRIVE) ---
@st.cache_data(ttl=600)
def fetch_drive_data():
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
        
        if not items: return {}
            
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
            # Normalización de cabeceras (Instrucción vital)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Crítico Drive: {e}")
        return {}

data = fetch_drive_data()

if data:
    # --- 4. INSTRUCCIONES DE NORMALIZACIÓN DEL MAESTRO ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        cols_m = ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO', 'GENERO', 'CATEGORIA']
        for c in cols_m:
            df_maestro[c] = df_maestro.get(c, 'SIN DATOS').fillna('SIN DATOS').astype(str).str.upper()
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 5. INSTRUCCIONES DE LIMPIEZA TRANSACCIONAL ---
    def clean_trans(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Lógica de Volumen
        c_q = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS', 'PARES'])), None)
        df['CANT'] = pd.to_numeric(df[c_q], errors='coerce').fillna(0) if c_q else 0
        
        # Lógica Temporal
        c_f = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MOVIMIENTO', 'DIA'])), None)
        if c_f:
            df['FECHA_DT'] = pd.to_datetime(df[c_f], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        df['CLIENTE_UP'] = df['CLIENTE'].fillna('DESCONOCIDO').astype(str).str.upper() if 'CLIENTE' in df.columns else 'DESCONOCIDO'
        return df

    df_so_raw = clean_trans('Sell_out')
    df_si_raw = clean_trans('Sell_in')
    df_stk_raw = clean_trans('Stock')

    # Gestión de Stock (Snapshot)
    if not df_stk_raw.empty:
        last_date = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == last_date].copy()
    else:
        df_stk_snap = pd.DataFrame()

    # --- 6. SIDEBAR: INSTRUCCIONES DE FILTRADO MULTI-NIVEL ---
    st.sidebar.header("🎛️ Filtros de Operación")
    q = st.sidebar.text_input("🔍 Buscar Modelo/SKU", "").upper()
    
    meses = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    target_mes = st.sidebar.selectbox("📅 Período Base", ["Todos"] + meses)

    with st.sidebar.expander("📂 Atributos de Producto"):
        f_emp = st.multiselect("Emprendimiento", sorted(df_maestro['EMPRENDIMIENTO'].unique()))
        f_dis = st.multiselect("Disciplina", sorted(df_maestro['DISCIPLINA'].unique()))
        f_fra = st.multiselect("Franja de Precio", sorted(df_maestro['FRANJA_PRECIO'].unique()))

    with st.sidebar.expander("🏢 Canales"):
        cli_all = sorted(list(set(df_so_raw['CLIENTE_UP'].unique()) | set(df_si_raw['CLIENTE_UP'].unique())))
        f_cli = st.multiselect("Seleccionar Cliente", cli_all)

    def apply_f(df, use_date=True):
        if df.empty: return df
        df = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if f_emp: df = df[df['EMPRENDIMIENTO'].isin(f_emp)]
        if f_dis: df = df[df['DISCIPLINA'].isin(f_dis)]
        if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
        if f_cli: df = df[df['CLIENTE_UP'].isin(f_cli)]
        if q: df = df[df['BUSQUEDA'].str.contains(q, na=False)]
        if use_date and target_mes != "Todos": df = df[df['MES'] == target_mes]
        return df

    df_so_f = apply_f(df_so_raw)
    df_stk_f = apply_f(df_stk_snap, use_date=False)

    # --- 7. DASHBOARD: KPIs Y MIXES DE TORTA ---
    st.title("🚀 Dass Intelligence: Sell Out & Abastecimiento")
    
    # Métricas Flash
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Pares Vendidos", f"{int(df_so_f['CANT'].sum()):,}")
    m2.metric("Stock Actual", f"{int(df_stk_f['CANT'].sum()):,}")
    m3.metric("SKUs Activos", len(df_so_f['SKU'].unique()))
    m4.metric("Cobertura Promedio", f"{round(df_stk_f['CANT'].sum()/df_so_f['CANT'].sum(), 1) if df_so_f['CANT'].sum()>0 else 0} Meses")

    st.divider()
    
    # Gráficos de Torta (Disciplina y Franja)
    c_t1, c_t2, c_t3 = st.columns(3)
    with c_t1:
        st.plotly_chart(px.pie(df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Mix Venta: Disciplina", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c_t2:
        st.plotly_chart(px.pie(df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Mix Venta: Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with c_t3:
        # Gráfico por Emprendimiento (Wholesale, Retail, Ecom)
        st.plotly_chart(px.pie(df_so_f.groupby('EMPRENDIMIENTO')['CANT'].sum().reset_index(), values='CANT', names='EMPRENDIMIENTO', title="Mix Venta: Canal/Emprendimiento", hole=0.4), use_container_width=True)

    # --- 8. INSTRUCCIONES DE RANKING Y TENDENCIAS ---
    st.divider()
    st.header("🏆 Análisis de Competitividad Interna")
    rk_c1, rk_c2 = st.columns(2)
    m_a = rk_c1.selectbox("Mes Actual (A)", meses, index=0)
    m_b = rk_c2.selectbox("Mes Anterior (B)", meses, index=min(1, len(meses)-1))

    def get_rk_data(m):
        d = df_so_raw[df_so_raw['MES'] == m].groupby('SKU')['CANT'].sum().reset_index()
        d['Puesto'] = d['CANT'].rank(ascending=False, method='min')
        return d

    rk_a, rk_b = get_rk_data(m_a), get_rk_data(m_b)
    df_trend = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(rk_a[['SKU', 'Puesto', 'CANT']], on='SKU', how='inner')
    df_trend = df_trend.merge(rk_b[['SKU', 'Puesto']], on='SKU', how='left', suffixes=('_A', '_B')).fillna(999)
    df_trend['Salto'] = df_trend['Puesto_B'] - df_trend['Puesto_A']

    st.subheader(f"Top 15 Productos en {m_a}")
    top15 = df_trend.sort_values('Puesto_A').head(15).copy()
    top15['Status'] = top15['Salto'].apply(lambda x: f"⬆️ +{int(x)}" if 0 < x < 500 else (f"⬇️ {int(x)}" if x < 0 else "🆕" if x >= 500 else "➡️"))
    st.dataframe(top15[['Puesto_A', 'SKU', 'DESCRIPCION', 'DISCIPLINA', 'CANT', 'Status']], use_container_width=True, hide_index=True)

    # --- 9. INSTRUCCIONES DE COBERTURA (MOS) Y QUUEBRES ---
    st.divider()
    st.header("🚨 Inteligencia de Stock (MOS)")
    
    # Cruzar con stock disponible en DASS (Depósito Central)
    stk_dass = df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    df_mos = df_trend.merge(stk_dass, on='SKU', how='left').fillna(0)
    df_mos['MOS'] = (df_mos['Stock_Dass'] / df_mos['CANT']).replace([float('inf')], 0).fillna(0)
    
    def semaforo(r):
        if r['Salto'] > 0 and r['MOS'] < 1 and r['CANT'] > 0: return '🔴 CRÍTICO'
        if r['Salto'] > 0 and r['MOS'] < 2 and r['CANT'] > 0: return '🟡 RIESGO'
        return '🟢 OK'
    
    df_mos['Alerta'] = df_mos.apply(semaforo, axis=1)
    
    mos_col1, mos_col2 = st.columns([2, 1])
    with mos_col1:
        st.plotly_chart(px.scatter(df_mos[df_mos['CANT']>0], x='Salto', y='MOS', size='CANT', color='Alerta', 
                                   hover_name='DESCRIPCION', title="Salto de Puesto vs Meses de Stock",
                                   color_discrete_map={'🔴 CRÍTICO': '#FF4B4B', '🟡 RIESGO': '#FFA500', '🟢 OK': '#28A745'}), use_container_width=True)
    with mos_col2:
        st.info("💡 **Interpretación:** Los productos en la zona roja son aquellos que subieron en el ranking de ventas pero tienen menos de 1 mes de stock.")
        st.dataframe(df_mos[df_mos['Alerta'] == '🔴 CRÍTICO'].sort_values('CANT', ascending=False)[['SKU', 'CANT', 'MOS']], hide_index=True)

    # --- 10. CONSOLIDADO MAESTRO Y EXPORTACIÓN ---
    st.divider()
    st.subheader("📋 Consolidado Maestro para Pedidos")
    
    # Agrupaciones finales (Resolviendo el error previo de sintaxis)
    final_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SO')
    final_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SI')
    final_stk = df_stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')
    
    df_final = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'GENERO']].merge(final_so, on='SKU', how='left')
    df_final = df_final.merge(final_si, on='SKU', how='left').merge(final_stk, on='SKU', how='left').fillna(0)
    
    st.dataframe(df_final.sort_values('Venta_SO', ascending=False), use_container_width=True, hide_index=True)
    
    # Función de exportación
    csv_data = df_final.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Descargar Tabla Maestra (CSV)", csv_data, f"bi_dass_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

else:
    st.warning("⚠️ Esperando carga de archivos CSV desde el Drive para iniciar el procesamiento.")
