import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# --- 1. CONFIGURACIÓN DE ENTORNO Y UI ---
st.set_page_config(page_title="Dass Torre de Control | High Intelligence", layout="wide", initial_sidebar_state="expanded")

# Estilos CSS de alta densidad para BI
st.markdown("""
    <style>
    .main { background-color: #f8f9fc; }
    .stMetric { background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e1e4e8; }
    [data-testid="stMetricValue"] { color: #0055A4; font-size: 2rem !important; }
    .stDataFrame { border: 1px solid #e1e4e8; border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. DICCIONARIOS MAESTROS Y CONFIGURACIÓN DE NEGOCIO ---
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

# --- 3. MOTOR DE CARGA Y AUDITORÍA DE DRIVE ---
@st.cache_data(ttl=600)
def load_and_verify_data():
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
            # INSTRUCCIÓN: Lectura resiliente con detección automática de separador
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            
            # INSTRUCCIÓN: Normalización profunda de encabezados (quita acentos, ñ, espacios y puntos)
            df.columns = (df.columns.str.strip()
                          .str.normalize('NFKD')
                          .str.encode('ascii', errors='ignore')
                          .str.decode('utf-8')
                          .str.replace('.', '', regex=False)
                          .str.upper())
            
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error crítico en Motor Drive: {e}")
        return {}

data = load_and_verify_data()

if data:
    # --- 4. INSTRUCCIONES DE PROCESAMIENTO: MAESTRO DE PRODUCTOS ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        
        # Mapeo de Atributos Críticos
        columnas_maestras = ['DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'DESCRIPCION', 'GENERO', 'CATEGORIA', 'LINEA']
        for col in columnas_maestras:
            df_maestro[col] = df_maestro.get(col, 'S/D').fillna('S/D').astype(str).str.upper()
        
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 5. INSTRUCCIONES DE LIMPIEZA: BASES TRANSACCIONALES ---
    def clean_transactional(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        # Normalización de SKUs
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Identificación inteligente de columna Cantidad
        c_q = next((c for c in df.columns if any(x in c for x in ['CANT', 'UNID', 'PARES', 'INGRESO', 'VENTA_CANT'])), None)
        df['CANT'] = pd.to_numeric(df[c_q], errors='coerce').fillna(0) if c_q else 0
        
        # Identificación inteligente de columna Fecha
        c_f = next((c for c in df.columns if any(x in c for x in ['FECHA', 'DIA', 'MES', 'PERIODO', 'MOVIMIENTO'])), None)
        if c_f:
            df['FECHA_DT'] = pd.to_datetime(df[c_f], dayfirst=True, errors='coerce')
            df['MES_LLAVE'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        # Identificación de Canal/Cliente
        df['CANAL_CLIENTE'] = df['CLIENTE'].fillna('GENERAL').astype(str).str.upper() if 'CLIENTE' in df.columns else 'GENERAL'
        return df

    df_so_raw = clean_transactional('Sell_out')
    df_si_raw = clean_transactional('Sell_in')
    df_stk_raw = clean_transactional('Stock')
    df_ing_raw = clean_transactional('Ingresos')

    # Snapshot de Stock (Última foto disponible)
    if not df_stk_raw.empty:
        last_stk_date = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == last_stk_date].copy()
    else:
        df_stk_snap = pd.DataFrame()

    # --- 6. SIDEBAR: INSTRUCCIONES DE FILTRADO MULTI-NIVEL ---
    st.sidebar.header("🎛️ Filtros de Inteligencia")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo", "").upper()
    
    meses_dispo = sorted([str(x) for x in df_so_raw['MES_LLAVE'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    target_mes = st.sidebar.selectbox("📅 Mes Base de Análisis", ["Todos"] + meses_dispo)

    with st.sidebar.expander("📂 Filtros de Producto", expanded=True):
        f_emp = st.multiselect("Emprendimiento", sorted(df_maestro['EMPRENDIMIENTO'].unique()))
        f_dis = st.multiselect("Disciplina", sorted(df_maestro['DISCIPLINA'].unique()))
        f_fra = st.multiselect("Franja de Precio", sorted(df_maestro['FRANJA_PRECIO'].unique()))
    
    with st.sidebar.expander("🏢 Filtros de Distribución"):
        canales_totales = sorted(list(set(df_so_raw['CANAL_CLIENTE'].unique()) | set(df_si_raw['CANAL_CLIENTE'].unique())))
        f_cli = st.multiselect("Clientes/Canales", canales_totales)

    # --- 7. INSTRUCCIÓN DE APLICACIÓN DE REGLAS DE FILTRADO ---
    def apply_bi_filters(df, use_date=True):
        if df.empty: return df
        # Enlace con atributos del Maestro
        df = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'DESCRIPCION', 'BUSQUEDA', 'GENERO']], on='SKU', how='left')
        
        if f_emp: df = df[df['EMPRENDIMIENTO'].isin(f_emp)]
        if f_dis: df = df[df['DISCIPLINA'].isin(f_dis)]
        if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
        if f_cli: df = df[df['CANAL_CLIENTE'].isin(f_cli)]
        if search_query: df = df[df['BUSQUEDA'].str.contains(search_query, na=False)]
        if use_date and target_mes != "Todos": df = df[df['MES_LLAVE'] == target_mes]
        return df

    df_so_f = apply_bi_filters(df_so_raw)
    df_si_f = apply_bi_filters(df_si_raw)
    df_stk_f = apply_bi_filters(df_stk_snap, use_date=False)

    # --- 8. DASHBOARD: KPIs Y MIX DE MERCADO ---
    st.title("📊 Torre de Control Operativa: Sell Out & Stock")
    
    # Fila de Métricas Principales
    k1, k2, k3, k4 = st.columns(4)
    vol_so = df_so_f['CANT'].sum()
    vol_stk = df_stk_f['CANT'].sum()
    k1.metric("Venta Sell Out", f"{int(vol_so):,}")
    k2.metric("Stock Central", f"{int(vol_stk):,}")
    k3.metric("Sell In Mes", f"{int(df_si_f['CANT'].sum()):,}")
    k4.metric("MOS Cobertura", f"{round(vol_stk/vol_so, 2) if vol_so > 0 else 0} Meses")

    st.divider()
    
    # Gráficos de Mix (Torta)
    mix1, mix2, mix3 = st.columns(3)
    with mix1:
        st.plotly_chart(px.pie(df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Mix Venta: Disciplina", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with mix2:
        st.plotly_chart(px.pie(df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Mix Venta: Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with mix3:
        st.plotly_chart(px.pie(df_so_f.groupby('EMPRENDIMIENTO')['CANT'].sum().reset_index(), values='CANT', names='EMPRENDIMIENTO', title="Mix Venta: Canal", hole=0.3), use_container_width=True)

    # --- 9. INSTRUCCIÓN DE INTELIGENCIA: RANKING Y SALTO DE PUESTO ---
    st.divider()
    st.header("🏆 Performance de Rankings y Variación")
    ma_col, mb_col = st.columns(2)
    m_a = ma_col.selectbox("Mes Referencia (A)", meses_dispo, index=0)
    m_b = mb_col.selectbox("Mes Comparación (B)", meses_dispo, index=min(1, len(meses_dispo)-1))

    def get_ranking_data(m):
        d = df_so_raw[df_so_raw['MES_LLAVE'] == m].groupby('SKU')['CANT'].sum().reset_index()
        d['Puesto'] = d['CANT'].rank(ascending=False, method='min')
        return d

    rk_a, rk_b = get_ranking_data(m_a), get_ranking_data(m_b)
    df_trend = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(rk_a[['SKU', 'Puesto', 'CANT']], on='SKU', how='inner')
    df_trend = df_trend.merge(rk_b[['SKU', 'Puesto']], on='SKU', how='left', suffixes=('_A', '_B')).fillna(999)
    df_trend['Salto'] = df_trend['Puesto_B'] - df_trend['Puesto_A']
    
    st.subheader(f"Top 15 Performance en {m_a}")
    top15 = df_trend.sort_values('Puesto_A').head(15).copy()
    top15['Variacion'] = top15['Salto'].apply(lambda x: f"⬆️ +{int(x)}" if 0 < x < 500 else (f"⬇️ {int(x)}" if x < 0 else "🆕" if x >= 500 else "➡️"))
    st.dataframe(top15[['Puesto_A', 'SKU', 'DESCRIPCION', 'CANT', 'Variacion']].rename(columns={'Puesto_A': 'Puesto', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)

    # --- 10. INSTRUCCIÓN DE ABASTECIMIENTO: ALERTA MOS ---
    st.divider()
    st.header("🚨 Análisis de Stock vs Demanda (MOS)")
    
    # Stock solo depósito central (DASS) para análisis de quiebre
    stk_dass = df_stk_f[df_stk_f['CANAL_CLIENTE'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Puro')
    df_mos = df_trend.merge(stk_dass, on='SKU', how='left').fillna(0)
    df_mos['MOS'] = (df_mos['Stock_Puro'] / df_mos['CANT']).replace([float('inf')], 0).fillna(0)
    
    def semaforo_bi(r):
        if r['Salto'] > 0 and r['MOS'] < 1 and r['CANT'] > 0: return '🔴 CRÍTICO'
        if r['Salto'] > 0 and r['MOS'] < 2 and r['CANT'] > 0: return '🟡 RIESGO'
        return '🟢 OK'
    
    df_mos['Alerta'] = df_mos.apply(semaforo_bi, axis=1)
    
    
    
    mos_c1, mos_c2 = st.columns([2, 1])
    with mos_c1:
        st.plotly_chart(px.scatter(df_mos[df_mos['CANT'] > 0], x='Salto', y='MOS', size='CANT', color='Alerta', 
                                   hover_name='DESCRIPCION', color_discrete_map={'🔴 CRÍTICO': '#FF4B4B', '🟡 RIESGO': '#FFA500', '🟢 OK': '#28A745'}), use_container_width=True)
    with mos_c2:
        st.write("**Resumen de Cobertura Crítica**")
        st.dataframe(df_mos[df_mos['Alerta'] == '🔴 CRÍTICO'].sort_values('CANT', ascending=False)[['SKU', 'CANT', 'MOS']].head(10), hide_index=True)

    # --- 11. CONSOLIDADO MAESTRO (SOLUCIÓN AL ERROR ATTRIBUTEERROR) ---
    st.divider()
    st.subheader("📋 Consolidado Maestro de Operaciones")
    
    # Agregaciones finales con el método .reset_index() correcto
    res_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SO')
    res_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SI')
    res_stk = df_stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total') # AQUÍ SE CORRIGIÓ EL ERROR
    
    df_final = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'GENERO']].merge(res_so, on='SKU', how='left')
    df_final = df_final.merge(res_si, on='SKU', how='left').merge(res_stk, on='SKU', how='left').fillna(0)
    
    st.dataframe(df_final.sort_values('Venta_SO', ascending=False), use_container_width=True, hide_index=True)
    
    # Botón de Descarga
    csv_out = df_final.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Descargar Reporte Consolidado (CSV)", csv_out, f"reporte_dass_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

else:
    st.info("⚠️ Aguardando sincronización con Google Drive. Asegurese de que los CSV estén en la carpeta correcta.")
