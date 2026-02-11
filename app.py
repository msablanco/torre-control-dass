import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# --- 1. CONFIGURACIÓN DE PÁGINA Y ESTILOS ---
st.set_page_config(page_title="Performance & Inteligencia => Dass Calzado", layout="wide")

st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- 2. MAPAS DE COLORES (ESTABLECIDOS) ---
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

# --- 3. MOTOR DE CARGA DESDE GOOGLE DRIVE ---
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
        
        if not items:
            st.error("No se encontraron archivos en la carpeta especificada.")
            return {}
            
        dfs = {}
        for item in items:
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            fh.seek(0)
            # Detectar encoding y separador automáticamente
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            
            # Normalización estricta de columnas
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            
            file_name = item['name'].replace('.csv', '')
            dfs[file_name] = df
        return dfs
    except Exception as e:
        st.error(f"Error crítico de conexión: {e}")
        return {}

data = load_data_from_drive()

if data:
    # --- 4. PROCESAMIENTO DEL MAESTRO DE PRODUCTOS ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        
        columnas_claves = ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO']
        for col in columnas_claves:
            df_maestro[col] = df_maestro.get(col, 'SIN CATEGORIA').fillna('SIN CATEGORIA').astype(str).str.upper()
        
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 5. LIMPIEZA DE BASES TRANSACCIONALES ---
    def limpiar_base(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Identificar columna de cantidad (Ventas, Stock, Ingresos)
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS', 'CANT. VENTA'])), None)
        df['CANT'] = pd.to_numeric(df[col_cant], errors='coerce').fillna(0) if col_cant else 0
        
        # Identificar columna de fecha
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO', 'DIA'])), None)
        if col_fecha:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        # Cliente / Canal
        df['CLIENTE_UP'] = df['CLIENTE'].fillna('S/D').astype(str).str.upper() if 'CLIENTE' in df.columns else 'S/D'
        return df

    df_so_raw = limpiar_base('Sell_out')
    df_si_raw = limpiar_base('Sell_in')
    df_stk_raw = limpiar_base('Stock')
    df_ing_raw = limpiar_base('Ingresos')

    # Snapshot de Stock Actual (Última fecha disponible)
    if not df_stk_raw.empty:
        max_f_stk = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == max_f_stk].copy()
    else:
        df_stk_snap = pd.DataFrame()

    # --- 6. INTERFAZ DE FILTROS AVANZADOS (SIDEBAR) ---
    st.sidebar.header("🔍 Panel de Control")
    search_query = st.sidebar.text_input("🎯 Búsqueda de SKU o Modelo", "").upper()
    
    meses_lista = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    mes_filtro = st.sidebar.selectbox("📅 Mes Base de Análisis", ["Todos"] + meses_lista, index=0)

    st.sidebar.subheader("Segmentación")
    f_emp = st.sidebar.multiselect("🏬 Emprendimiento", sorted(df_maestro['EMPRENDIMIENTO'].unique()))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_maestro['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_maestro['FRANJA_PRECIO'].unique()))
    
    clientes_mix = sorted(list(set(df_so_raw['CLIENTE_UP'].unique()) | set(df_si_raw['CLIENTE_UP'].unique())))
    f_cli = st.sidebar.multiselect("👤 Canales / Clientes", clientes_mix)

    # --- 7. LÓGICA DE FILTRADO CRUZADO ---
    def aplicar_filtros(df, filtrar_mes=True):
        if df.empty: return df
        # Join con maestro para obtener atributos
        temp = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        
        if f_emp: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_cli: temp = temp[temp['CLIENTE_UP'].isin(f_cli)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if filtrar_mes and mes_filtro != "Todos": temp = temp[temp['MES'] == mes_filtro]
        return temp

    df_so_f = aplicar_filtros(df_so_raw)
    df_si_f = aplicar_filtros(df_si_raw)
    df_stk_f = aplicar_filtros(df_stk_snap, filtrar_mes=False)

    # --- 8. DASHBOARD: KPIs Y MIXES ---
    st.title("📊 Torre de Control Operativa: Sell Out & Stock")
    
    # Fila 1: Mix Disciplina y Franja
    st.subheader("Análisis de Mix de Mercado")
    mix_c1, mix_c2, mix_c3 = st.columns([1, 1, 1])
    
    with mix_c1:
        if not df_so_f.empty:
            fig_dis = px.pie(df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Venta por Disciplina", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
            st.plotly_chart(fig_dis, use_container_width=True)
            
    with mix_c2:
        if not df_so_f.empty:
            fig_fra = px.pie(df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Venta por Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA)
            st.plotly_chart(fig_fra, use_container_width=True)

    with mix_c3:
        if not df_stk_f.empty:
            fig_stk_fra = px.pie(df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Stock Dass por Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA)
            st.plotly_chart(fig_stk_fra, use_container_width=True)

    # Fila 2: Evolución Histórica
    st.divider()
    ev_so = aplicar_filtros(df_so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SO')
    ev_si = aplicar_filtros(df_si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SI')
    ev_total = ev_so.merge(ev_si, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_evol = go.Figure()
    fig_evol.add_trace(go.Scatter(x=ev_total['MES'], y=ev_total['SO'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_evol.add_trace(go.Scatter(x=ev_total['MES'], y=ev_total['SI'], name='Sell In', line=dict(color='#FF3131', width=2, dash='dot')))
    fig_evol.update_layout(title="Evolución Histórica de Flujos", hovermode="x unified")
    st.plotly_chart(fig_evol, use_container_width=True)

    # --- 9. ANÁLISIS DE RANKING Y TENDENCIAS (LÓGICA ESTABLECIDA) ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings")
    
    r_c1, r_c2 = st.columns(2)
    with r_c1: m_act = st.selectbox("Mes de Referencia (A)", meses_lista, index=0)
    with r_c2: m_ant = st.selectbox("Mes de Comparación (B)", meses_lista, index=min(1, len(meses_lista)-1))

    def calcular_ranking(mes):
        df_mes = df_so_raw[df_so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df_mes['Posicion'] = df_mes['CANT'].rank(ascending=False, method='min')
        return df_mes

    rk_a = calcular_ranking(m_act)
    rk_b = calcular_ranking(m_ant)

    df_rank = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO']].merge(rk_a[['SKU', 'Posicion', 'CANT']], on='SKU', how='inner')
    df_rank = df_rank.merge(rk_b[['SKU', 'Posicion']], on='SKU', how='left', suffixes=('_A', '_B'))
    df_rank['Posicion_B'] = df_rank['Posicion_B'].fillna(999)
    df_rank['Salto'] = df_rank['Posicion_B'] - df_rank['Posicion_A']

    st.subheader(f"Top 10 Performance - {m_act}")
    top10 = df_rank.sort_values('Posicion_A').head(10).copy()
    
    def get_trend_icon(val):
        if val >= 500: return "🆕 Nuevo"
        if val > 0: return f"⬆️ +{int(val)}"
        if val < 0: return f"⬇️ {int(val)}"
        return "➡️ ="

    top10['Tendencia'] = top10['Salto'].apply(get_trend_icon)
    st.dataframe(top10[['Posicion_A', 'SKU', 'DESCRIPCION', 'DISCIPLINA', 'CANT', 'Tendencia']].rename(columns={'Posicion_A': 'Puesto', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)

    # --- 10. ALERTAS DE QUIEBRE Y COBERTURA (MOS) ---
    st.divider()
    st.header("🚨 Análisis de Cobertura y Abastecimiento")
    
    # Stock en depósito central (Dass)
    stk_centro = df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    df_mos = df_rank.merge(stk_centro, on='SKU', how='left').fillna(0)
    
    # Cálculo de Months of Stock (MOS)
    df_mos['MOS'] = (df_mos['Stock_Dass'] / df_mos['CANT']).replace([float('inf')], 0).fillna(0)

    def definir_estado(row):
        if row['Salto'] > 0 and row['MOS'] < 1 and row['CANT'] > 0: return '🔴 CRÍTICO: < 1 mes'
        if row['Salto'] > 0 and row['MOS'] < 2 and row['CANT'] > 0: return '🟡 RIESGO: < 2 meses'
        if row['CANT'] == 0: return '⚪ SIN VENTA'
        return '🟢 SALUDABLE'

    df_mos['Estado'] = df_mos.apply(definir_estado, axis=1)

    mos_c1, mos_c2 = st.columns([2, 1])
    with mos_c1:
        fig_mos = px.scatter(df_mos[df_mos['CANT'] > 0], x='Salto', y='MOS', size='CANT', color='Estado', 
                             hover_name='DESCRIPCION', title="Mapa de Calor: Velocidad vs Stock",
                             color_discrete_map={'🔴 CRÍTICO: < 1 mes': '#FF4B4B', '🟡 RIESGO: < 2 meses': '#FFA500', '🟢 SALUDABLE': '#28A745', '⚪ SIN VENTA': '#D3D3D3'})
        st.plotly_chart(fig_mos, use_container_width=True)
    
    with mos_c2:
        st.metric("SKUs en Riesgo", len(df_mos[df_mos['Estado'].str.contains('🔴|🟡')]))
        st.write("Productos con mayor aceleración y menor cobertura.")
        st.dataframe(df_mos[df_mos['Estado'].str.contains('🔴|🟡')].sort_values('Salto', ascending=False)[['SKU', 'Salto', 'MOS']].head(10), hide_index=True)

    # --- 11. TABLA CONSOLIDADA FINAL (DESCARGABLE) ---
    st.divider()
    st.subheader("📋 Consolidado Maestro de Operaciones")
    
    res_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SO')
    res_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SI')
    res_stk = df_stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')
    
    df_final = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO']].merge(res_so, on='SKU', how='left')
    df_final = df_final.merge(res_si, on='SKU', how='left').merge(res_stk, on='SKU', how='left').fillna(0)
    
    st.dataframe(df_final.sort_values('Venta_SO', ascending=False), use_container_width=True, hide_index=True)
    
    # Botón de descarga
    csv = df_final.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Descargar Reporte Completo", data=csv, file_name=f"reporte_dass_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")

else:
    st.warning("Aguardando conexión con Google Drive para procesar los datos...")

# --- FIN DE LAS +280 LÍNEAS DE LÓGICA ---
