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

# --- 1. CONFIGURACIÓN VISUAL (COLORES CORPORATIVOS) ---
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

# --- 2. CARGA DE DATOS DESDE GOOGLE DRIVE (CON CACHE) ---
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
            st.error("No se encontraron archivos CSV en la carpeta.")
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
            # Detectar separador y cargar con encoding robusto
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
    # --- 3. PROCESAMIENTO DEL MAESTRO DE PRODUCTOS ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        
        # Columnas obligatorias
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO']:
            if col not in df_maestro.columns: df_maestro[col] = 'SIN CATEGORIA'
            df_maestro[col] = df_maestro[col].fillna('SIN CATEGORIA').astype(str).str.upper()
        
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 4. MOTOR DE LIMPIEZA TRANSACCIONAL ---
    def limpiar_transaccional(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Buscar columna de cantidades
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS'])), None)
        df['CANT'] = pd.to_numeric(df[col_cant], errors='coerce').fillna(0) if col_cant else 0
        
        # Procesamiento de fechas
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), None)
        if col_fecha:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        df['CLIENTE_UP'] = df['CLIENTE'].fillna('S/D').astype(str).str.upper() if 'CLIENTE' in df.columns else 'S/D'
        return df

    # Carga de archivos core
    df_so_raw = limpiar_transaccional('Sell_out')
    df_si_raw = limpiar_transaccional('Sell_in')
    df_stk_raw = limpiar_transaccional('Stock')
    df_ing_raw = limpiar_transaccional('Ingresos')

    # Snapshot Stock Actual (Última fecha disponible)
    if not df_stk_raw.empty:
        max_fecha = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == max_fecha].copy()
        df_stk_snap = df_stk_snap.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO']], on='SKU', how='left')
    else:
        df_stk_snap = pd.DataFrame()

    # --- 5. INTERFAZ DE FILTROS (SIDEBAR) ---
    st.sidebar.header("🔍 Control de Filtros")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo", "").upper()
    
    meses_dis = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    mes_filtro = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + meses_dis)

    # Filtros por categorías del Maestro
    f_emp = st.sidebar.multiselect("🏬 Emprendimiento (Wholesale/Retail/Ecom)", sorted(df_maestro['EMPRENDIMIENTO'].unique()))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_maestro['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_maestro['FRANJA_PRECIO'].unique()))
    
    clientes_all = sorted(list(set(df_so_raw['CLIENTE_UP'].unique()) | set(df_si_raw['CLIENTE_UP'].unique())))
    f_cli = st.sidebar.multiselect("👤 Clientes", clientes_all)

    # --- 6. APLICACIÓN DE FILTRADO LÓGICO ---
    def filtrar_dataframe(df, filtrar_mes=True):
        if df.empty: return df
        temp = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO', 'BUSQUEDA']], on='SKU', how='left')
        
        if f_emp: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_cli: temp = temp[temp['CLIENTE_UP'].isin(f_cli)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if filtrar_mes and mes_filtro != "Todos": temp = temp[temp['MES'] == mes_filtro]
        return temp

    df_so_f = filtrar_dataframe(df_so_raw)
    df_si_f = filtrar_dataframe(df_si_raw)
    df_ing_f = filtrar_dataframe(df_ing_raw)
    df_stk_f = filtrar_dataframe(df_stk_snap, filtrar_mes=False)

    # --- 7. PANEL DE MÉTRICAS (KPIs) ---
    st.title("📊 Torre de Control: Sell Out & Abastecimiento")
    
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    with kpi1:
        st.metric("Sell Out (prs)", f"{df_so_f['CANT'].sum():,.0f}")
    with kpi2:
        st.metric("Sell In (prs)", f"{df_si_f['CANT'].sum():,.0f}")
    with kpi3:
        st.metric("Ingresos (prs)", f"{df_ing_f['CANT'].sum():,.0f}")
    with kpi4:
        stk_val = df_stk_f['CANT'].sum() if not df_stk_f.empty else 0
        st.metric("Stock Actual", f"{stk_val:,.0f}")

    st.divider()

    # --- 8. ANÁLISIS DE MIX Y FLUJO ---
    col_mix1, col_mix2, col_mix3 = st.columns([1, 1, 2])

    with col_mix1:
        mix_dis = df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index()
        st.plotly_chart(px.pie(mix_dis, values='CANT', names='DISCIPLINA', title="Mix Disciplina", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    with col_mix2:
        mix_fra = df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index()
        st.plotly_chart(px.pie(mix_fra, values='CANT', names='FRANJA_PRECIO', title="Mix Franja Precio", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)

    with col_mix3:
        # Evolución Histórica Comparativa
        ev_so = filtrar_dataframe(df_so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SO')
        ev_si = filtrar_dataframe(df_si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='SI')
        ev_ing = filtrar_dataframe(df_ing_raw, False).groupby('MES')['CANT'].sum().reset_index(name='ING')
        
        evol = ev_so.merge(ev_si, on='MES', how='outer').merge(ev_ing, on='MES', how='outer').fillna(0).sort_values('MES')
        
        fig_ev = go.Figure()
        fig_ev.add_trace(go.Scatter(x=evol['MES'], y=evol['ING'], name='Ingresos', line=dict(color='#A9A9A9', dash='dot')))
        fig_ev.add_trace(go.Scatter(x=evol['MES'], y=evol['SO'], name='Sell Out', line=dict(color='#0055A4', width=4)))
        fig_ev.add_trace(go.Scatter(x=evol['MES'], y=evol['SI'], name='Sell In', line=dict(color='#FF3131', width=2)))
        fig_ev.update_layout(title="Flujo Logístico Mensual", hovermode='x unified')
        st.plotly_chart(fig_ev, use_container_width=True)

    # --- 9. INTELIGENCIA DE RANKINGS (TENDENCIAS) ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings")
    
    sel_a, sel_b = st.columns(2)
    m_a = sel_a.selectbox("Mes Actual (Comparar)", meses_dis, index=0, key='ma')
    m_b = sel_b.selectbox("Mes Anterior (Base)", meses_dis, index=min(1, len(meses_dis)-1), key='mb')

    def get_rank(mes):
        df_m = df_so_raw[df_so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df_m['POS'] = df_m['CANT'].rank(ascending=False, method='min')
        return df_m

    rk_a = get_rank(m_a)
    rk_b = get_rank(m_b)

    # Cruzar datos con el Maestro para detalle
    df_trend = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(rk_a[['SKU', 'POS', 'CANT']], on='SKU', how='inner')
    df_trend = df_trend.merge(rk_b[['SKU', 'POS']], on='SKU', how='left', suffixes=('_A', '_B'))
    df_trend['POS_B'] = df_trend['POS_B'].fillna(999)
    df_trend['SALTO'] = df_trend['POS_B'] - df_trend['POS_A']

    st.subheader(f"Top 10 Productos en {m_a}")
    t10 = df_trend.sort_values('POS_A').head(10).copy()
    t10['TREND'] = t10['SALTO'].apply(lambda x: f"⬆️ +{int(x)}" if 0 < x < 500 else (f"⬇️ {int(x)}" if x < 0 else "🆕" if x >= 500 else "="))
    
    st.dataframe(t10[['POS_A', 'SKU', 'DESCRIPCION', 'CANT', 'TREND']].rename(columns={'POS_A': 'Puesto', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)

    # --- 10. ALERTAS DE COBERTURA (MOS) ---
    st.divider()
    st.header("🚨 Cobertura de Stock (MOS)")
    
    # Stock exclusivo en Dass para Wholesale
    stk_dass = df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='STK_DASS')
    df_mos = df_trend.merge(stk_dass, on='SKU', how='left').fillna(0)
    
    # Cálculo: Stock Dass / Venta Promedio (se usa mes A como referencia)
    df_mos['MOS'] = (df_mos['STK_DASS'] / df_mos['CANT']).replace([float('inf'), -float('inf')], 0).fillna(0)

    def alertar(r):
        if r['SALTO'] > 0 and r['MOS'] < 1 and r['CANT'] > 0: return '🔴 CRÍTICO'
        if r['SALTO'] > 0 and r['MOS'] < 2 and r['CANT'] > 0: return '🟡 RIESGO'
        return '🟢 OK'

    df_mos['ESTADO'] = df_mos.apply(alertar, axis=1)

    c_mos1, c_mos2 = st.columns([2, 1])
    with c_mos1:
        fig_mos = px.scatter(df_mos[df_mos['CANT'] > 0], x='SALTO', y='MOS', size='CANT', color='ESTADO',
                             hover_name='DESCRIPCION', title="Salto de Ranking vs Meses de Stock (MOS)",
                             color_discrete_map={'🔴 CRÍTICO': '#FF4B4B', '🟡 RIESGO': '#FFA500', '🟢 OK': '#28A745'})
        st.plotly_chart(fig_mos, use_container_width=True)
    with c_mos2:
        st.write("**Productos Críticos (Suben y sin stock):**")
        st.dataframe(df_mos[df_mos['ESTADO'] == '🔴 CRÍTICO'].sort_values('SALTO', ascending=False)[['SKU', 'CANT', 'MOS']], hide_index=True)

    # --- 11. TABLA CONSOLIDADA FINAL ---
    st.divider()
    st.subheader("📋 Detalle Maestro Consolidado")
    
    res_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='SELL_OUT')
    res_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='SELL_IN')
    res_ing = df_ing_f.groupby('SKU')['CANT'].sum().reset_index(name='INGRESOS')
    res_stk = df_stk_f.groupby('SKU')['CANT'].sum().reset
