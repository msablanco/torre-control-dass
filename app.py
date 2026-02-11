import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# --- 1. CONFIGURACIÓN DE INTERFAZ ---
st.set_page_config(page_title="Dass Performance - Torre de Control", layout="wide")

# Estilos CSS para métricas y contenedores
st.markdown("""
    <style>
    .main { background-color: #f4f7f9; }
    [data-testid="stMetric"] { background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .stDataFrame { background-color: #ffffff; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. CONFIGURACIÓN DE MASTER DATA (ESTABLECIDA) ---
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

# --- 3. INSTRUCCIONES DE CARGA Y AUDITORÍA DE DRIVE ---
@st.cache_data(ttl=600)
def load_and_audit_drive():
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
            st.warning("⚠️ No se encontraron archivos CSV en la carpeta configurada.")
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
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            # INSTRUCCIÓN: Normalización agresiva de cabeceras para evitar errores de llave
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"❌ Error en conexión con Drive: {e}")
        return {}

data = load_and_audit_drive()

if data:
    # --- 4. INSTRUCCIONES DE NORMALIZACIÓN DEL MAESTRO ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        
        target_cols = ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'EMPRENDIMIENTO', 'GENERO', 'CATEGORIA']
        for col in target_cols:
            if col in df_maestro.columns:
                df_maestro[col] = df_maestro[col].fillna('SIN DEFINIR').astype(str).str.upper()
            else:
                df_maestro[col] = 'SIN DEFINIR'
        
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 5. INSTRUCCIONES DE LIMPIEZA TRANSACCIONAL ---
    def procesar_base(nombre_base):
        df = data.get(nombre_base, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Búsqueda de columna de cantidad (instrucción dinámica)
        c_vol = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS', 'PARES'])), None)
        df['CANT'] = pd.to_numeric(df[c_vol], errors='coerce').fillna(0) if c_vol else 0
        
        # Búsqueda de columna de fecha
        c_fec = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MOVIMIENTO', 'DIA'])), None)
        if c_fec:
            df['FECHA_DT'] = pd.to_datetime(df[c_fec], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        df['CLIENTE_UP'] = df['CLIENTE'].fillna('GENERAL').astype(str).str.upper() if 'CLIENTE' in df.columns else 'GENERAL'
        return df

    df_so_raw = procesar_base('Sell_out')
    df_si_raw = procesar_base('Sell_in')
    df_stk_raw = procesar_base('Stock')
    df_ing_raw = procesar_base('Ingresos')

    # Snapshot Stock (Instrucción: Solo última foto)
    if not df_stk_raw.empty:
        ultima_fecha = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == ultima_fecha].copy()
    else:
        df_stk_snap = pd.DataFrame()

    # --- 6. SIDEBAR: INSTRUCCIONES DE FILTRADO JERÁRQUICO ---
    st.sidebar.header("🎯 Filtros de Torre")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Modelo", "").upper()
    
    meses_disponibles = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    periodo_base = st.sidebar.selectbox("📅 Mes Base de Análisis", ["Todos"] + meses_disponibles)

    with st.sidebar.expander("👟 Filtros de Atributo"):
        f_emp = st.multiselect("Emprendimiento", sorted(df_maestro['EMPRENDIMIENTO'].unique()))
        f_dis = st.multiselect("Disciplina", sorted(df_maestro['DISCIPLINA'].unique()))
        f_fra = st.multiselect("Franja de Precio", sorted(df_maestro['FRANJA_PRECIO'].unique()))

    with st.sidebar.expander("🏬 Filtros de Canal"):
        canal_list = sorted(list(set(df_so_raw['CLIENTE_UP'].unique()) | set(df_si_raw['CLIENTE_UP'].unique())))
        f_cli = st.multiselect("Cliente/Canal", canal_list)

    # --- 7. LÓGICA DE APLICACIÓN DE FILTROS ---
    def aplicar_reglas_filtro(df, f_mes=True):
        if df.empty: return df
        # Enlace con maestro
        temp = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        
        if f_emp: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_cli: temp = temp[temp['CLIENTE_UP'].isin(f_cli)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if f_mes and periodo_base != "Todos": temp = temp[temp['MES'] == periodo_base]
        return temp

    df_so_f = aplicar_reglas_filtro(df_so_raw)
    df_si_f = aplicar_reglas_filtro(df_si_raw)
    df_stk_f = aplicar_reglas_filtro(df_stk_snap, f_mes=False)

    # --- 8. DASHBOARD VISUAL: MIX Y EVOLUCIÓN ---
    st.title("📊 Control de Performance: Sell Out & Stock")
    
    # KPIs Rápidos
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Pares SO", f"{int(df_so_f['CANT'].sum()):,}")
    kpi2.metric("Pares Stock", f"{int(df_stk_f['CANT'].sum()):,}")
    kpi3.metric("Sell In (Mes)", f"{int(df_si_f['CANT'].sum()):,}")
    kpi4.metric("SKUs Filtrados", len(df_so_f['SKU'].unique()))

    st.divider()
    
    c_m1, c_m2, c_m3 = st.columns(3)
    with c_m1:
        st.plotly_chart(px.pie(df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Venta por Disciplina", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c_m2:
        st.plotly_chart(px.pie(df_so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO', title="Venta por Franja", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    with c_m3:
        st.plotly_chart(px.pie(df_so_f.groupby('EMPRENDIMIENTO')['CANT'].sum().reset_index(), values='CANT', names='EMPRENDIMIENTO', title="Venta por Canal", hole=0.3), use_container_width=True)

    # --- 9. INSTRUCCIONES DE RANKING Y TENDENCIAS ---
    st.divider()
    st.header("🏆 Rankings de Velocidad")
    rk_a_col, rk_b_col = st.columns(2)
    sel_a = rk_a_col.selectbox("Mes Actual (A)", meses_disponibles, index=0)
    sel_b = rk_b_col.selectbox("Mes Anterior (B)", meses_disponibles, index=min(1, len(meses_disponibles)-1))

    def calcular_posicion(mes):
        d = df_so_raw[df_so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        d['Pos'] = d['CANT'].rank(ascending=False, method='min')
        return d

    rk_a, rk_b = calcular_posicion(sel_a), calcular_posicion(sel_b)
    df_rank = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Pos', 'CANT']], on='SKU', how='inner')
    df_rank = df_rank.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna(999)
    df_rank['Salto'] = df_rank['Pos_B'] - df_rank['Pos_A']

    st.subheader(f"Top 10 Performers - {sel_a}")
    df_v_rank = df_rank.sort_values('Pos_A').head(10).copy()
    df_v_rank['Status'] = df_v_rank['Salto'].apply(lambda x: f"⬆️ +{int(x)}" if 0 < x < 500 else (f"⬇️ {int(x)}" if x < 0 else "🆕" if x >= 500 else "➡️"))
    st.dataframe(df_v_rank[['Pos_A', 'SKU', 'DESCRIPCION', 'CANT', 'Status']], hide_index=True, use_container_width=True)

    # --- 10. INSTRUCCIONES DE COBERTURA (MOS) ---
    st.divider()
    st.header("🚨 Cobertura de Stock (MOS)")
    
    # Stock exclusivo de DASS para análisis de reposición
    stk_reposicion = df_stk_f[df_stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    df_mos = df_rank.merge(stk_reposicion, on='SKU', how='left').fillna(0)
    df_mos['MOS'] = (df_mos['Stock_Dass'] / df_mos['CANT']).replace([float('inf')], 0).fillna(0)

    def semaforo_logico(r):
        if r['Salto'] > 0 and r['MOS'] < 1 and r['CANT'] > 0: return '🔴 CRÍTICO'
        if r['Salto'] > 0 and r['MOS'] < 2 and r['CANT'] > 0: return '🟡 RIESGO'
        return '🟢 OK'

    df_mos['Alerta'] = df_mos.apply(semaforo_logico, axis=1)
    
    fig_mos = px.scatter(df_mos[df_mos['CANT']>0], x='Salto', y='MOS', size='CANT', color='Alerta', 
                         hover_name='DESCRIPCION', color_discrete_map={'🔴 CRÍTICO': '#FF4B4B', '🟡 RIESGO': '#FFA500', '🟢 OK': '#28A745'})
    st.plotly_chart(fig_mos, use_container_width=True)

    # --- 11. DETALLE MAESTRO CONSOLIDADO (CORREGIDO) ---
    st.divider()
    st.header("📝 Detalle Maestro Consolidado")
    
    res_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SO')
    res_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_SI')
    # LA CORRECCIÓN AL ERROR DE LA IMAGEN:
    res_stk = df_stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    
    df_final = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO', 'EMPRENDIMIENTO']].merge(res_so, on='SKU', how='left')
    df_final = df_final.merge(res_si, on='SKU', how='left').merge(res_stk, on='SKU', how='left').fillna(0)
    
    st.dataframe(df_final.sort_values('Venta_SO', ascending=False), use_container_width=True, hide_index=True)
    
    # Botón de descarga
    csv_bytes = df_final.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Descargar Consolidado CSV", csv_bytes, f"bi_dass_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

else:
    st.info("💡 Subí los archivos CSV a la carpeta de Drive configurada para comenzar.")
