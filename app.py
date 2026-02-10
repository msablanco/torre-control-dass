import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
import google.generativeai as genai

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Performance & Inteligencia => Fila Calzado", layout="wide")

# --- CONFIGURACIÓN IA (GEMINI) ---
if "GEMINI_API_KEY" in st.secrets:
    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    st.warning("⚠️ Configura GEMINI_API_KEY en Secrets.")


# --- 2. CONFIGURACIÓN VISUAL (MAPAS DE COLORES) ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3', 'OTRO': '#E5E5E5'
}

COLOR_MAP_FRA = {
    'PINNACLE': '#4B0082', 'BEST': '#1E90FF', 'BETTER': '#32CD32', 
    'GOOD': '#FF8C00', 'CORE': 
'#696969', 'SIN CATEGORIA': '#D3D3D3'
}

# --- 3. CARGA DE DATOS DESDE GOOGLE DRIVE ---
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
            st.error("No se encontraron archivos CSV en Drive.")
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
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error al conectar con Drive: {e}")
        return {}

data = load_data_from_drive()

if data:
    # --- 4. PROCESAMIENTO DEL MAESTRO ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']:
            df_maestro[col] = df_maestro.get(col, 'SIN CATEGORIA').fillna('SIN CATEGORIA').astype(str).str.upper()
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 5. LIMPIEZA DE TRANSACCIONALES ---
    def limpiar_transaccional(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        col_c = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS'])), 'CANT')
        df['CANT'] = pd.to_numeric(df[col_c], errors='coerce').fillna(0) if col_c in df.columns else 0
        
        col_f = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), 'FECHA')
        if col_f in df.columns:
            df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        df['CLIENTE_UP'] = df['CLIENTE'].fillna('S/D').astype(str).str.upper() if 'CLIENTE' in df.columns else 'S/D'
        return df

    df_so_raw = limpiar_transaccional('Sell_out')
    df_si_raw = limpiar_transaccional('Sell_in')
    df_stk_raw = limpiar_transaccional('Stock')
    df_ing_raw = limpiar_transaccional('Ingresos')

    # Snapshot de Stock Actual
    if not df_stk_raw.empty:
        max_fecha_stk = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == max_fecha_stk].copy()
        df_stk_snap = df_stk_snap.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
    else:
        df_stk_snap = pd.DataFrame()

    # --- 6. INTERFAZ DE FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo", "").upper()
    meses_disp = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    mes_filtro = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + meses_disp, index=0)
    f_disciplina = st.sidebar.multiselect("👟 Disciplina", sorted(list(df_maestro['DISCIPLINA'].unique())))
    f_clientes = st.sidebar.multiselect("👤 Filtrar por Cliente", sorted(list(set(df_so_raw['CLIENTE_UP'].unique()) | set(df_si_raw['CLIENTE_UP'].unique()))))

    def filtrar_dataframe(df, filtrar_mes=True):
        if df.empty: return df
        temp = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if f_disciplina: temp = temp[temp['DISCIPLINA'].isin(f_disciplina)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if f_clientes: temp = temp[temp['CLIENTE_UP'].isin(f_clientes)]
        if filtrar_mes and mes_filtro != "Todos": temp = temp[temp['MES'] == mes_filtro]
        return temp

    df_so_f = filtrar_dataframe(df_so_raw)
    df_si_f = filtrar_dataframe(df_si_raw)
    df_ing_f = filtrar_dataframe(df_ing_raw)

# --- 7. IA Y DASHBOARD ---
    st.title("📊 Torre de Control: Sell Out & Abastecimiento")

with st.expander("🤖 IA - Consultas Directas sobre la Operación", expanded=True):
        user_question = st.chat_input("Consulta tendencias, ingresos o quiebres...")
        if user_question and "GEMINI_API_KEY" in st.secrets:
            # Contexto resumido de tus datos actuales para la IA
            contexto = f"SO: {df_so_f['CANT'].sum():.0f}. SI: {df_si_f['CANT'].sum():.0f}. Ingresos: {df_ing_f['CANT'].sum():.0f}."
            try:
                # Esta es la parte que hace la conexión real
                response = model.generate_content(f"Eres analista de Dass. Datos: {contexto}. Responde breve: {user_question}")
                st.info(f"**Análisis IA:** {response.text}")
            except Exception as e:
                st.error(f"Error de conexión: {e}")

st.divider() # <--- Verifica que esta línea esté alineada con 'with'

k1, k2, k3, k4 = st.columns(4)
k1.metric("Sell Out (Pares)", f"{df_so_f['CANT'].sum():,.0f}")
k2.metric("Sell In (Pares)", f"{df_si_f['CANT'].sum():,.0f}")
k3.metric("Ingresos 2025", f"{df_ing_f['CANT'].sum():,.0f}")
    
    stock_dass = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not df_stk_snap.empty else 0
    k4.metric("Stock Depósito Dass", f"{stock_dass:,.0f}")

    # Asegúrate de que st.divider() tenga exactamente el mismo nivel que 'with'
st.divider()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Sell Out (Pares)", f"{df_so_f['CANT'].sum():,.0f}")
kpi2.metric("Sell In (Pares)", f"{df_si_f['CANT'].sum():,.0f}")
kpi3.metric("Ingresos 2025", f"{df_ing_f['CANT'].sum():,.0f}")
    
    stock_dass = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not df_stk_snap.empty else 0
    kpi4.metric("Stock Depósito Dass", f"{stock_dass:,.0f}")
    # --- 8. MIX Y EVOLUCIÓN HISTÓRICA ---
    st.divider()
    col_m1, col_m2, col_m3 = st.columns([1, 1, 2])
    with col_m1:
        if not df_so_f.empty:
            fig_mix_so = px.pie(df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Mix Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
            st.plotly_chart(fig_mix_so, use_container_width=True)
    with col_m2:
        if not df_stk_snap.empty:
            stk_mix = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index()
            st.plotly_chart(px.pie(stk_mix, values='CANT', names='DISCIPLINA', title="Mix Stock Depósito", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with col_m3:
        evol_so = filtrar_dataframe(df_so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
        evol_si = filtrar_dataframe(df_si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell In')
        evol_ing = filtrar_dataframe(df_ing_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Ingresos')
        evol_total = evol_so.merge(evol_si, on='MES', how='outer').merge(evol_ing, on='MES', how='outer').fillna(0).sort_values('MES')
        fig_evol = go.Figure()
        fig_evol.add_trace(go.Scatter(x=evol_total['MES'], y=evol_total['Ingresos'], name='Ingresos', line=dict(color='gray', dash='dot')))
        fig_evol.add_trace(go.Scatter(x=evol_total['MES'], y=evol_total['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
        fig_evol.add_trace(go.Scatter(x=evol_total['MES'], y=evol_total['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3)))
        st.plotly_chart(fig_evol, use_container_width=True)

    # --- 9. RANKING Y TENDENCIAS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings")
    mes_a = st.selectbox("Comparación (A)", meses_disp, index=0, key='ma')
    mes_b = st.selectbox("Base (B)", meses_disp, index=min(1, len(meses_disp)-1), key='mb')

    def get_rk(mes):
        df = df_so_raw[df_so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df['Pos'] = df['CANT'].rank(ascending=False, method='min')
        return df

    rk_a, rk_b = get_rk(mes_a), get_rk(mes_b)
    df_tend = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a, on='SKU', how='inner')
    df_tend = df_tend.merge(rk_b[['SKU', 'Pos']], on='SKU', how='left', suffixes=('_A', '_B')).fillna(999)
    df_tend['Salto'] = df_tend['Pos_B'] - df_tend['Pos_A']
    
    st.subheader(f"Top 10 en {mes_a}")
    st.dataframe(df_tend.sort_values('Pos_A').head(10)[['Pos_A', 'SKU', 'DESCRIPCION', 'CANT', 'Salto']], use_container_width=True, hide_index=True)

    # --- 10. EXPLORADOR POR DISCIPLINA ---
    st.divider()
    d_foc = st.selectbox("Análisis por Disciplina:", sorted(df_maestro['DISCIPLINA'].unique()))
    df_d = df_tend[df_tend['DISCIPLINA'] == d_foc].sort_values('CANT', ascending=False).head(10)
    c_d1, c_d2 = st.columns([2, 1])
    with c_d1: st.dataframe(df_d[['SKU', 'DESCRIPCION', 'CANT', 'Salto']], use_container_width=True)
    with c_d2: st.plotly_chart(px.bar(df_d, x='CANT', y='SKU', orientation='h', title="Top Volumen", color_discrete_sequence=[COLOR_MAP_DIS.get(d_foc, '#000')]), use_container_width=True)

    # --- 11. MOS Y ALERTAS ---
    st.divider()
    st.header("🚨 Alerta de Quiebre (MOS)")
    stk_g = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='STK')
    df_al = df_tend.merge(stk_g, on='SKU', how='left').fillna(0)
    df_al['MOS'] = (df_al['STK'] / (df_al['CANT'] + 0.1)).round(1)

    def semaforo(row):
        if row['Salto'] >= 5 and row['MOS'] < 1.0 and row['CANT'] > 0: return '🔴 CRÍTICO'
        if row['Salto'] > 0 and row['MOS'] < 2.0 and row['CANT'] > 0: return '🟡 ADVERTENCIA'
        return '🟢 OK'

    df_al['Estado'] = df_al.apply(semaforo, axis=1)
    st.plotly_chart(px.scatter(df_al[df_al['CANT']>0], x='Salto', y='MOS', size='CANT', color='Estado', hover_name='DESCRIPCION', color_discrete_map={'🔴 CRÍTICO': '#ff4b4b', '🟡 ADVERTENCIA': '#ffa500', '🟢 OK': '#28a745'}), use_container_width=True)

    # --- 12. TABLA MAESTRA DETALLADA ---
    st.divider()
    st.subheader("📋 Consolidado Maestro")
    res_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_Out')
    res_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_In')
    df_f = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(res_so, on='SKU', how='left').merge(res_si, on='SKU', how='left').fillna(0)
    st.dataframe(df_f.sort_values('Sell_Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("Verifique la carpeta de Drive.")










