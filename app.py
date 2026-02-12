import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Fila Calzado - Inteligencia de Stock", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL (PUNTO 1) ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3'
}

# --- 2. CARGA DE DATOS (GOOGLE DRIVE) ---
@st.cache_data(ttl=600)
def load_data_from_drive():
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
        st.error(f"Error Crítico Drive: {e}")
        return {}

data = load_data_from_drive()

if data:
    # --- 3. PROCESAMIENTO MAESTRO Y LIMPIEZA SKU ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col, default in {'DISCIPLINA': 'SIN CATEGORIA', 'FRANJA_PRECIO': 'SIN CATEGORIA', 'DESCRIPCION': 'SIN DESCRIPCION'}.items():
            if col not in df_ma.columns: df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    def clean_generic(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'FECHA_DT', 'MES', 'CLIENTE_UP'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        # Buscar columna cantidad (CANT, UNIDADES, etc)
        col_c = next((c for c in df.columns if any(x in c for x in ['CANT', 'UNID', 'QTY'])), 'CANT')
        df['CANT'] = pd.to_numeric(df[col_c], errors='coerce').fillna(0)
        # Buscar columna fecha
        col_f = next((c for c in df.columns if any(x in c for x in ['FECHA', 'ARRIVO', 'ETA', 'VENTA'])), 'FECHA')
        df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
        df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').astype(str).str.upper()
        return df

    so_raw = clean_generic('Sell_out')
    stk_raw = clean_generic('Stock')
    ing_raw = clean_generic('ingresos')

    # --- 4. SIDEBAR (PUNTO 1) ---
    st.sidebar.header("🔍 Control de Gestión")
    meses_disponibles = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_mes = st.sidebar.selectbox("📅 Mes de Análisis", meses_disponibles if meses_disponibles else ["S/D"])
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franjas", sorted(df_ma['FRANJA_PRECIO'].unique()))
    search = st.sidebar.text_input("🎯 Buscar SKU / Modelo").upper()

    # --- LOGICA DE FILTRADO ---
    def filtrar(df, por_mes=True, tipo=None):
        if df.empty: return df
        df = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if por_mes: df = df[df['MES'] == f_mes]
        if f_dis: df = df[df['DISCIPLINA'].isin(f_dis)]
        if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
        if search: df = df[df['BUSQUEDA'].str.contains(search, na=False)]
        return df

    so_f = filtrar(so_raw)
    stk_f = filtrar(stk_raw)
    
    # Procesamiento Ingresos (Punto 12): Pasados vs Futuros
    # Ingresos Pasados: Los que ocurrieron en el mes seleccionado
    ing_mes = filtrar(ing_raw, por_mes=True)
    # Ingresos Futuros: Todos los que tienen fecha posterior al último día del mes seleccionado
    fecha_corte = pd.to_datetime(f_mes + "-01") + pd.offsets.MonthEnd(0)
    ing_futuro_total = ing_raw[ing_raw['FECHA_DT'] > fecha_corte].copy()
    # Filtrar ingresos futuros por disciplina/busqueda para coherencia
    ing_futuro_f = filtrar(ing_futuro_total, por_mes=False)

    # --- 5. PANEL DE KPIs (PUNTO 2) ---
    st.title(f"📊 Performance & Stock Intelligence - {f_mes}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Venta Mes (SO)", f"{so_f['CANT'].sum():,.0f}")
    c2.metric("Stock Cierre Mes", f"{stk_f['CANT'].sum():,.0f}")
    c3.metric("Ingresos Realizados", f"{ing_mes['CANT'].sum():,.0f}")
    c4.metric("Ingresos Programados", f"{ing_futuro_f['CANT'].sum():,.0f}")

    # --- 6. LINEA DE TIEMPO (PUNTO 3) ---
    st.subheader("📈 3. Evolución Histórica: Ventas, Stock e Ingresos")
    hist_so = filtrar(so_raw, por_mes=False).groupby('MES')['CANT'].sum().reset_index(name='Venta')
    hist_stk = filtrar(stk_raw, por_mes=False).groupby('MES')['CANT'].sum().reset_index(name='Stock')
    hist_ing = filtrar(ing_raw, por_mes=False).groupby('MES')['CANT'].sum().reset_index(name='Ingresos')
    
    df_h = hist_so.merge(hist_stk, on='MES', how='outer').merge(hist_ing, on='MES', how='outer').fillna(0).sort_values('MES')
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Venta'], name='Venta (SO)', line=dict(color='#0055A4', width=4)))
    fig_h.add_trace(go.Bar(x=df_h['MES'], y=df_h['Stock'], name='Stock Total', marker_color='#D3D3D3', opacity=0.5))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Ingresos'], name='Ingresos', mode='markers+lines', line=dict(color='#FF3131', dash='dot')))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 7. TORTAS (PUNTO 4 Y 5) ---
    st.divider()
    col_t1, col_t2, col_t3 = st.columns(3)
    with col_t1:
        st.write("**4. Venta por Disciplina**")
        st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with col_t2:
        st.write("**4. Venta por Franja**")
        st.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), values='CANT', names='FRANJA_PRECIO'), use_container_width=True)
    with col_t3:
        st.write("**5. Ubicación del Stock**")
        stk_f['UBICACION'] = stk_f['CLIENTE_UP'].apply(lambda x: 'PLANTA DASS' if 'DASS' in x else 'CANAL CLIENTES')
        st.plotly_chart(px.pie(stk_f.groupby('UBICACION')['CANT'].sum().reset_index(), values='CANT', names='UBICACION', color_discrete_sequence=['#00A693', '#FFD700']), use_container_width=True)

    # --- 8. TABLA MAESTRA (PUNTO 6 Y 7) ---
    st.divider()
    st.subheader("📋 6. Detalle Estratégico de SKUs")
    
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='VENTA')
    t_stk_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS')].groupby('SKU')['CANT'].sum().reset_index(name='STK_CLI')
    t_stk_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS')].groupby('SKU')['CANT'].sum().reset_index(name='STK_DASS')
    t_ing_f = ing_futuro_f.groupby('SKU')['CANT'].sum().reset_index(name='ING_FUTURO')
    
    df_m = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(t_so, on='SKU', how='left')\
            .merge(t_stk_c, on='SKU', how='left')\
            .merge(t_stk_d, on='SKU', how='left')\
            .merge(t_ing_f, on='SKU', how='left').fillna(0)
    
    df_m['COBERTURA'] = (df_m['STK_CLI'] / df_m['VENTA']).replace([float('inf')], 99).fillna(0)
    
    def semaforo_cob(val):
        if val == 0: return ''
        if val < 1.5: return 'background-color: #FFB3B3' # Rojo
        if val <= 3.5: return 'background-color: #B3FFB3' # Verde
        return 'background-color: #FFFFB3' # Amarillo

    st.dataframe(df_m.sort_values('VENTA', ascending=False).style.applymap(semaforo_cob, subset=['COBERTURA']), use_container_width=True, hide_index=True)

    # --- 9. RANKINGS Y SALTOS (PUNTO 8 Y 9) ---
    st.divider()
    st.subheader("🏆 8-9. Ranking y Saltos de Performance")
    c_rk1, c_rk2 = st.columns(2)
    with c_rk1:
        mes_ant = meses_disponibles[min(1, len(meses_disponibles)-1)]
        rk_act = so_raw[so_raw['MES'] == f_mes].groupby('SKU')['CANT'].sum().rank(ascending=False)
        rk_ant = so_raw[so_raw['MES'] == mes_ant].groupby('SKU')['CANT'].sum().rank(ascending=False)
        
        df_rk = df_ma[['SKU', 'DESCRIPCION']].merge(rk_act.reset_index(name='Pos_Hoy'), on='SKU', how='inner')
        df_rk = df_rk.merge(rk_ant.reset_index(name='Pos_Ayer'), on='SKU', how='left').fillna(999)
        df_rk['Salto'] = df_rk['Pos_Ayer'] - df_rk['Pos_Hoy']
        st.write(f"Top 10 Tendencia ({f_mes} vs {mes_ant})")
        st.dataframe(df_rk.sort_values('Pos_Hoy').head(10), use_container_width=True, hide_index=True)

    # --- 10. EXPLORADOR TACTICO (PUNTO 10) ---
    with c_rk2:
        st.write("**10. Explorador Táctico**")
        dis_sel = st.selectbox("Elegir Disciplina:", sorted(df_ma['DISCIPLINA'].unique()))
        df_tact = df_m[df_m['DISCIPLINA'] == dis_sel].sort_values('VENTA', ascending=False).head(10)
        st.bar_chart(df_tact, x='SKU', y='VENTA')

    # --- 11. QUIEBRES Y ALERTAS (PUNTO 11, 12, 13) ---
    st.divider()
    st.subheader("⚠️ 11-13. Gestión de Quiebres y Resumen Ejecutivo")
    df_quiebre = df_m[(df_m['VENTA'] > 0) & (df_m['STK_CLI'] == 0)].copy()
    
    if not df_quiebre.empty:
        st.error(f"ATENCIÓN: {len(df_quiebre)} SKUs en quiebre con venta activa.")
        st.dataframe(df_quiebre[['SKU', 'DESCRIPCION', 'VENTA', 'ING_FUTURO', 'DISCIPLINA']], use_container_width=True, hide_index=True)
    else:
        st.success("No se detectan quiebres críticos en la selección actual.")

    # Resumen Ejecutivo (Punto 13)
    st.info(f"Resumen de Inventario: El stock total de {stk_f['CANT'].sum():,.0f} unidades representa una cobertura promedio de {(stk_f['CANT'].sum()/so_f['CANT'].sum() if so_f['CANT'].sum()>0 else 0):.1f} meses de venta.")
