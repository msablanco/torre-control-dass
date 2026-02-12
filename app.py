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
    # --- 3. PROCESAMIENTO MAESTRO ---
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
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'MES', 'CLIENTE_UP'])
        
        # Limpieza Crítica de SKU
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Búsqueda inteligente de columna de cantidad (CANT, CANTIDAD, UNIDADES, etc.)
        posibles_cols = ['UNIDADES', 'CANTIDAD', 'CANT', 'CANT_TOTAL', 'CANT.', 'QTY']
        col_found = next((c for c in df.columns if any(p in c for p in posibles_cols)), None)
        
        if col_found:
            df['CANT'] = pd.to_numeric(df[col_found], errors='coerce').fillna(0)
        else:
            df['CANT'] = 0
            
        # Fecha y Mes
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'ETA'])), 'FECHA')
        if col_fecha in df.columns:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        else:
            df['MES'] = "S/D"
            
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.upper()
        return df[['SKU', 'CANT', 'MES', 'CLIENTE_UP']]

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')
    ingresos_raw = clean_df('ingresos')

    # --- 4. FILTROS SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    meses_op = sorted(list(set(so_raw['MES'].dropna()) | set(stk_raw['MES'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes de Análisis", meses_op if meses_op else ["S/D"])
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo").upper()
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['DISCIPLINA'].unique()))
    
    # --- 5. LOGICA INDEPENDIENTE DE INGRESOS ---
    # Esto asegura que los ingresos se vean siempre, ignorando el mes seleccionado
    df_ing_proc = ingresos_raw.groupby('SKU')['CANT'].sum().reset_index(name='Ingresos_Futuros')
    
    def apply_logic(df, filter_month=True, tipo=None):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        if filter_month: temp = temp[temp['MES'] == f_periodo]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        return temp

    so_f = apply_logic(so_raw)
    stk_f = apply_logic(stk_raw)

    # --- 6. KPI / HEADER ---
    st.title(f"📊 Dashboard Performance - {f_periodo}")
    
    # --- 7. EVOLUCIÓN HISTÓRICA ---
    st.subheader("📈 Evolución de Ventas y Stock")
    h_so = apply_logic(so_raw, filter_month=False).groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
    h_stk = apply_logic(stk_raw, filter_month=False).groupby('MES')['CANT'].sum().reset_index(name='Stock Total')
    df_hist = h_so.merge(h_stk, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_hist['MES'], y=df_hist['Sell Out'], name='Ventas', line=dict(color='#0055A4', width=3)))
    fig.add_trace(go.Bar(x=df_hist['MES'], y=df_hist['Stock Total'], name='Stock', marker_color='#D3D3D3', opacity=0.6))
    st.plotly_chart(fig, use_container_width=True)

    # --- 8. TABLA PRINCIPAL (DETALLE) ---
    st.divider()
    st.subheader("📋 Detalle General e Ingresos")
    
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta_Mes')
    t_stk = stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Actual')
    
    # Consolidación final
    df_resumen = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].copy()
    df_resumen = df_resumen.merge(t_so, on='SKU', how='left')
    df_resumen = df_resumen.merge(t_stk, on='SKU', how='left')
    df_resumen = df_resumen.merge(df_ing_proc, on='SKU', how='left')
    df_resumen = df_resumen.fillna(0)
    
    # Cálculo de Cobertura
    df_resumen['Meses_Cobertura'] = (df_resumen['Stock_Actual'] / df_resumen['Venta_Mes']).replace([float('inf')], 99).fillna(0)
    
    # Filtro: Solo mostrar lo que tiene movimiento o stock o ingresos
    df_resumen = df_resumen[(df_resumen['Venta_Mes']>0) | (df_resumen['Stock_Actual']>0) | (df_resumen['Ingresos_Futuros']>0)]

    def color_cobertura(val):
        if val == 0: return ''
        color = '#FFB3B3' if val < 1.5 else '#B3FFB3' if val < 3.5 else '#FFFFB3'
        return f'background-color: {color}'

    st.dataframe(
        df_resumen.style.applymap(color_cobertura, subset=['Meses_Cobertura']),
        use_container_width=True, hide_index=True
    )

    # --- 9-11. RANKING Y TENDENCIAS ---
    st.divider()
    st.subheader("🏆 Top 10 Productos del Mes")
    top_10 = df_resumen.sort_values('Venta_Mes', ascending=False).head(10)
    st.table(top_10[['SKU', 'DESCRIPCION', 'Venta_Mes', 'Stock_Actual', 'Ingresos_Futuros']])

    # --- 12. EXPLORADOR TÁCTICO POR DISCIPLINA ---
    st.divider()
    st.subheader("👟 Análisis por Disciplina")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        dis_pie = df_resumen.groupby('DISCIPLINA')['Venta_Mes'].sum().reset_index()
        st.plotly_chart(px.pie(dis_pie, values='Venta_Mes', names='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    
    with col2:
        f_dis_sel = st.selectbox("Filtrar Tabla Táctica:", sorted(df_ma['DISCIPLINA'].unique()))
        df_dis_view = df_resumen[df_resumen['DISCIPLINA'] == f_dis_sel].sort_values('Venta_Mes', ascending=False)
        st.dataframe(df_dis_view, use_container_width=True, hide_index=True)

    # --- 13. ALERTAS DE QUIEBRE ---
    st.divider()
    st.subheader("⚠️ Alerta de Quiebre (Venden pero no hay stock)")
    quiebres = df_resumen[(df_resumen['Venta_Mes'] > 10) & (df_resumen['Stock_Actual'] == 0)]
    if not quiebres.empty:
        st.error(f"Se detectaron {len(quiebres)} productos con venta activa y stock cero.")
        st.dataframe(quiebres[['SKU', 'DESCRIPCION', 'Venta_Mes', 'Ingresos_Futuros']], use_container_width=True, hide_index=True)
    else:
        st.success("No hay quiebres críticos detectados.")
