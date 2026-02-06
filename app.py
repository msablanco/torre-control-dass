import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# Configuración de página
st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CONFIGURACIÓN DE COLORES (ESTRICTO) ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'OTRO': '#D3D3D3', 'SIN CAT': '#E5E5E5'
}

# --- 2. FUNCIONES DE CARGA Y LIMPIEZA ---
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
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

def clean_df(df, name):
    if df.empty: return pd.DataFrame()
    df = df.copy()
    # Estandarizar SKU
    df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
    # Estandarizar Cantidades
    c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
    df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
    # Estandarizar Fechas
    c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
    df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
    df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
    # Columnas de agrupación (Paso 1 del usuario)
    df['Emprendimiento'] = df.get('EMPRENDIMIENTO', 'S/E').fillna('S/E').astype(str).str.strip().str.upper()
    df['Cliente'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.strip().str.upper()
    return df

# --- 3. PROCESAMIENTO PRINCIPAL ---
data = load_data()

if data:
    # Cargar Maestro
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']:
            if col in df_ma.columns:
                df_ma[col] = df_ma[col].fillna('OTRO').astype(str).str.upper().str.strip()

    # Cargar y limpiar archivos
    so_raw = clean_df(data.get('Sell_out', pd.DataFrame()), 'Sell_out')
    si_raw = clean_df(data.get('Sell_in', pd.DataFrame()), 'Sell_in')
    stk_raw = clean_df(data.get('Stock', pd.DataFrame()), 'Stock')

    # --- 4. SIDEBAR (FILTROS SOLICITADOS) ---
    st.sidebar.header("🔍 Filtros de Control")
    f_search = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    meses_dis = sorted(list(set(so_raw['Mes'].dropna())), reverse=True) if not so_raw.empty else []
    f_mes = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_dis)
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    st.sidebar.divider()
    
    # PASO 1: FILTRO EMPRENDIMIENTO UNIFICADO
    lista_emp = sorted(list(set(so_raw['Emprendimiento'].unique()) | set(si_raw['Emprendimiento'].unique())))
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", options=lista_emp, default=lista_emp)

    # Filtro Cliente Sell Out (Columna E)
    lista_cli = sorted(so_raw['Cliente'].unique()) if not so_raw.empty else []
    f_cli_so = st.sidebar.multiselect("📉 Clientes Sell Out", options=lista_cli, default=lista_cli)

    # --- 5. APLICACIÓN DE FILTROS ---
    def apply_filters(df, is_so=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_search: 
            temp = temp[temp['SKU'].str.contains(f_search, na=False) | temp['DESCRIPCION'].str.contains(f_search, na=False)]
        if f_mes != "Todos": temp = temp[temp['Mes'] == f_mes]
        if f_emp: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        if is_so and f_cli_so: temp = temp[temp['Cliente'].isin(f_cli_so)]
        return temp

    so_f = apply_filters(so_raw, is_so=True)
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw)

    # --- 6. KPIs Y TOTALES ---
    stk_max = stk_f[stk_f['Fecha_dt'] == stk_f['Fecha_dt'].max()] if not stk_f.empty else pd.DataFrame()
    
    def sum_q(df, key):
        if df.empty: return 0
        return df[df['Emprendimiento'].str.contains(key, na=False)]['Cant'].sum()

    st.subheader("📊 Resumen Ejecutivo")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Sell Out Total", f"{so_f['Cant'].sum():,.0f}")
    c2.metric("Stock Dass", f"{sum_q(stk_max, 'DASS'):,.0f}")
    c3.metric("Stock Wholesale", f"{sum_q(stk_max, 'WHOLESALE'):,.0f}")
    c4.metric("Stock Retail", f"{sum_q(stk_max, 'RETAIL'):,.0f}")
    c5.metric("Stock E-com", f"{sum_q(stk_max, 'E-COM'):,.0f}")

    # --- 7. GRÁFICOS (COLORES ESTRICTOS) ---
    st.divider()
    def draw_pie(df, title):
        if df.empty or df['Cant'].sum() == 0: return None
        data_pie = df.groupby('DISCIPLINA')['Cant'].sum().reset_index()
        fig = px.pie(data_pie, values='Cant', names='DISCIPLINA', title=title,
                     color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        return fig

    st.subheader("👟 Distribución por Disciplina")
    g1, g2, g3, g4 = st.columns(4)
    with g1:
        f = draw_pie(stk_max[stk_max['Emprendimiento'].str.contains('DASS')], "Stock Dass")
        if f: st.plotly_chart(f, use_container_width=True)
    with g2:
        f = draw_pie(so_f[so_f['Emprendimiento'].str.contains('WHOLESALE')], "SO Wholesale")
        if f: st.plotly_chart(f, use_container_width=True)
    with g3:
        f = draw_pie(so_f[so_f['Emprendimiento'].str.contains('RETAIL')], "SO Retail")
        if f: st.plotly_chart(f, use_container_width=True)
    with g4:
        f = draw_pie(so_f[so_f['Emprendimiento'].str.contains('E-COM')], "SO E-com")
        if f: st.plotly_chart(f, use_container_width=True)

    # --- 8. LÍNEA DE TIEMPO (2 LÍNEAS) ---
    st.subheader("📈 Evolución: Sell Out vs Stock (Wholesale/Retail/Ecom)")
    # Re-filtrar sin el mes actual para ver la historia
    h_so = apply_filters(so_raw, is_so=True).groupby('Mes')['Cant'].sum().reset_index()
    h_stk = apply_filters(stk_raw).groupby('Mes')['Cant'].sum().reset_index()
    
    fig_ev = go.Figure()
    fig_ev.add_trace(go.Scatter(x=h_so['Mes'], y=h_so['Cant'], name='SELL OUT', line=dict(color='#FF3131', width=3)))
    fig_ev.add_trace(go.Scatter(x=h_stk['Mes'], y=h_stk['Cant'], name='STOCK', line=dict(color='#0055A4', width=3, dash='dot')))
    st.plotly_chart(fig_ev, use_container_width=True)

    # --- 9. TABLA DE DETALLE (TODOS LOS CAMPOS) ---
    st.divider()
    st.subheader("📋 Tabla de Información Detallada")
    
    # Cálculos por SKU
    t_so = so_f.groupby('SKU')['Cant'].sum().reset_index(name='Sell out Total')
    t_m3 = so_raw[so_raw['Mes'].isin(meses_dis[:3])].groupby(['Mes', 'SKU'])['Cant'].sum().reset_index()
    t_max = t_m3.groupby('SKU')['Cant'].max().reset_index(name='Max_Mensual_3M')
    t_stk_c = stk_max[~stk_max['Emprendimiento'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index(name='Stock Clientes')
    t_stk_d = stk_max[stk_max['Emprendimiento'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index(name='Stock Dass')
    t_si = si_f.groupby('SKU')['Cant'].sum().reset_index(name='Sell In Total')
    
    # Merge Final
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left')
    for t in [t_max, t_stk_c, t_stk_d, t_si]:
        df_final = df_final.merge(t, on='SKU', how='left')
    
    df_final = df_final.fillna(0)
    # Cálculo MOS
    df_final['MOS'] = (df_final['Stock Clientes'] / df_final['Max_Mensual_3M']).replace([float('inf')], 0).fillna(0).round(1)
    
    # Columnas finales solicitadas
    cols = ['SKU','DESCRIPCION','DISCIPLINA','FRANJA_PRECIO','Sell out Total','Max_Mensual_3M','Stock Clientes','MOS','Stock Dass','Sell In Total']
    st.dataframe(df_final[cols].sort_values('Sell out Total', ascending=False), use_container_width=True)

else:
    st.warning("No se encontraron archivos CSV en la carpeta configurada.")
