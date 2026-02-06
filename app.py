import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL (MAPAS DE COLORES ACTUALIZADOS) ---
# Colores para Disciplinas
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3', 'OTRO': '#E5E5E5'
}

# Colores para Franjas de Precio (Contraste mejorado para evitar confusión GOOD vs KIDS)
COLOR_MAP_FRA = {
    'PINNACLE': '#1A237E', # Azul Navy (Oscuro)
    'BEST': '#2E7D32',      # Verde Bosque
    'BETTER': '#FBC02D',    # Amarillo Oro
    'GOOD': '#D32F2F',      # Rojo Intenso
    'CORE': '#757575',      # Gris
    'SIN CATEGORIA': '#E0E0E0'
}

# --- 2. CARGA DE DATOS DESDE GOOGLE DRIVE ---
@st.cache_data(ttl=600)
def load_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'", 
            fields="files(id, name)"
        ).execute()
        
        dfs = {}
        for item in results.get('files', []):
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            
            # Detectar separador automáticamente y normalizar columnas
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error en la conexión con Drive: {e}")
        return {}

data = load_data()

if data:
    # --- 3. PROCESAMIENTO DEL MAESTRO DE PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        
        # Columnas obligatorias
        for col, default in {'DISCIPLINA': 'SIN CATEGORIA', 'FRANJA_PRECIO': 'SIN CATEGORIA', 'DESCRIPCION': 'SIN DESCRIPCION'}.items():
            if col not in df_ma.columns:
                df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    # --- 4. FUNCIÓN DE LIMPIEZA PARA SELL IN / OUT / STOCK ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty:
            return pd.DataFrame(columns=['SKU', 'CANT', 'MES', 'FECHA_DT', 'CLIENTE_UP'])
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Identificar columna de cantidad
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'CANT')
        df['CANT'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        
        # Procesar fechas
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), 'FECHA')
        df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        # Cliente
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.upper()
        return df

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 5. SIDEBAR: FILTROS GLOBALES ---
    st.sidebar.header("🔍 Filtros de Control")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo").upper()
    
    # Filtro de Mes
    meses_disponibles = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True) if not so_raw.empty else []
    f_periodo = st.sidebar.selectbox("📅 Período Mensual", ["Todos"] + meses_disponibles)
    
    # Filtros de Categoría (Blindados con str para evitar TypeError en sorted)
    opts_dis = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_dis = st.sidebar.multiselect("👟 Filtrar Disciplinas", opts_dis)
    
    opts_fra = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_fra = st.sidebar.multiselect("💰 Filtrar Franjas de Precio", opts_fra)
    
    # Filtros de Clientes
    clientes_so = sorted(so_raw['CLIENTE_UP'].unique()) if not so_raw.empty else []
    clientes_si = sorted(si_raw['CLIENTE_UP'].unique()) if not si_raw.empty else []
    f_cli_so = st.sidebar.multiselect("👤 Clientes Sell Out", clientes_so)
    f_cli_si = st.sidebar.multiselect("📦 Clientes Sell In", clientes_si)
    
    selected_clients = set(f_cli_so) | set(f_cli_si)

    # --- 6. LÓGICA DE APLICACIÓN DE FILTROS ---
    def apply_filters(df, filter_month=True):
        if df.empty: return df
        temp = df.copy()
        
        # Cruzar con maestro
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        
        # Manejo de SKUs no registrados
        temp['DISCIPLINA'] = temp['DISCIPLINA'].fillna('SIN CATEGORIA')
        temp['FRANJA_PRECIO'] = temp['FRANJA_PRECIO'].fillna('SIN CATEGORIA')
        temp['DESCRIPCION'] = temp['DESCRIPCION'].fillna('PRODUCTO NO EN MAESTRO')

        # Aplicar filtros del Sidebar
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query:
            mask = temp['BUSQUEDA'].str.contains(search_query, na=False) | temp['SKU'].str.contains(search_query, na=False)
            temp = temp[mask]
        
        if filter_month and f_periodo != "Todos":
            temp = temp[temp['MES'] == f_periodo]
            
        if selected_clients:
            temp = temp[temp['CLIENTE_UP'].isin(selected_clients)]
            
        return temp

    so_f = apply_filters(so_raw)
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw)

    # --- 7. DASHBOARD: KPIs PRINCIPALES ---
    st.title("📊 Torre de Control Dass v11.38")
    
    # Snapshot de Stock (Última fecha disponible en el archivo)
    max_date = stk_f['FECHA_DT'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['FECHA_DT'] == max_date] if max_date else pd.DataFrame()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out (Venta)", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In (Factura)", f"{si_f['CANT'].sum():,.0f}")
    
    stock_dass = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k3.metric("Stock en Dass", f"{stock_dass:,.0f}")
    
    stock_cliente = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k4.metric("Stock en Clientes", f"{stock_cliente:,.0f}")

    # --- 8. BLOQUE DE GRÁFICOS: DISCIPLINA ---
    st.divider()
    st.subheader("📌 Distribución por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    
    if stock_dass > 0:
        fig_sd = px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                        values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
        c1.plotly_chart(fig_sd, use_container_width=True)
        
    if not so_f.empty:
        fig_so = px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                        values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
        c2.plotly_chart(fig_so, use_container_width=True)
        
    if stock_cliente > 0:
        fig_sc = px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                        values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
        c3.plotly_chart(fig_sc, use_container_width=True)
        
    if not si_f.empty:
        fig_si = px.bar(si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index(), 
                        x='MES', y='CANT', color='DISCIPLINA', title="Evolución Sell In", color_discrete_map=COLOR_MAP_DIS)
        c4.plotly_chart(fig_si, use_container_width=True)

    # --- 9. BLOQUE DE GRÁFICOS: FRANJA DE PRECIO (COLOR CONSISTENTE) ---
    st.subheader("💰 Distribución por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    
    if stock_dass > 0:
        fig_f1 = px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                        values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA)
        f1.plotly_chart(fig_f1, use_container_width=True)
        
    if not so_f.empty:
        fig_f2 = px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                        values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA)
        f2.plotly_chart(fig_f2, use_container_width=True)
        
    if stock_cliente > 0:
        fig_f3 = px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                        values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA)
        f3.plotly_chart(fig_f3, use_container_width=True)
        
    if not si_f.empty:
        fig_f4 = px.bar(si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index(), 
                        x='MES', y='CANT', color='FRANJA_PRECIO', title="Evolución Franjas", color_discrete_map=COLOR_MAP_FRA)
        f4.plotly_chart(fig_f4, use_container_width=True)

    # --- 10. LÍNEA DE TIEMPO HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica Comparativa")
    
    # Agrupar datos históricos (sin filtro de mes activo)
    h_so = apply_filters(so_raw, filter_month=False).groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Venta (SO)'})
    h_si = apply_filters(si_raw, filter_month=False).groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Factura (SI)'})
    h_stk = apply_filters(stk_raw, filter_month=False)
    
    h_sd = h_stk[h_stk['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Stock Dass'})
    h_sc = h_stk[~h_stk['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('MES')['CANT'].sum().reset_index().rename(columns={'CANT': 'Stock Cliente'})
    
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd, on='MES', how='outer').merge(h_sc, on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Venta (SO)'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Factura (SI)'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Dass'], name='Stock Dass', line=dict(color='#00A693', width=2)))
    fig_line.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Cliente'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
    st.plotly_chart(fig_line, use_container_width=True)

    # --- 11. TABLA DE DETALLE POR SKU ---
    st.divider()
    st.subheader("📋 Detalle Analítico por SKU")
    
    # Agregados por SKU
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Venta SO')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Compra SI')
    t_sd = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_sc = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    # Tabla Final
    df_tab = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_sc, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_si, on='SKU', how='left').fillna(0)
    
    # Eliminar filas sin datos para mejorar legibilidad
    df_tab = df_tab[(df_tab['Venta SO'] > 0) | (df_tab['Stock Cliente'] > 0) | (df_tab['Stock Dass'] > 0) | (df_tab['Compra SI'] > 0)]
    
    st.dataframe(df_tab.sort_values('Venta SO', ascending=False), use_container_width=True, hide_index=True)

else:
    st.warning("⚠️ No se encontraron archivos CSV en la carpeta de Google Drive. Por favor, revisa la conexión y el Folder ID.")
