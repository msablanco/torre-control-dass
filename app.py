import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CARGA Y CONFIGURACIÓN ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000'
}

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

data = load_data()

if data:
    # --- 2. MAESTRO PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col in ['DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']:
            if col in df_ma.columns:
                df_ma[col] = df_ma[col].fillna('OTRO').astype(str).str.upper().str.strip()

    # --- 3. LIMPIEZA UNIFICADA ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Columnas de filtrado
        df['Emprendimiento'] = df.get('EMPRENDIMIENTO', 'WHOLESALE').fillna('WHOLESALE').astype(str).str.strip().str.upper()
        df['Cliente'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.strip().str.upper()
        return df

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 4. FILTROS SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    f_search = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    
    meses_dis = sorted(list(set(so_raw['Mes'].dropna())), reverse=True) if not so_raw.empty else []
    f_mes = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_dis)
    
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['DISCIPLINA'].unique()))
    f_fra = st.sidebar.multiselect("💰 Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    st.sidebar.divider()
    # Filtro de Emprendimiento solicitado
    todas_opciones_emp = sorted(list(set(so_raw['Emprendimiento'].unique()) | set(stk_raw['Emprendimiento'].unique())))
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", todas_opciones_emp, default=todas_opciones_emp)

    clientes_so = sorted(so_raw['Cliente'].unique()) if not so_raw.empty else []
    f_so_cli = st.sidebar.multiselect("📉 Clientes Sell Out", clientes_so, default=clientes_so)

    def apply_filters(df, type_df=None, filter_month=True):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_search: temp = temp[temp['SKU'].str.contains(f_search, na=False) | temp['DESCRIPCION'].str.contains(f_search, na=False)]
        if filter_month and f_mes != "Todos": temp = temp[temp['Mes'] == f_mes]
        if f_emp: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        if type_df == 'SO' and f_so_cli: temp = temp[temp['Cliente'].isin(f_so_cli)]
        return temp

    so_f = apply_filters(so_raw, type_df='SO')
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw)

    # --- 5. LÓGICA DE GRÁFICOS (Sectores) ---
    max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

    def get_sector(df, val):
        if df.empty: return pd.DataFrame()
        return df[df['Emprendimiento'].str.contains(val, na=False)]

    # --- 6. KPIs ---
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out Total", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Stock Dass", f"{get_sector(stk_snap, 'DASS')['Cant'].sum():,.0f}")
    k3.metric("Stock Clientes", f"{get_sector(stk_snap, 'WHOLESALE')['Cant'].sum():,.0f}")
    k4.metric("Stock Retail", f"{get_sector(stk_snap, 'RETAIL')['Cant'].sum():,.0f}")
    k5.metric("Stock E-com", f"{get_sector(stk_snap, 'E-COM')['Cant'].sum():,.0f}")

    # --- 7. GRÁFICOS DE TORTA ---
    st.subheader("📌 Análisis por Disciplina")
    def safe_pie(df, title):
        if not df.empty and df['Cant'].sum() > 0:
            return px.pie(df.groupby('DISCIPLINA')['Cant'].sum().reset_index(), values='Cant', names='DISCIPLINA', title=title, color_discrete_map=COLOR_MAP_DIS)
        return None

    r1 = st.columns(4)
    plots = [
        (get_sector(stk_snap, 'DASS'), "Stock Dass"),
        (get_sector(so_f, 'WHOLESALE'), "Sell Out Wholesale"),
        (get_sector(so_f, 'RETAIL'), "Sell Out Retail"),
        (get_sector(so_f, 'E-COM'), "Sell Out E-com")
    ]
    for i, (d, t) in enumerate(plots):
        fig = safe_pie(d, t)
        if fig: r1[i].plotly_chart(fig, use_container_width=True)
        else: r1[i].info(f"{t}: Sin datos")

    # --- 8. EVOLUCIÓN (Línea de tiempo con 2 líneas) ---
    st.divider()
    st.subheader("📈 Evolución: Sell Out vs Stock Clientes")
    # Consolidamos por mes sin filtrar el mes actual para ver la historia
    hist_so = apply_filters(so_raw, type_df='SO', filter_month=False)
    hist_stk = apply_filters(stk_raw, filter_month=False)
    
    evol_so = get_sector(hist_so, 'WHOLESALE').groupby('Mes')['Cant'].sum().reset_index()
    evol_stk = get_sector(hist_stk, 'WHOLESALE').groupby('Mes')['Cant'].sum().reset_index()
    
    fig_evol = go.Figure()
    fig_evol.add_trace(go.Scatter(x=evol_so['Mes'], y=evol_so['Cant'], name='SELL OUT', line=dict(color='#FF3131', width=3)))
    fig_evol.add_trace(go.Scatter(x=evol_stk['Mes'], y=evol_stk['Cant'], name='STOCK', line=dict(color='#0055A4', width=3, dash='dot')))
    st.plotly_chart(fig_evol, use_container_width=True)

    # --- 9. TABLA DE DETALLE SOLICITADA ---
    st.subheader("📋 Tabla de Información Detallada")
    
    # Cálculos para la tabla (Agrupado por SKU)
    # 1. Sell Out Total (del mes filtrado)
    t_so = so_f.groupby('SKU')['Cant'].sum().reset_index(name='Sell out Total')
    
    # 2. Max Mensual 3 Meses (usando raw para tener historia)
    last_3_months = meses_dis[:3]
    t_3m = so_raw[so_raw['Mes'].isin(last_3_months)].groupby(['Mes', 'SKU'])['Cant'].sum().reset_index()
    t_max_3m = t_3m.groupby('SKU')['Cant'].max().reset_index(name='Max_Mensual_3M')
    
    # 3. Stock Clientes (Wholesale + Retail + Ecom)
    t_stk_cli = stk_snap[stk_snap['Emprendimiento'] != 'DASS CENTRAL'].groupby('SKU')['Cant'].sum().reset_index(name='Stock Clientes')
    
    # 4. Stock Dass
    t_stk_dass = get_sector(stk_snap, 'DASS').groupby('SKU')['Cant'].sum().reset_index(name='Stock Dass')
    
    # 5. Sell In Total
    t_si = si_f.groupby('SKU')['Cant'].sum().reset_index(name='Sell In Total')
    
    # Unión de todas las métricas
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].copy()
    for t in [t_so, t_max_3m, t_stk_cli, t_stk_dass, t_si]:
        df_final = df_final.merge(t, on='SKU', how='left')
    
    df_final = df_final.fillna(0)
    
    # Cálculo MOS (Stock Clientes / Max_Mensual_3M)
    df_final['MOS'] = (df_final['Stock Clientes'] / df_final['Max_Mensual_3M']).replace([float('inf'), -float('inf')], 0).fillna(0).round(1)
    
    # Filtrar solo SKUs que tengan algún movimiento o stock para no ver miles de ceros
    df_final = df_final[(df_final['Sell out Total'] > 0) | (df_final['Stock Clientes'] > 0) | (df_final['Stock Dass'] > 0)]
    
    st.dataframe(df_final.sort_values('Sell out Total', ascending=False), use_container_width=True)
