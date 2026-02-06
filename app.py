import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.express as px
import plotly.graph_objects as go

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CONFIGURACIÓN DE COLORES ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000', 'OTRO': '#D3D3D3'
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
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

def clean_df(df):
    if df is None or df.empty: return pd.DataFrame()
    df = df.copy()
    df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
    c_cant = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT'])), 'CANT')
    df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
    c_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'MES'])), 'FECHA')
    df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
    df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
    df['EMPRENDIMIENTO'] = df.get('EMPRENDIMIENTO', 'S/E').fillna('S/E').astype(str).str.strip().str.upper()
    df['CLIENTE'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.strip().str.upper()
    return df

data = load_data()

if data:
    # Maestro y limpieza base
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
    
    so_raw = clean_df(data.get('Sell_out'))
    si_raw = clean_df(data.get('Sell_in'))
    stk_raw = clean_df(data.get('Stock'))

    # --- 3. SIDEBAR (TODOS LOS FILTROS) ---
    st.sidebar.header("🔍 Panel de Control")
    f_search = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    
    meses_dis = sorted(list(set(so_raw['Mes'].dropna())), reverse=True) if not so_raw.empty else []
    f_mes = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_dis)
    
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['DISCIPLINA'].unique()) if not df_ma.empty else [])
    f_fra = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_ma['FRANJA_PRECIO'].unique()) if not df_ma.empty else [])
    
    st.sidebar.divider()
    # Filtro Emprendimiento Unificado
    opts_emp = sorted(list(set(so_raw['EMPRENDIMIENTO'].unique()) | set(si_raw['EMPRENDIMIENTO'].unique()) | set(stk_raw['EMPRENDIMIENTO'].unique())))
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento", options=opts_emp, default=opts_emp)
    
    # Filtro Cliente Sell Out
    opts_cli = sorted(so_raw['CLIENTE'].unique()) if not so_raw.empty else []
    f_cli = st.sidebar.multiselect("📉 Clientes Sell Out", options=opts_cli, default=opts_cli)

    def apply_filters(df, is_so=False):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        if f_search: temp = temp[temp['SKU'].str.contains(f_search, na=False) | temp['DESCRIPCION'].str.contains(f_search, na=False)]
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if f_mes != "Todos": temp = temp[temp['Mes'] == f_mes]
        if f_emp: temp = temp[temp['EMPRENDIMIENTO'].isin(f_emp)]
        if is_so and f_cli: temp = temp[temp['CLIENTE'].isin(f_cli)]
        return temp

    so_f = apply_filters(so_raw, is_so=True)
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw)

    # --- 4. KPIs ---
    st.subheader("📊 Resumen Performance")
    stk_max = stk_f[stk_f['Fecha_dt'] == stk_f['Fecha_dt'].max()] if not stk_f.empty else pd.DataFrame()
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sell Out Total", f"{so_f['Cant'].sum():,.0f}")
    c2.metric("Sell In Total", f"{si_f['Cant'].sum():,.0f}")
    c3.metric("Stock Wholesale", f"{stk_max[stk_max['EMPRENDIMIENTO'] == 'WHOLESALE']['Cant'].sum():,.0f}")
    c4.metric("Stock Dass", f"{stk_max[stk_max['EMPRENDIMIENTO'].str.contains('DASS', na=False)]['Cant'].sum():,.0f}")

    # --- 5. GRÁFICOS DE TORTA (DISCIPLINA Y FRANJA) ---
    st.divider()
    col_t1, col_t2 = st.columns(2)
    
    with col_t1:
        st.write("### Por Disciplina")
        if not so_f.empty:
            fig_dis = px.pie(so_f.groupby('DISCIPLINA')['Cant'].sum().reset_index(), 
                             values='Cant', names='DISCIPLINA', color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
            st.plotly_chart(fig_dis, use_container_width=True)
            
    with col_t2:
        st.write("### Por Franja de Precio")
        if not so_f.empty:
            fig_fra = px.pie(so_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), 
                             values='Cant', names='FRANJA_PRECIO', hole=0.4)
            st.plotly_chart(fig_fra, use_container_width=True)

    # --- 6. LÍNEA DE TIEMPO ---
    st.subheader("📈 Evolución: Sell Out vs Stock")
    ev_so = so_f.groupby('Mes')['Cant'].sum().reset_index()
    ev_stk = stk_f.groupby('Mes')['Cant'].sum().reset_index()
    fig_ev = go.Figure()
    fig_ev.add_trace(go.Scatter(x=ev_so['Mes'], y=ev_so['Cant'], name='Sell Out', line=dict(color='#FF3131', width=3)))
    fig_ev.add_trace(go.Scatter(x=ev_stk['Mes'], y=ev_stk['Cant'], name='Stock Total', line=dict(color='#0055A4', width=3, dash='dot')))
    st.plotly_chart(fig_ev, use_container_width=True)

    # --- 7. TABLA DE DATOS ---
    st.divider()
    st.subheader("📋 Detalle por SKU")
    
    t_so = so_f.groupby('SKU')['Cant'].sum().reset_index(name='Sell out Total')
    t_si = si_f.groupby('SKU')['Cant'].sum().reset_index(name='Sell In Total')
    t_stk_c = stk_max[stk_max['EMPRENDIMIENTO'] != 'DASS'].groupby('SKU')['Cant'].sum().reset_index(name='Stock Clientes')
    t_stk_d = stk_max[stk_max['EMPRENDIMIENTO'].str.contains('DASS', na=False)].groupby('SKU')['Cant'].sum().reset_index(name='Stock Dass')
    
    # Máximo mensual últimos 3 meses para MOS
    m3_l = meses_dis[:3]
    t_m3 = so_raw[so_raw['Mes'].isin(m3_l)].groupby(['Mes', 'SKU'])['Cant'].sum().reset_index()
    t_max3 = t_m3.groupby('SKU')['Cant'].max().reset_index(name='Max_Mensual_3M')
    
    df_f = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].copy()
    for t in [t_so, t_max3, t_stk_c, t_stk_d, t_si]:
        df_f = df_f.merge(t, on='SKU', how='left')
    
    df_f = df_f.fillna(0)
    df_f['MOS'] = (df_f['Stock Clientes'] / df_f['Max_Mensual_3M']).replace([float('inf')], 0).fillna(0).round(1)
    
    cols = ['SKU','DESCRIPCION','DISCIPLINA','FRANJA_PRECIO','Sell out Total','Max_Mensual_3M','Stock Clientes','MOS','Stock Dass','Sell In Total']
    st.dataframe(df_f[cols].sort_values('Sell out Total', ascending=False), use_container_width=True)

else:
    st.error("No se detectaron archivos en la carpeta de Google Drive.")
