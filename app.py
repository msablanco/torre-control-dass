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

# --- 1. CONFIGURACIÓN VISUAL ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3'
}
COLOR_MAP_FRA = {
    'PINNACLE': '#4B0082', 'BEST': '#1E90FF', 'BETTER': '#32CD32', 
    'GOOD': '#FF8C00', 'CORE': '#696969', 'SIN CATEGORIA': '#D3D3D3'
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
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. PROCESAMIENTO MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma.columns = df_ma.columns.str.strip().str.upper()
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])

    # --- 3. FUNCIONES DE LIMPIEZA CORREGIDAS ---
    def clean_stock(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU','CANT','CLIENTE_F','FECHA_DT','MES'])
        res = pd.DataFrame()
        res['SKU'] = df.iloc[:, 0].astype(str).str.strip().str.upper()
        res['CANT'] = pd.to_numeric(df.iloc[:, 1], errors='coerce').fillna(0)
        res['CLIENTE_F'] = df.iloc[:, 5].astype(str).str.strip().str.upper() # Col F
        res['FECHA_DT'] = pd.to_datetime(df.iloc[:, 3], dayfirst=True, errors='coerce')
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    def clean_sales(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU','CANT','FECHA_DT','MES'])
        df.columns = df.columns.str.strip().str.upper()
        res = pd.DataFrame()
        res['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        # Búsqueda segura de columna de cantidad
        col_c = next((c for c in df.columns if any(x in c for x in ['CANT', 'UNID'])), None)
        res['CANT'] = pd.to_numeric(df[col_c], errors='coerce').fillna(0) if col_c else 0
        # Búsqueda segura de columna de fecha
        col_f = next((c for c in df.columns if 'FECHA' in c), None)
        res['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce') if col_f else pd.NaT
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    so_f_raw = clean_sales('Sell_out')
    si_f_raw = clean_sales('Sell_in')
    stk_f_raw = clean_stock('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    search_query = st.sidebar.text_input("🎯 SKU").upper()
    
    def enrich_and_filter(df):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        temp['DISCIPLINA'] = temp['DISCIPLINA'].fillna('SIN CATEGORIA')
        if search_query:
            temp = temp[temp['SKU'].str.contains(search_query) | temp['DESCRIPCION'].str.contains(search_query, na=False)]
        return temp

    so_f = enrich_and_filter(so_f_raw)
    si_f = enrich_and_filter(si_f_raw)
    stk_f = enrich_and_filter(stk_f_raw)

    # --- 5. MÉTRICAS (SUMAR.SI F:F; wholesale; B:B) ---
    st.title("📊 Torre de Control Dass v11.38")
    max_date = stk_f['FECHA_DT'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['FECHA_DT'] == max_date] if max_date else pd.DataFrame()

    val_stk_cli = stk_snap[stk_snap['CLIENTE_F'] == 'WHOLESALE']['CANT'].sum()
    val_stk_dass = stk_snap[stk_snap['CLIENTE_F'].str.contains('DASS', na=False)]['CANT'].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['CANT'].sum():,.0f}")
    k3.metric("Stock Dass", f"{val_stk_dass:,.0f}")
    k4.metric("Stock Cliente", f"{val_stk_cli:,.0f}")

    # --- 6. GRÁFICOS ---
    st.divider()
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    
    with c1:
        if val_stk_dass > 0:
            st.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_F'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        if not so_f.empty:
            st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if val_stk_cli > 0:
            st.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_F'] == 'WHOLESALE'].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        if not si_f.empty:
            df_bar = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
            total_mes = df_bar.groupby('MES')['CANT'].transform('sum')
            df_bar['%'] = (df_bar['CANT'] / total_mes) * 100
            fig = px.bar(df_bar, x='MES', y='CANT', color='DISCIPLINA', title="Sell In (Participación %)", 
                         text=df_bar['%'].apply(lambda x: f'{x:.1f}%'), color_discrete_map=COLOR_MAP_DIS)
            fig.update_traces(textposition='inside'); fig.update_layout(barmode='stack')
            st.plotly_chart(fig, use_container_width=True)

    # --- 7. EVOLUCIÓN HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica")
    h_so = so_f.groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
    h_si = si_f.groupby('MES')['CANT'].sum().reset_index(name='Sell In')
    h_stk = stk_f.groupby(['MES', 'CLIENTE_F'])['CANT'].sum().reset_index()
    h_sd = h_stk[h_stk['CLIENTE_F'].str.contains('DASS', na=False)].rename(columns={'CANT': 'Stock Dass'})
    h_sc = h_stk[h_stk['CLIENTE_F'] == 'WHOLESALE'].rename(columns={'CANT': 'Stock Cliente'})
    
    df_h = h_so.merge(h_si, on='MES', how='outer').merge(h_sd[['MES', 'Stock Dass']], on='MES', how='outer').merge(h_sc[['MES', 'Stock Cliente']], on='MES', how='outer').fillna(0).sort_values('MES')
    
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Dass'], name='Stock Dass', line=dict(color='#00A693', width=2)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Stock Cliente'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 8. TABLA DETALLE ---
    st.divider()
    st.subheader("📋 Detalle por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_sd = stk_snap[stk_snap['CLIENTE_F'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_sc = stk_snap[stk_snap['CLIENTE_F'] == 'WHOLESALE'].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_res = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_si, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_sc, on='SKU', how='left').fillna(0)
    st.dataframe(df_res[df_res[['Sell Out', 'Sell In', 'Stock Dass', 'Stock Cliente']].sum(axis=1) > 0].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)
else:
    st.error("No se pudo conectar con la base de datos.")
