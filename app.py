import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.16", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL ---
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
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. MAESTRO (Incluye Franja) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        # Aseguramos Disciplina y Franja_Precio
        for col, default in {'Disciplina': 'OTRO', 'FRANJA_PRECIO': 'SIN CAT', 'Descripcion': 'SIN DESCRIPCION'}.items():
            if col not in df_ma.columns: df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. LIMPIEZA ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'Cant')
        df['Cant'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        df['Cliente_up'] = df.get('Cliente', '').fillna('').astype(str).str.upper()
        return df[['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up']]

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_ma['FRANJA_PRECIO'].unique()))
    f_cli_so = st.sidebar.multiselect("👤 Cliente Sell Out", sorted(so_raw['Cliente_up'].unique()))
    f_cli_si = st.sidebar.multiselect("📦 Cliente Sell In", sorted(si_raw['Cliente_up'].unique()))

    selected_clients = set(f_cli_so) | set(f_cli_si)

    def apply_logic(df, filter_month=True):
        temp = df.copy()
        if temp.empty: return temp
        m_filt = df_ma.copy()
        if f_dis: m_filt = m_filt[m_filt['Disciplina'].isin(f_dis)]
        if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]
        if search_query: m_filt = m_filt[m_filt['Busqueda'].str.contains(search_query, na=False)]
        temp = temp[temp['SKU'].isin(m_filt['SKU'])]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if selected_clients and 'Cliente_up' in temp.columns:
            temp = temp[temp['Cliente_up'].isin(selected_clients)]
        return temp.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')

    so_f = apply_logic(so_raw)
    si_f = apply_logic(si_raw)
    stk_f = apply_logic(stk_raw)

    # --- 5. TABS ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Inteligencia de Abastecimiento"])

    with tab_control:
        max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
        stk_snap = stk_f[stk_f['Fecha_dt'] == max_date] if max_date else pd.DataFrame()

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Sell Out (Venta)", f"{so_f['Cant'].sum():,.0f}")
        k2.metric("Sell In (Factura)", f"{si_f['Cant'].sum():,.0f}")
        val_dass = stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)]['Cant'].sum() if not stk_snap.empty else 0
        k3.metric("Stock Dass", f"{val_dass:,.0f}")
        val_cli = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)]['Cant'].sum() if not stk_snap.empty else 0
        k4.metric("Stock Cliente", f"{val_cli:,.0f}")

        # --- GRAFICOS POR DISCIPLINA ---
        st.subheader("📌 Análisis por Disciplina")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        if val_dass > 0: c1.plotly_chart(px.pie(stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stk Dass (Dis)", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        if not so_f.empty: c2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out (Dis)", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        if val_cli > 0: c3.plotly_chart(px.pie(stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stk Cliente (Dis)", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        if not si_f.empty: c4.plotly_chart(px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Facturación por Disciplina", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # --- GRAFICOS POR FRANJA (NUEVO/RESTAURADO) ---
        st.subheader("💰 Análisis por Franja de Precio")
        f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
        if val_dass > 0: f1.plotly_chart(px.pie(stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stk Dass (Franja)"), use_container_width=True)
        if not so_f.empty: f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out (Franja)"), use_container_width=True)
        if val_cli > 0: f3.plotly_chart(px.pie(stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stk Cliente (Franja)"), use_container_width=True)
        if not si_f.empty: f4.plotly_chart(px.bar(si_f.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Facturación por Franja"), use_container_width=True)

        # Evolución Histórica
        st.divider()
        st.subheader("📈 Evolución Histórica")
        so_h = apply_logic(so_raw, filter_month=False).groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out'})
        si_h = apply_logic(si_raw, filter_month=False).groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In'})
        stk_h_raw = apply_logic(stk_raw, filter_month=False)
        sd_h = stk_h_raw[stk_h_raw['Cliente_up'].str.contains('DASS', na=False)].groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        sc_h = stk_h_raw[~stk_h_raw['Cliente_up'].str.contains('DASS', na=False)].groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Cliente'})
        df_h = so_h.merge(si_h, on='Mes', how='outer').merge(sd_h, on='Mes', how='outer').merge(sc_h, on='Mes', how='outer').fillna(0).sort_values('Mes')
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
        fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
        fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Stock Dass'], name='Stock Dass', line=dict(color='#00A693', width=2)))
        fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Stock Cliente'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
        st.plotly_chart(fig, use_container_width=True)

        # Tabla Detalle
        st.divider()
        st.subheader("📋 Detalle Operativo")
        t_so_op = so_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out Total'})
        t_si_op = si_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In Total'})
        t_stk_d_op = stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'}) if not stk_snap.empty else pd.DataFrame(columns=['SKU', 'Stock Dass'])
        t_stk_c_op = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Cliente'}) if not stk_snap.empty else pd.DataFrame(columns=['SKU', 'Stock Cliente'])
        df_op = df_ma[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO']].merge(t_so_op, on='SKU', how='left').merge(t_stk_c_op, on='SKU', how='left').merge(t_stk_d_op, on='SKU', how='left').merge(t_si_op, on='SKU', how='left').fillna(0)
        df_op = df_op[(df_op['Sell Out Total'] > 0) | (df_op['Stock Cliente'] > 0) | (df_op['Stock Dass'] > 0) | (df_op['Sell In Total'] > 0)]
        st.dataframe(df_op.sort_values('Sell Out Total', ascending=False), use_container_width=True, hide_index=True)

    with tab_intel:
        st.header("🎯 Pronóstico y Sugerencia de Compra")
        # Aquí sigue la lógica de inteligencia...
