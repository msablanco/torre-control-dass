import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.22", layout="wide")

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
    # --- 2. MAESTRO (Dato de Franja y Disciplina) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        col_f = next((c for c in df_ma.columns if 'FRANJA' in c.upper() or 'PRECIO' in c.upper()), 'FRANJA_PRECIO')
        df_ma['Disciplina'] = df_ma.get('Disciplina', 'OTRO').fillna('OTRO').astype(str).str.upper().str.strip()
        df_ma['FRANJA_PRECIO'] = df_ma.get(col_f, 'SIN CAT').fillna('SIN CAT').astype(str).str.upper().str.strip()
        df_ma['Descripcion'] = df_ma.get('Descripcion', 'SIN DESCRIPCIÓN').fillna('SIN DESCRIPCIÓN').astype(str).str.upper()
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. LIMPIEZA DE TRANSACCIONES ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'Cant')
        df['Cant'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO', 'MES'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        df['Cliente_up'] = df.get('Cliente', 'DESCONOCIDO').fillna('DESCONOCIDO').astype(str).str.upper()
        return df[['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up']]

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_ma['FRANJA_PRECIO'].unique()))
    f_cli_target = st.sidebar.multiselect("👤 Seleccionar Clientes", sorted(list(set(so_raw['Cliente_up']) | set(stk_raw['Cliente_up']))))

    def apply_logic(df, filter_month=True):
        temp = df.copy()
        if temp.empty: return temp
        temp = temp.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion', 'Busqueda']], on='SKU', how='left')
        temp['Disciplina'] = temp['Disciplina'].fillna('OTRO')
        temp['FRANJA_PRECIO'] = temp['FRANJA_PRECIO'].fillna('SIN CAT')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if search_query: temp = temp[temp['Busqueda'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if f_cli_target: temp = temp[temp['Cliente_up'].isin(f_cli_target)]
        return temp

    so_f, si_f, stk_f = apply_logic(so_raw), apply_logic(si_raw), apply_logic(stk_raw)

    # --- 5. TABS ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Inteligencia de Abastecimiento"])

    with tab_control:
        max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
        stk_snap = stk_f[stk_f['Fecha_dt'] == max_date] if max_date else pd.DataFrame()
        
        # DEFINICIÓN CLAVE: ¿Quién es Dass y quién es Cliente?
        is_dass = stk_snap['Cliente_up'].str.contains('DASS', na=False)
        df_stk_dass = stk_snap[is_dass]
        df_stk_clie = stk_snap[~is_dass]

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Sell Out", f"{so_f['Cant'].sum():,.0f}")
        k2.metric("Sell In", f"{si_f['Cant'].sum():,.0f}")
        k3.metric("Stock Dass", f"{df_stk_dass['Cant'].sum():,.0f}")
        k4.metric("Stock Cliente", f"{df_stk_clie['Cant'].sum():,.0f}")

        # --- FILA 1: DISCIPLINAS ---
        st.subheader("📌 Distribución por Disciplina")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        c1.plotly_chart(px.pie(df_stk_dass.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        # Aquí forzamos que si no hay datos, muestre un mensaje en lugar de nada
        if not df_stk_clie.empty:
            c3.plotly_chart(px.pie(df_stk_clie.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        else:
            c3.info("Sin Stock Cliente p/ el filtro")
        c4.plotly_chart(px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In Mensual", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # --- FILA 2: FRANJAS ---
        st.subheader("💰 Distribución por Franja de Precio")
        f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
        f1.plotly_chart(px.pie(df_stk_dass.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Dass"), use_container_width=True)
        f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out"), use_container_width=True)
        if not df_stk_clie.empty:
            f3.plotly_chart(px.pie(df_stk_clie.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Cliente"), use_container_width=True)
        else:
            f3.info("Sin Stock Cliente p/ el filtro")
        f4.plotly_chart(px.bar(si_f.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In por Franja"), use_container_width=True)

        # --- LINEA DE TIEMPO ---
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

    with tab_intel:
        st.header("🎯 Sugerencia de Reposición")
        # (Lógica de inteligencia aquí...)
        st.dataframe(apply_logic(so_raw, filter_month=False).groupby('SKU')['Cant'].sum().reset_index().sort_values('Cant', ascending=False))
