import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.25", layout="wide")

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
    # --- 2. PROCESAMIENTO MAESTRO ---
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
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Emprendimiento', 'Cliente_up'])
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        c_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNID', 'CANT'])), 'Cant')
        df['Cant'] = pd.to_numeric(df[c_cant], errors='coerce').fillna(0)
        
        c_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'MES'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[c_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Lógica de Emprendimiento mejorada
        if 'EMPRENDIMIENTO' in df.columns:
            df['Emprendimiento'] = df['EMPRENDIMIENTO'].fillna('OTROS').astype(str).str.upper().str.strip()
        else:
            # Si no está la columna, intentamos deducir por el nombre del cliente
            c_cli = next((c for c in df.columns if 'CLIENTE' in c.upper()), 'Cliente')
            df['Cliente_up'] = df.get(c_cli, 'DESCONOCIDO').fillna('DESCONOCIDO').astype(str).str.upper()
            df['Emprendimiento'] = df['Cliente_up'].apply(lambda x: 'DASS CENTRAL' if 'DASS' in x else 'CLIENTES')
        
        return df

    so_f = clean_df('Sell_out')
    si_f = clean_df('Sell_in')
    stk_f = clean_df('Stock')

    # --- 4. FILTROS SIDEBAR ---
    st.sidebar.header("🔍 Filtros de Negocio")
    f_periodo = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + sorted(list(set(so_f['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franja de Precio", sorted(df_ma['FRANJA_PRECIO'].unique()))
    
    # NUEVO FILTRO SOLICITADO
    opciones_emp = sorted(stk_f['Emprendimiento'].unique()) if not stk_f.empty else []
    f_emp = st.sidebar.multiselect("🏢 Emprendimiento (Stock)", opciones_emp)

    def apply_filters(df, filter_month=True):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion', 'Busqueda']], on='SKU', how='left')
        temp['Disciplina'] = temp['Disciplina'].fillna('OTRO')
        temp['FRANJA_PRECIO'] = temp['FRANJA_PRECIO'].fillna('SIN CAT')
        
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if f_emp and 'Emprendimiento' in temp.columns: temp = temp[temp['Emprendimiento'].isin(f_emp)]
        return temp

    so_filt = apply_filters(so_f)
    si_filt = apply_filters(si_f)
    stk_filt = apply_filters(stk_f)

    # --- 5. TABS ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Abastecimiento"])

    with tab_control:
        # Foto actual para KPIs y Tortas
        max_date = stk_filt['Fecha_dt'].max() if not stk_filt.empty else None
        stk_snap = stk_filt[stk_filt['Fecha_dt'] == max_date].copy() if max_date else pd.DataFrame()

        # División de Stock por Emprendimiento
        df_dass = stk_snap[stk_snap['Emprendimiento'] == 'DASS CENTRAL']
        df_clie = stk_snap[stk_snap['Emprendimiento'] == 'CLIENTES']

        # KPIs Principales
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Sell Out", f"{so_filt['Cant'].sum():,.0f}")
        k2.metric("Sell In", f"{si_filt['Cant'].sum():,.0f}")
        k3.metric("Stock Dass Central", f"{df_dass['Cant'].sum():,.0f}")
        k4.metric("Stock Clientes", f"{df_clie['Cant'].sum():,.0f}")

        # --- FILA 1: DISCIPLINAS ---
        st.subheader("📌 Análisis por Disciplina")
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        c1.plotly_chart(px.pie(df_dass.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stk Dass Central", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c2.plotly_chart(px.pie(so_filt.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Venta (Sell Out)", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c3.plotly_chart(px.pie(df_clie.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stk Clientes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c4.plotly_chart(px.bar(si_filt.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In Mensual", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # --- FILA 2: FRANJAS ---
        st.subheader("💰 Análisis por Franja")
        f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
        f1.plotly_chart(px.pie(df_dass.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stk Dass Central"), use_container_width=True)
        f2.plotly_chart(px.pie(so_filt.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out"), use_container_width=True)
        f3.plotly_chart(px.pie(df_clie.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stk Clientes"), use_container_width=True)
        f4.plotly_chart(px.bar(si_filt.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In por Franja"), use_container_width=True)

        # --- LÍNEA DE TIEMPO (RECONSTRUIDA) ---
        st.divider()
        st.subheader("📈 Evolución Histórica de Stocks y Ventas")
        # Para la línea de tiempo NO filtramos por mes de la sidebar (usamos apply_filters con False)
        stk_h = apply_filters(stk_f, filter_month=False).groupby(['Mes', 'Emprendimiento'])['Cant'].sum().reset_index()
        so_h = apply_filters(so_f, filter_month=False).groupby('Mes')['Cant'].sum().reset_index()
        
        fig = go.Figure()
        # Línea de Venta
        fig.add_trace(go.Scatter(x=so_h['Mes'], y=so_h['Cant'], name='SELL OUT (Venta)', line=dict(color='#0055A4', width=4)))
        # Líneas dinámicas por cada Emprendimiento de Stock
        for emp in stk_h['Emprendimiento'].unique():
            df_emp_h = stk_h[stk_h['Emprendimiento'] == emp]
            fig.add_trace(go.Scatter(x=df_emp_h['Mes'], y=df_emp_h['Cant'], name=f"STOCK {emp}", mode='lines+markers'))
        
        fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

    with tab_intel:
        st.header("🎯 Inteligencia de Abastecimiento")
        # Muestra tabla simple para validar datos
        st.write("Datos procesados para sugerencia:")
        st.dataframe(so_filt.groupby('SKU')['Cant'].sum().reset_index().sort_values('Cant', ascending=False), use_container_width=True)
