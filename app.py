import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.18", layout="wide")

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
            # Detección de separador específica para tus archivos
            df = pd.read_csv(fh, encoding='latin-1', sep=';', engine='python', dtype=str, on_bad_lines='skip')
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. MAESTRO (Mapeo por Posición de Columna) ---
    df_ma_raw = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma = pd.DataFrame()

    if not df_ma_raw.empty:
        # Forzamos nombres de columnas si el CSV no tiene o son distintos
        # Según tu archivo: 0:SKU, 1:Descripcion, 3:Disciplina, 5:Franja
        cols = df_ma_raw.columns.tolist()
        df_ma['SKU'] = df_ma_raw.iloc[:, 0].astype(str).str.strip().str.upper()
        df_ma['Descripcion'] = df_ma_raw.iloc[:, 1].fillna('SIN DESCRIPCION').astype(str).str.upper()
        df_ma['Disciplina'] = df_ma_raw.iloc[:, 3].fillna('OTRO').astype(str).str.upper().str.strip()
        
        # La Franja suele ser la columna 5 o 6 en tu CSV (Pinnacle, Best, etc.)
        if len(cols) >= 6:
            df_ma['FRANJA_PRECIO'] = df_ma_raw.iloc[:, 5].fillna('SIN CAT').astype(str).str.upper().str.strip()
        else:
            df_ma['FRANJA_PRECIO'] = 'SIN CAT'
        
        # Limpieza de valores vacíos que vienen como 'NAN' string
        df_ma['FRANJA_PRECIO'] = df_ma['FRANJA_PRECIO'].replace(['NAN', ''], 'SIN CAT')
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. LIMPIEZA DE TRANSACCIONES ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up'])
        
        # SKU siempre es la primera columna
        df['SKU'] = df.iloc[:, 0].astype(str).str.strip().str.upper()
        
        # Buscar columna de cantidad (suele ser la 4 o 5)
        col_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNID', 'CANT', 'QTY'])), df.columns[min(len(df.columns)-1, 4)])
        df['Cant'] = pd.to_numeric(df[col_cant], errors='coerce').fillna(0)
        
        # Buscar columna de fecha
        col_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'MES'])), df.columns[min(len(df.columns)-1, 3)])
        df['Fecha_dt'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        
        # Cliente
        col_cli = next((c for c in df.columns if 'CLIENTE' in c.upper()), 'Cliente')
        df['Cliente_up'] = df.get(col_cli, 'DESCONOCIDO').fillna('DESCONOCIDO').astype(str).str.upper()
        
        return df[['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up']]

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    f_franja = st.sidebar.multiselect("💰 Franja (Pinnacle/Best/etc)", sorted(df_ma['FRANJA_PRECIO'].unique()))
    f_cli_so = st.sidebar.multiselect("👤 Cliente Sell Out", sorted(so_raw['Cliente_up'].unique()))
    
    selected_clients = set(f_cli_so)

    def apply_logic(df, filter_month=True):
        temp = df.copy()
        if temp.empty: return temp
        
        # El Merge CRUCIAL con el Maestro corregido
        temp = temp.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion', 'Busqueda']], on='SKU', how='left')
        temp['Disciplina'] = temp['Disciplina'].fillna('OTRO')
        temp['FRANJA_PRECIO'] = temp['FRANJA_PRECIO'].fillna('SIN CAT')
        
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if search_query: temp = temp[temp['Busqueda'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if selected_clients: temp = temp[temp['Cliente_up'].isin(selected_clients)]
            
        return temp

    so_f = apply_logic(so_raw)
    si_f = apply_logic(si_raw)
    stk_f = apply_logic(stk_raw)

    # --- 5. TABS ---
    tab_control, tab_intel = st.tabs(["📊 Torre de Control", "🚨 Inteligencia de Abastecimiento"])

    with tab_control:
        max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
        stk_snap = stk_f[stk_f['Fecha_dt'] == max_date] if max_date else pd.DataFrame()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Sell Out", f"{so_f['Cant'].sum():,.0f}")
        k2.metric("Sell In", f"{si_f['Cant'].sum():,.0f}")
        
        is_dass = stk_snap['Cliente_up'].str.contains('DASS', na=False)
        val_dass = stk_snap[is_dass]['Cant'].sum()
        k3.metric("Stock Dass", f"{val_dass:,.0f}")
        
        val_cli = stk_snap[~is_dass]['Cant'].sum()
        k4.metric("Stock Cliente", f"{val_cli:,.0f}")

        # Gráficos de Disciplina
        st.subheader("📌 Análisis por Disciplina")
        c1, c2, c3 = st.columns(3)
        c1.plotly_chart(px.pie(stk_snap[is_dass].groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c3.plotly_chart(px.pie(stk_snap[~is_dass].groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

        # Gráficos de Franja (AQUÍ ESTÁ EL FIX)
        st.subheader("💰 Análisis por Franja (Pinnacle, Best, Good)")
        f1, f2, f3 = st.columns(3)
        f1.plotly_chart(px.pie(stk_snap[is_dass].groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Dass (Franja)"), use_container_width=True)
        f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Sell Out (Franja)"), use_container_width=True)
        f3.plotly_chart(px.pie(stk_snap[~is_dass].groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Stock Cliente (Franja)"), use_container_width=True)

        st.divider()
        st.subheader("📋 Detalle de Productos")
        t_so = so_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out'})
        t_stk_d = stk_snap[is_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        t_stk_c = stk_snap[~is_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Cliente'})
        
        df_det = df_ma[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
        st.dataframe(df_det[df_det[['Sell Out', 'Stock Dass', 'Stock Cliente']].sum(axis=1) > 0].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

    with tab_intel:
        st.header("🚨 Inteligencia de Abastecimiento")
        # (Lógica de sugerencia de compra aquí...)
