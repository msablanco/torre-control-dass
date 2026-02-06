import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v7.0", layout="wide")

# --- MAPA DE COLORES FIJOS POR DISCIPLINA ---
COLOR_MAP = {
    'SPORTSWEAR': '#0055A4', 
    'RUNNING': '#87CEEB',    
    'TRAINING': '#FF3131',   
    'HERITAGE': '#00A693',   
    'KIDS': '#FFB6C1',       
    'TENNIS': '#FFD700',     
    'TENIS': '#FFD700',
    'SANDALS': '#90EE90',    
    'OUTDOOR': '#8B4513',
    'FOOTBALL': '#000000',
    'FUTBOL': '#000000'
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
        st.error(f"Error Drive: {e}")
        return None

data = load_data()

if data:
    # --- 1. PROCESAMIENTO MAESTRO (Validación Robusta) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # Validación de FRANJA_PRECIO
    if 'FRANJA_PRECIO' not in df_ma.columns:
        df_ma['FRANJA_PRECIO'] = 'SIN CATEGORIA'
    else:
        df_ma['FRANJA_PRECIO'] = df_ma['FRANJA_PRECIO'].fillna('SIN CATEGORIA')
    
    # Validación de Disciplina
    if 'Disciplina' not in df_ma.columns:
        df_ma['Disciplina'] = 'OTRO'
    else:
        df_ma['Disciplina'] = df_ma['Disciplina'].fillna('OTRO')

    df_ma['FRANJA_PRECIO'] = df_ma['FRANJA_PRECIO'].astype(str).str.upper()
    df_ma['Disciplina'] = df_ma['Disciplina'].astype(str).str.upper()

    # --- 2. STOCK (Lógica Cliente DASS) ---
    stk_raw = data.get('Stock', pd.DataFrame())
    st_dass_grp = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    st_cli_grp = pd.DataFrame(columns=['SKU', 'Stock Clientes', 'Cliente'])
    
    if not stk_raw.empty:
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Cliente_stk'] = stk_raw['Cliente'].fillna('').astype(str).str.upper().str.strip()
        mask_dass = stk_raw['Cliente_stk'].str.contains('DASS', na=False)
        
        st_dass_grp = stk_raw[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        st_cli_grp = stk_raw[~mask_dass].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

    # --- 3. VENTAS ---
    si_raw = data.get('Sell_in', pd.DataFrame())
    si_grp = pd.DataFrame(columns=['SKU', 'Sell in', 'Cliente'])
    if not si_raw.empty:
        si_raw['Sell in'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        si_grp = si_raw.groupby(['SKU', 'Cliente'])['Sell in'].sum().reset_index()

    so_raw = data.get('Sell_out', pd.DataFrame())
    so_final = pd.DataFrame(columns=['SKU', 'Sell out Clientes', 'Cliente'])
    if not so_raw.empty:
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_final = so_raw.groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Clientes'})

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros de Gestión")
    clis_all = sorted(list(set(si_grp['Cliente'].dropna().unique().tolist() + st_cli_grp['Cliente'].dropna().unique().tolist())))
    f_cli = st.sidebar.multiselect("Cliente", [c for c in clis_all if str(c) not in ['DASS', '0', 'nan']])
    f_dis = st.sidebar.multiselect("Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))

    if f_cli:
        si_grp = si_grp[si_grp['Cliente'].isin(f_cli)]
        so_final = so_final[so_final['Cliente'].isin(f_cli)]
        st_cli_grp = st_cli_grp[st_cli_grp['Cliente'].isin(f_cli)]
    
    df = df_ma.merge(st_dass_grp, on='SKU', how='left').merge(si_grp.groupby('SKU')['Sell in'].sum().reset_index(), on='SKU', how='left')
    df = df.merge(so_final.groupby('SKU')['Sell out Clientes'].sum().reset_index(), on='SKU', how='left')
    df = df.merge(st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index(), on='SKU', how='left').fillna(0)

    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]

    # --- 5. DASHBOARD ---
    st.title("📊 Torre de Control Dass v7.0")

    def safe_pie_colored(dataframe, val_col, name_col, title_str, col_target, use_map=False):
        clean_df = dataframe[dataframe[val_col] > 0]
        if not clean_df.empty:
            if use_map:
                fig = px.pie(clean_df, values=val_col, names=name_col, title=title_str, 
                             color=name_col, color_discrete_map=COLOR_MAP)
            else:
                fig = px.pie(clean_df, values=val_col, names=name_col, title=title_str)
            fig.update_traces(textinfo='percent+label')
            col_target.plotly_chart(fig, use_container_width=True)
        else:
            col_target.warning(f"Sin datos: {title_str}")

    st.subheader("📌 Participación por Disciplina (Colores Fijos)")
    g1, g2, g3 = st.columns(3)
    safe_pie_colored(df, 'Stock Dass', 'Disciplina', "Stock Dass (Foto)", g1, use_map=True)
    safe_pie_colored(df, 'Sell in', 'Disciplina', "Ingresos (Sell In)", g2, use_map=True)
    safe_pie_colored(df, 'Sell out Clientes', 'Disciplina', "Sell Out Clientes", g3, use_map=True)

    st.subheader("🏆 Participación por Franja")
    p1, p2, p3 = st.columns(3)
    safe_pie_colored(df, 'Stock Dass', 'FRANJA_PRECIO', "Stock Dass / Franja", p1)
    safe_pie_colored(df, 'Sell in', 'FRANJA_PRECIO', "Ingresos / Franja", p2)
    safe_pie_colored(df, 'Sell out Clientes', 'FRANJA_PRECIO', "Sell Out / Franja", p3)

    # --- 6. RANKING ---
    st.divider()
    df['WOS'] = np.where(df['Sell out Clientes']>0, df['Stock Clientes']/df['Sell out Clientes'], 0)
    st.dataframe(df[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell in', 'Sell out Clientes', 'Stock Dass', 'Stock Clientes', 'WOS']].sort_values('Sell out Clientes', ascending=False), use_container_width=True)

else:
    st.info("Conectando con Drive...")
