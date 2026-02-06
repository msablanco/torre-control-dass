import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v5.6", layout="wide")

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
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

data = load_data()

if data:
    # --- 1. PROCESAMIENTO ---
    df_base = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    def clean_agg(df_name, val_col, new_name):
        if df_name in data:
            df_tmp = data[df_name].copy()
            df_tmp[new_name] = pd.to_numeric(df_tmp[val_col], errors='coerce').fillna(0)
            return df_tmp.groupby('SKU').agg({new_name: 'sum', 'Cliente': 'first'}).reset_index()
        return pd.DataFrame(columns=['SKU', new_name])

    si = clean_agg('Sell_in', 'Unidades', 'Sell In')
    so = clean_agg('Sell_out', 'Unidades', 'Sell Out')
    
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    if not stk_raw.empty:
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Ubicacion'] = stk_raw['Ubicacion'].fillna('').astype(str).str.upper()
        mask_dass = stk_raw['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)
        st_dass = stk_raw[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        st_cli = stk_raw[~mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    else:
        st_dass = st_cli = pd.DataFrame(columns=['SKU', 'Stock Dass', 'Stock Clientes'])

    df = df_base.merge(si, on='SKU', how='left').merge(so, on='SKU', how='left')
    df = df.merge(st_dass, on='SKU', how='left').merge(st_cli, on='SKU', how='left').fillna(0)

    # --- 2. FILTROS INTELIGENTES ---
    df_active = df[(df['Sell In'] > 0) | (df['Sell Out'] > 0) | (df['Stock Clientes'] > 0)].copy()
    
    st.sidebar.header("🔍 Filtros")
    cli_opt = sorted([str(x) for x in df_active['Cliente'].unique() if x not in ['0', 'nan']])
    sel_cli = st.sidebar.multiselect("Cliente", cli_opt)
    disc_opt = sorted([str(x) for x in df_active['Disciplina'].unique() if x not in ['0', 'nan']])
    sel_disc = st.sidebar.multiselect("Franja (Disciplina)", disc_opt)

    if sel_cli: df = df[df['Cliente'].isin(sel_cli)]
    if sel_disc: df = df[df['Disciplina'].isin(sel_disc)]

    # --- 3. DASHBOARD VISUAL ---
    st.title("👟 Dashboard de Gestión por Franja")

    # Gráficos DASS (Fila 1)
    st.subheader("🏢 Gestión de Inventario y Despacho DASS")
    col1, col2 = st.columns(2)
    
    fig_stk_dass = px.pie(df[df['Stock Dass']>0], values='Stock Dass', names='Disciplina', title="Participación Stock DASS")
    col1.plotly_chart(fig_stk_dass, use_container_width=True)
    
    fig_si_dass = px.pie(df[df['Sell In']>0], values='Sell In', names='Disciplina', title="Participación Sell In (Ingresos)")
    col2.plotly_chart(fig_si_dass, use_container_width=True)

    st.divider()

    # Gráficos Clientes (Fila 2)
    st.subheader("🛍️ Performance en Punto de Venta (Clientes)")
    col3, col4, col5 = st.columns(3)
    
    fig_so_cli = px.pie(df[df['Sell Out']>0], values='Sell Out', names='Disciplina', title="Sell Out por Franja")
    col3.plotly_chart(fig_so_cli, use_container_width=True)
    
    fig_stk_cli = px.pie(df[df['Stock Clientes']>0], values='Stock Clientes', names='Disciplina', title="Stock Clientes por Franja")
    col4.plotly_chart(fig_stk_cli, use_container_width=True)
    
    # Ingresos (Usando Sell In como proxy de ingresos al canal)
    fig_ing = px.pie(df[df['Sell In']>0], values='Sell In', names='Disciplina', title="Ingresos por Franja", hole=0.3)
    col5.plotly_chart(fig_ing, use_container_width=True)

    # --- 4. TABLA DETALLADA ---
    st.divider()
    st.subheader("📋 Detalle Unificado")
    
    df['Sell Through %'] = np.where(df['Sell In'] > 0, (df['Sell Out'] / df['Sell In']) * 100, 0)
    
    cols = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %']
    st.dataframe(
        df[cols].style.format({'Sell In': '{:,.0f}', 'Sell Out': '{:,.0f}', 'Stock Clientes': '{:,.0f}', 'Stock Dass': '{:,.0f}', 'Sell Through %': '{:.1f}%'}),
        use_container_width=True, height=400
    )

else:
    st.info("Cargando datos desde Drive...")
