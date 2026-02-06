import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np

st.set_page_config(page_title="Dass Performance v5.4", layout="wide")

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
    # --- 1. PROCESAMIENTO DE UNIFICACIÓN ---
    # Usamos el Maestro como esqueleto base
    df_base = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    def get_clean_sum(df_key, col_val, new_name):
        if df_key in data:
            temp = data[df_key].copy()
            temp[new_name] = pd.to_numeric(temp[col_val], errors='coerce').fillna(0)
            # Rescatamos el cliente si existe para el filtro
            if 'Cliente' in temp.columns:
                return temp.groupby('SKU').agg({new_name: 'sum', 'Cliente': 'first'}).reset_index()
            return temp.groupby('SKU')[new_name].sum().reset_index()
        return pd.DataFrame(columns=['SKU', new_name])

    # Unificar por SKU cada origen de datos
    si = get_clean_sum('Sell_in', 'Unidades', 'Sell In')
    so = get_clean_sum('Sell_out', 'Unidades', 'Sell Out')
    
    # Procesar Stock separando por Ubicación
    stk_df = data.get('Stock', pd.DataFrame()).copy()
    if not stk_df.empty:
        stk_df['Cant'] = pd.to_numeric(stk_df['Cantidad'], errors='coerce').fillna(0)
        stk_df['Ubicacion'] = stk_df['Ubicacion'].fillna('').astype(str).str.upper()
        
        mask_dass = stk_df['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)
        st_dass = stk_df[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        st_cli = stk_df[~mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    else:
        st_dass = st_cli = pd.DataFrame(columns=['SKU', 'Stock Dass', 'Stock Clientes'])

    # --- 2. MERGE Y CÁLCULOS DE NEGOCIO ---
    df = df_base.merge(si, on='SKU', how='left').merge(so, on='SKU', how='left')
    df = df.merge(st_dass, on='SKU', how='left').merge(st_cli, on='SKU', how='left')
    
    # Limpieza crucial para evitar el error de Streamlit
    df = df.fillna(0)
    
    # Nuevas Columnas Solicitadas
    df['Ingresos'] = df['Sell In']
    # Sell Through = Venta vs Ingreso
    df['Sell Through %'] = np.where(df['Sell In'] > 0, (df['Sell Out'] / df['Sell In']) * 100, 0)
    # Rotación = Stock en Clientes / Venta Mensual
    df['Rotacion (Meses)'] = np.where(df['Sell Out'] > 0, (df['Stock Clientes'] / df['Sell Out']), 0)

    # --- 3. FILTROS SUPERIORES ---
    st.title("👟 Performance Consolidado Dass v5.4")
    
    c1, c2 = st.columns(2)
    with c1:
        if 'Cliente' in df.columns:
            list_cli = sorted([str(x) for x in df['Cliente'].unique() if x != '0'])
            f_cli = st.multiselect("Filtrar por Clientes", list_cli)
            if f_cli: df = df[df['Cliente'].isin(f_cli)]
    with c2:
        if 'Disciplina' in df.columns:
            list_dis = sorted([str(x) for x in df['Disciplina'].unique() if x != '0'])
            f_dis = st.multiselect("Filtrar por Disciplina", list_dis)
            if f_dis: df = df[df['Disciplina'].isin(f_dis)]

    # --- 4. VISUALIZACIÓN ---
    st.subheader("📋 Pipeline de Inventario y Performance")
    
    cols_order = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion (Meses)']
    df_view = df[[c for c in cols_order if c in df.columns]].copy()

    # Formateo Seguro: Creamos un diccionario solo para las columnas que existen
    format_map = {}
    for c in ['Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass']:
        if c in df_view.columns: format_map[c] = "{:,.0f}"
    if 'Sell Through %' in df_view.columns: format_map['Sell Through %'] = "{:.1f}%"
    if 'Rotacion (Meses)' in df_view.columns: format_map['Rotacion (Meses)'] = "{:.2f} m"

    try:
        st.dataframe(
            df_view.style.format(format_map),
            use_container_width=True, 
            height=600
        )
    except:
        # Si falla el estilo, mostramos la tabla cruda para no detener la app
        st.dataframe(df_view, use_container_width=True, height=600)

    # Resumen Ejecutivo
    st.divider()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell In Total", f"{df_view['Sell In'].sum():,.0f}")
    k2.metric("Sell Out Total", f"{df_view['Sell Out'].sum():,.0f}")
    k3.metric("Stock en Clientes", f"{df_view['Stock Clientes'].sum():,.0f}")
    k4.metric("Stock en Dass", f"{df_view['Stock Dass'].sum():,.0f}")

else:
    st.info("Conectando con Google Drive...")
