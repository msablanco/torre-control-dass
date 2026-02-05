import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Dass Performance v5.2", layout="wide")

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
        st.error(f"Error: {e}")
        return None

data = load_data()

if data:
    # --- 1. PREPARACIÓN DE DATOS ---
    maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # Función para sumar valores numéricos por SKU y opcionalmente por Cliente
    def get_sum(df_name, val_col):
        if df_name in data:
            df = data[df_name].copy()
            df[val_col] = pd.to_numeric(df[val_col], errors='coerce').fillna(0)
            # Agrupamos para unificar duplicados
            return df.groupby('SKU').agg({val_col: 'sum', 'Cliente': 'first' if 'Cliente' in df.columns else lambda x: 'N/A'}).reset_index()
        return pd.DataFrame(columns=['SKU', val_col, 'Cliente'])

    si = get_sum('Sell_in', 'Unidades').rename(columns={'Unidades': 'Sell In'})
    so = get_sum('Sell_out', 'Unidades').rename(columns={'Unidades': 'Sell Out'})
    
    # Procesar Stock
    stk_df = data.get('Stock', pd.DataFrame()).copy()
    if not stk_df.empty:
        stk_df['Cant'] = pd.to_numeric(stk_df['Cantidad'], errors='coerce').fillna(0)
        stk_df['Ubicacion'] = stk_df['Ubicacion'].astype(str).str.upper()
        mask_dass = stk_df['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)
        
        stk_d = stk_df[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        stk_c = stk_df[~mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    else:
        stk_d = stk_c = pd.DataFrame(columns=['SKU', 'Stock Dass', 'Stock Clientes'])

    # --- 2. CONSOLIDACIÓN FINAL ---
    df = maestro.merge(si, on='SKU', how='left').merge(so, on='SKU', how='left')
    df = df.merge(stk_d, on='SKU', how='left').merge(stk_c, on='SKU', how='left').fillna(0)

    # --- 3. CÁLCULOS SOLICITADOS ---
    df['Ingresos'] = df['Sell In'] # En este modelo ingresos = unidades sell in
    df['Sell Through %'] = (df['Sell Out'] / df['Sell In'] * 100).replace([float('inf'), -float('inf')], 0).fillna(0)
    # Rotación: Stock Clientes vs Sell Out (cuántos meses dura el stock en calle)
    df['Rotacion'] = (df['Stock Clientes'] / df['Sell Out']).replace([float('inf'), -float('inf')], 0).fillna(0)

    # --- 4. FILTROS SUPERIORES ---
    st.title("👟 Performance Dass v5.2")
    
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        lista_clientes = sorted(df['Cliente'].unique()) if 'Cliente' in df.columns else ['N/A']
        f_cliente = st.multiselect("Filtrar Clientes", lista_clientes)
    with col_b:
        f_disc = st.multiselect("Filtrar Disciplina", sorted(df['Disciplina'].unique()) if 'Disciplina' in df.columns else [])
    
    # Aplicar Filtros
    if f_cliente: df = df[df['Cliente'].isin(f_cliente)]
    if f_disc: df = df[df['Disciplina'].isin(f_disc)]

    # --- 5. TABLA FORMATEADA ---
    st.subheader("📊 Tabla de Performance Consolidada")
    
    # Definimos columnas exactas y el formato (solo si existen)
    cols_finales = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion']
    df_view = df[[c for c in cols_finales if c in df.columns]]

    # Formateo seguro para evitar el StreamlitAPIException
    format_dict = {}
    for c in ['Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass']:
        if c in df_view.columns: format_dict[c] = "{:,.0f}"
    if 'Sell Through %' in df_view.columns: format_dict['Sell Through %'] = "{:.1f}%"
    if 'Rotacion' in df_view.columns: format_dict['Rotacion'] = "{:.2f} m"

    st.dataframe(df_view.style.format(format_dict), use_container_width=True, height=500)

    # KPIs Inferiores
    st.divider()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell In Total", f"{df_view['Sell In'].sum():,.0f}")
    k2.metric("Sell Out Total", f"{df_view['Sell Out'].sum():,.0f}")
    k3.metric("Stock Total", f"{(df_view['Stock Clientes'].sum() + df_view['Stock Dass'].sum()):,.0f}")
    st_global = (df_view['Sell Out'].sum() / df_view['Sell In'].sum() * 100) if df_view['Sell In'].sum() > 0 else 0
    k4.metric("Sell Through Global", f"{st_global:.1f}%")

else:
    st.info("Procesando datos de Drive...")
