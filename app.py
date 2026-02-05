import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Performance Dass v5.0", layout="wide")

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
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            # Normalización de nombres de columnas
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error cargando archivos: {e}")
        return None

data = load_data()

# Verificamos que tengamos la información necesaria
if data:
    # 1. Base: Maestro de Productos
    df_final = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # 2. Procesar Sell Out (Venta de Clientes al público)
    if 'Sell_out' in data:
        so = data['Sell_out'].groupby('SKU')['Unidades'].sum().reset_index()
        so.columns = ['SKU', 'Sell Out']
        df_final = df_final.merge(so, on='SKU', how='left')

    # 3. Procesar Sell In (Lo que Dass le vendió a los Clientes)
    if 'Sell_in' in data:
        si = data['Sell_in'].groupby('SKU')['Unidades'].sum().reset_index()
        si.columns = ['SKU', 'Sell In']
        df_final = df_final.merge(si, on='SKU', how='left')

    # 4. Procesar Stock Clientes (Ubicación != 'DASS' o similar según tu archivo)
    if 'Stock' in data:
        # Stock Dass (Asumimos que el archivo de stock tiene una columna 'Ubicacion')
        s_dass = data['Stock'][data['Stock']['Ubicacion'].str.contains('DASS|DEPOSITO', na=False, case=False)]
        s_dass = s_dass.groupby('SKU')['Cantidad'].sum().reset_index()
        s_dass.columns = ['SKU', 'Stock Dass']
        
        # Stock Clientes (Lo que no es Dass)
        s_cli = data['Stock'][~data['Stock']['Ubicacion'].str.contains('DASS|DEPOSITO', na=False, case=False)]
        s_cli = s_cli.groupby('SKU')['Cantidad'].sum().reset_index()
        s_cli.columns = ['SKU', 'Stock Clientes']
        
        df_final = df_final.merge(s_dass, on='SKU', how='left')
        df_final = df_final.merge(s_cli, on='SKU', how='left')

    # Limpieza final de la tabla
    df_final = df_final.fillna(0)
    
    # Columnas requeridas por vos
    columnas_ok = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass']
    
    # Solo mostrar las columnas que existan en el merge
    df_display = df_final[[c for c in columnas_ok if c in df_final.columns]]

    # --- INTERFAZ ---
    st.title("👟 Desaborad Performance - Calzado v5.0")
    
    # Filtros sidebar
    if 'Disciplina' in df_display.columns:
        disc = st.sidebar.multiselect("Disciplina", df_display['Disciplina'].unique())
        if disc: df_display = df_display[df_display['Disciplina'].isin(disc)]

    # Mostrar Tabla Principal
    st.subheader("Análisis Consolidado de Inventario y Ventas")
    st.dataframe(
        df_display.style.format({
            'Sell In': '{:,.0f}',
            'Sell Out': '{:,.0f}',
            'Stock Clientes': '{:,.0f}',
            'Stock Dass': '{:,.0f}'
        }),
        use_container_width=True,
        height=600
    )

    # Cálculo de Cobertura Total (Stock Total / Sell Out Mensual)
    st.divider()
    stock_total = df_display['Stock Clientes'].sum() + df_display['Stock Dass'].sum()
    venta_total = df_display['Sell Out'].sum()
    cobertura = stock_total / venta_total if venta_total > 0 else 0
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Stock Consolidado", f"{stock_total:,.0f}")
    c2.metric("Sell Out Total", f"{venta_total:,.0f}")
    c3.metric("Cobertura Global (Meses)", f"{cobertura:.2f}")

else:
    st.warning("No se pudieron cargar los datos. Revisa la conexión con Google Drive.")

