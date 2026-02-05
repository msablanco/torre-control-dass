import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Torre Dass", layout="wide")

@st.cache_data(ttl=3600, show_spinner="Descargando datos de Drive...")
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
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            
            # Lectura veloz con detección de tildes
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', on_bad_lines='skip')
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            
            name = item['name'].replace('.csv', '')
            if 'Sell_out' in name:
                # Agrupamos inmediatamente para reducir las 200k filas
                df = df.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df['VPS'] = df['Unidades'] / 4
                name = 'Sell_out'
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error: {e}")
        return None

data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # Unificación
    df_stock = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    df = df_stock.merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(data['Sell_out'][['SKU', 'Cliente', 'Ubicacion', 'VPS']], on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    st.title("👟 Torre de Control Dass")
    
    # Filtro lateral
    cli = sorted(df['Cliente'].unique())
    f_cli = st.sidebar.multiselect("Clientes", cli, default=cli[:1])
    df_f = df[df['Cliente'].isin(f_cli)] if f_cli else df

    # Pantalla principal
    c1, c2, c3 = st.columns(3)
    c1.metric("Stock", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Sem", f"{df_f['VPS'].sum():,.0f}")
    c3.metric("WOS Medio", f"{df_f['WOS'].replace(99, 0).mean():.1f}")

    st.dataframe(df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']], use_container_width=True)
else:
    st.info("Buscando archivos en Drive... Asegúrate de que se llamen Stock.csv, Maestro_Productos.csv y Sell_out.csv")
