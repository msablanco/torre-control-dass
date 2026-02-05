import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Torre de Control Dass", layout="wide")

@st.cache_data(ttl=3600) # Cache de 1 hora para no re-procesar las 200k filas
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
                status, done = downloader.next_chunk()
            fh.seek(0)
            name = item['name'].replace('.csv', '')
            
            # Carga optimizada: usamos engine='c' para velocidad y saltamos líneas malas
            df_temp = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', on_bad_lines='skip')
            df_temp.columns = df_temp.columns.str.strip()
            
            # Si es el archivo pesado, lo achicamos de inmediato
            if name == 'Sell_out':
                df_temp = df_temp.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df_temp['VPS'] = df_temp['Unidades'] / 4
            
            dfs[name] = df_temp
        return dfs
    except Exception as e:
        st.error(f"Error técnico: {e}")
        return None

data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # Unificación basada en tus capturas (Cantidad vs Unidades)
    df_stock = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    
    # Merge inteligente
    df = df_stock.merge(data['Maestro_Productos'], on='SKU', how='left')
    # Unimos con las ventas ya resumidas
    df = df.merge(data['Sell_out'][['SKU', 'Cliente', 'Ubicacion', 'VPS']], 
                  on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    st.title("👟 Torre de Control Dass")
    
    # Filtro lateral para no saturar la pantalla
    clientes_disponibles = sorted(df['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Seleccionar Clientes", clientes_disponibles, default=clientes_disponibles[:3])
    
    df_f = df[df['Cliente'].isin(f_cliente)]

    # Visualización
    c1, c2, c3 = st.columns(3)
    c1.metric("Stock en Pantalla", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Semanal", f"{df_f['VPS'].sum():,.0f}")
    c3.metric("WOS Promedio", f"{df_f['WOS'].replace(99, 0).mean():.1f}")

    st.dataframe(df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']], use_container_width=True)

else:
    st.info("Asegúrate de que los archivos en Drive se llamen exactamente: Stock.csv, Maestro_Productos.csv y Sell_out.csv")

