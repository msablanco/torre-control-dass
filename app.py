import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Torre de Control Dass", layout="wide", page_icon="👟")

@st.cache_data(ttl=3600)
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
            
            # Cargamos con motor optimizado
            df_temp = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', on_bad_lines='skip')
            
            # NORMALIZACIÓN DE COLUMNAS (Quita tildes, espacios y pasa a minúsculas)
            df_temp.columns = (df_temp.columns.str.strip()
                               .str.replace('ó', 'o').str.replace('á', 'a')
                               .str.replace('é', 'e').str.replace('í', 'i')
                               .str.replace('ú', 'u'))
            
            # Procesar el archivo pesado de 200k filas reduciéndolo de inmediato
            if name == 'Sell_out':
                df_temp = df_temp.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df_temp['VPS'] = df_temp['Unidades'] / 4
            
            dfs[name] = df_temp
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return None

data = load_data()

# Validar que existan los 3 archivos clave
if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # Ajustamos nombres de columnas de tus fotos
    df_stock = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    
    # Cruce de datos (Merge)
    df = df_stock.merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(data['Sell_out'][['SKU', 'Cliente', 'Ubicacion', 'VPS']], 
                  on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # Semanas de Inventario (WOS)
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    # --- DASHBOARD ---
    st.title("👟 Torre de Control Dass")
    
    # Sidebar con filtros para mejorar performance
    st.sidebar.header("Filtros")
    clientes = sorted(df['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Seleccionar Cliente", clientes, default=clientes[:1])
    
    df_f = df[df['Cliente'].isin(f_cliente)]

    # Métricas
    c1, c2, c3 = st.columns(3)
    c1.metric("Stock Actual", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Semanal", f"{df_f['VPS'].sum():,.0f}")
    c3.metric("WOS Promedio", f"{df_f['WOS'].replace(99, 0).mean():.1f}")

    # Visualización de Tabla
    st.dataframe(df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']], use_container_width=True)

    # Consultas IA
    st.divider()
    user_input = st.chat_input("Pregúntale a la IA (ej: ¿Cuáles son los SKUs con WOS crítico?)")
    if user_input:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        contexto = df_f.head(15).to_string()
        response = model.generate_content(f"Análisis Dass:\n{contexto}\nPregunta: {user_input}")
        st.info(response.text)
else:
    st.warning("Verifica los nombres: Stock.csv, Maestro_Productos.csv y Sell_out.csv")



