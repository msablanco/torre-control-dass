import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Torre de Control Dass", layout="wide")

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
            
            # Lectura robusta: detecta separador y maneja caracteres especiales
            df_temp = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', on_bad_lines='skip')
            
            # LIMPIEZA TOTAL: Quitamos tildes y espacios de los nombres de columnas
            df_temp.columns = (df_temp.columns.str.strip()
                               .str.replace('ó', 'o', regex=False)
                               .str.replace('á', 'a', regex=False))
            
            # Si es el archivo de 200k filas, lo resumimos apenas entra
            if name == 'Sell_out':
                # Agrupamos por SKU, Cliente y Ubicacion para reducir tamaño un 90%
                df_temp = df_temp.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df_temp['VPS'] = df_temp['Unidades'] / 4
            
            dfs[name] = df_temp
        return dfs
    except Exception as e:
        st.error(f"Error técnico en carga: {e}")
        return None

data = load_data()

# Verificamos que los archivos básicos existan
if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # Normalizamos nombres de columnas de Stock según tu foto
    df_stock = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    
    # Cruce de datos (Merge)
    df = df_stock.merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(data['Sell_out'][['SKU', 'Cliente', 'Ubicacion', 'VPS']], 
                  on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # Cálculo de WOS (Semanas de inventario)
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    # --- INTERFAZ ---
    st.title("👟 Torre de Control Dass")
    
    # Filtro lateral dinámico
    st.sidebar.header("Filtros")
    clientes = sorted(df['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Seleccionar Cliente", clientes, default=clientes[:2] if clientes else [])
    
    df_f = df[df['Cliente'].isin(f_cliente)] if f_cliente else df

    # Indicadores principales
    col1, col2, col3 = st.columns(3)
    col1.metric("Stock Seleccionado", f"{df_f['Stock_Actual'].sum():,.0f}")
    col2.metric("Venta Semanal (Prom)", f"{df_f['VPS'].sum():,.0f}")
    col3.metric("WOS Promedio", f"{df_f['WOS'].replace(99, 0).mean():.1f} sem")

    # Tabla con los datos
    st.subheader("📋 Detalle de Inventario")
    st.dataframe(df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']], use_container_width=True)

    # IA de Gemini para análisis
    st.divider()
    user_q = st.chat_input("Pregunta algo sobre el stock (ej: ¿Qué clientes tienen poco WOS?)")
    if user_q:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        contexto = df_f.head(20).to_string()
        resp = model.generate_content(f"Contexto Dass:\n{contexto}\nPregunta: {user_q}")
        st.info(resp.text)

else:
    st.warning("⚠️ No se encontraron los archivos con los nombres correctos: Stock.csv, Maestro_Productos.csv, Sell_out.csv")
    st.info("Asegúrate de que los archivos en Drive se llamen exactamente: Stock.csv, Maestro_Productos.csv y Sell_out.csv")


