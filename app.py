import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Torre de Control Dass", layout="wide")

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
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            name = item['name'].replace('.csv', '')
            # Lectura optimizada para archivos grandes y formato Excel
            dfs[name] = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', on_bad_lines='skip')
            dfs[name].columns = dfs[name].columns.str.strip() # Limpiar espacios en nombres
        return dfs
    except Exception as e:
        st.error(f"Error: {e}")
        return None

data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # 1. OPTIMIZACIÓN: Procesar Sell Out rápido
    df_out = data['Sell_out']
    # Si detecta que es muy grande, sumamos primero por SKU/Cliente para alivianar
    vps = df_out.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
    vps['VPS'] = vps['Unidades'] / 4 # Venta promedio últimas 4 semanas

    # 2. UNIFICACIÓN (Ajustado a tus fotos: Cantidad vs Unidades)
    df_stock = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    
    # Cruce de datos
    df = df_stock.merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(vps[['SKU', 'Cliente', 'Ubicacion', 'VPS']], on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # 3. CÁLCULO DE WOS
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    # --- INTERFAZ ---
    st.title("👟 Torre de Control Dass")
    
    # Filtros rápidos
    st.sidebar.header("Filtros")
    f_cliente = st.sidebar.multiselect("Filtrar Cliente", df['Cliente'].unique(), default=df['Cliente'].unique()[:5])
    
    df_f = df[df['Cliente'].isin(f_cliente)]

    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Stock Total", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Semanal", f"{df_f['VPS'].sum():,.0f}")
    c3.metric("WOS Promedio", f"{df_f['WOS'].replace(99, 0).mean():.1f}")

    # Tabla Principal
    st.dataframe(df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']], use_container_width=True)

    # IA (Gemini)
    st.divider()
    user_query = st.chat_input("Pregúntale a la IA sobre el stock...")
    if user_query:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        contexto = df_f.head(20).to_string()
        response = model.generate_content(f"Datos: {contexto}\nPregunta: {user_query}")
        st.write(response.text)
else:
    st.warning("Verifica que los archivos en Drive se llamen: Stock.csv, Maestro_Productos.csv y Sell_out.csv")
