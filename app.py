import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# 1. Configuración visual
st.set_page_config(page_title="Dass | Torre de Control", layout="wide", page_icon="👟")

# Estilo para mejorar las métricas
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 38px; color: #007bff; }
    .main { background-color: #f8f9fa; }
    </style>
    """, unsafe_allow_html=True)

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
                _, done = downloader.next_chunk()
            fh.seek(0)
            
            # Leemos y normalizamos columnas (quitamos tildes para el código)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            
            name = item['name'].replace('.csv', '')
            if 'Sell_out' in name:
                # Agrupamos las 200k filas para que la app sea veloz
                df = df.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df['VPS'] = df['Unidades'] / 4 # Ventas Promedio Semanal
                name = 'Sell_out'
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return None

# --- LÓGICA DE DATOS ---
data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # Unimos Stock con Maestro para tener descripciones
    df_main = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    df_main = df_main.merge(data['Maestro_Productos'], on='SKU', how='left')
    
    # Unimos con Ventas (Sell_out)
    df_main = df_main.merge(data['Sell_out'][['SKU', 'Cliente', 'Ubicacion', 'VPS']], 
                            on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # Cálculo de WOS (Semanas de Stock)
    df_main['WOS'] = df_main.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    # --- DISEÑO DEL DASHBOARD ---
    st.title("👟 Torre de Control de Inventario Dass")
    
    # Sidebar: Filtros
    st.sidebar.header("Filtros Globales")
    clientes = sorted(df_main['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Filtrar por Cliente", clientes, default=clientes[:1])
    
    df_f = df_main[df_main['Cliente'].isin(f_cliente)] if f_cliente else df_main

    # Fila 1: Métricas principales
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Stock Físico", f"{df_f['Stock_Actual'].sum():,.0f}")
    m2.metric("Venta Semanal", f"{df_f['VPS'].sum():,.0f}")
    wos_m = df_f[df_f['WOS'] < 99]['WOS'].mean()
    m3.metric("WOS Promedio", f"{wos_m:.1f} sem")
    m4.metric("SKUs Activos", f"{df_f['SKU'].nunique():,}")

    # Fila 2: Tabla interactiva con colores
    st.subheader("📊 Detalle de Inventario y Cobertura")
    
    # Semáforo de WOS: Rojo < 4 semanas, Amarillo 4-8, Verde > 8
    def color_wos(val):
        if val == 99: return 'color: gray'
        color = 'red' if val < 4 else 'orange' if val < 8 else 'green'
        return f'color: {color}; font-weight: bold'

    st.dataframe(
        df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']]
        .sort_values('WOS')
        .style.map(color_wos, subset=['WOS'])
        .format({'Stock_Actual': '{:,.0f}', 'VPS': '{:,.1f}', 'WOS': '{:,.1f}'}),
        use_container_width=True, height=500
    )

    # Fila 3: Consultas con Inteligencia Artificial
    st.divider()
    st.subheader("🤖 Consultar a la IA")
    chat = st.chat_input("Ej: ¿Qué productos están en riesgo de quiebre de stock?")
    
    if chat:
        with st.spinner("Analizando inventario..."):
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-1.5-flash')
            # Le pasamos un resumen de los datos a la IA
            resumen = df_f.head(20).to_string()
            prompt = f"Como analista de Dass, responde basándote en este resumen de stock:\n{resumen}\nPregunta: {chat}"
            response = model.generate_content(prompt)
            st.info(response.text)

else:
    st.info("Esperando que los archivos CSV se procesen correctamente...")

