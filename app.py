import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Dass Performance Engine", layout="wide", page_icon="📊")

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
            # Normalizar columnas: quitar tildes y espacios
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            
            name = item['name'].replace('.csv', '')
            if 'Sell_out' in name:
                # Agrupación Mensual de Unidades
                df = df.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df = df.rename(columns={'Unidades': 'Venta_Mensual'})
                name = 'Sell_out'
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return None

data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # UNIFICACIÓN DE DATOS (Relación Sell Out / Stock)
    df = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    # Cruzamos con Sell Out por SKU, Cliente y Ubicacion
    df = df.merge(data['Sell_out'], on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # --- LÓGICA DE ANÁLISIS (Referencia HTML v5.0) ---
    
    # 1. Cobertura Mensual (Stock Actual / Venta Mensual)
    df['Cobertura'] = df.apply(lambda x: x['Stock_Actual'] / x['Venta_Mensual'] if x['Venta_Mensual'] > 0 else 12, axis=1)
    
    # 2. Sugerencia de Compra (Target: 3 meses de venta como inventario ideal)
    MESES_TARGET = 3 
    df['Sugerido_Compra'] = df.apply(lambda x: max(0, (x['Venta_Mensual'] * MESES_TARGET) - x['Stock_Actual']), axis=1)

    # --- DASHBOARD ---
    st.title("📊 Análisis de Performance y Cobertura | Dass")
    
    # Filtros laterales
    st.sidebar.header("Filtros de Análisis")
    clientes = sorted(df['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Seleccionar Clientes", clientes, default=clientes[:1])
    
    df_f = df[df['Cliente'].isin(f_cliente)]

    # MÉTRICAS DE GESTIÓN
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock Físico", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Mensual (Sell Out)", f"{df_f['Venta_Mensual'].sum():,.0f}")
    c3.metric("Sugerido Reposición", f"{df_f['Sugerido_Compra'].sum():,.0f}")
    cob_media = df_f[df_f['Cobertura'] < 12]['Cobertura'].mean()
    c4.metric("Cobertura Promedio", f"{cob_media:.1f} meses")

    # TABLA DE ANÁLISIS (Sin columna Articulo)
    st.subheader("📋 Sugerencia de Compra y Estado de Cobertura")
    
    def semaforo_cobertura(val):
        # Rojo: Riesgo de quiebre (< 1 mes), Naranja: Bajo stock (1-2), Verde: Saludable (> 2)
        if val == 12: return 'color: gray'
        color = '#E53935' if val < 1 else '#FB8C00' if val < 2 else '#43A047'
        return f'color: {color}; font-weight: bold'

    # Mostramos solo columnas de datos y análisis
    st.dataframe(
        df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'Venta_Mensual', 'Cobertura', 'Sugerido_Compra']]
        .sort_values(by='Sugerido_Compra', ascending=False)
        .style.map(semaforo_cobertura, subset=['Cobertura'])
        .format({'Stock_Actual': '{:,.0f}', 'Venta_Mensual': '{:,.0f}', 'Cobertura': '{:,.1f} m', 'Sugerido_Compra': '{:,.0f}'}),
        use_container_width=True, height=500
    )

    # ANALISTA IA
    st.divider()
    st.subheader("🤖 Consultar Análisis de Performance")
    pregunta = st.chat_input("Ej: ¿Cuáles son los 5 SKUs con mayor urgencia de compra?")
    
    if pregunta:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        # Pasamos resumen de performance a la IA
        resumen = df_f.groupby('SKU').agg({'Stock_Actual': 'sum', 'Venta_Mensual': 'sum', 'Sugerido_Compra': 'sum'}).head(20).to_string()
        response = model.generate_content(f"Analiza este performance de stock mensual:\n{resumen}\nPregunta: {pregunta}")
        st.info(response.text)

else:
    st.warning("Verifica los archivos CSV en Drive...")
