import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# 1. CONFIGURACIÓN
st.set_page_config(page_title="Dass Performance Engine", layout="wide")

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
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            name = item['name'].replace('.csv', '')
            if 'Sell_out' in name:
                df = df.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df = df.rename(columns={'Unidades': 'Venta_Mensual'})
                name = 'Sell_out'
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error: {e}")
        return None

data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # UNIFICACIÓN (Siguiendo lógica Performance v5.0)
    df = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    df = df.merge(data['Sell_out'], on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # CÁLCULOS CLAVE
    df['Cobertura'] = df.apply(lambda x: x['Stock_Actual'] / x['Venta_Mensual'] if x['Venta_Mensual'] > 0 else 12, axis=1)
    MESES_TARGET = 3 
    df['Sugerido_Compra'] = df.apply(lambda x: max(0, (x['Venta_Mensual'] * MESES_TARGET) - x['Stock_Actual']), axis=1)

    # --- INTERFAZ ---
    st.title("📊 Dass Performance & Sugerido de Compra")
    
    # Filtros
    clientes = sorted(df['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Filtrar Cliente", clientes, default=clientes[:1])
    df_f = df[df['Cliente'].isin(f_cliente)]

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock Total", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Sell Out Mensual", f"{df_f['Venta_Mensual'].sum():,.0f}")
    c3.metric("Sugerido Reposición", f"{df_f['Sugerido_Compra'].sum():,.0f}")
    cob_m = df_f[df_f['Cobertura'] < 12]['Cobertura'].mean()
    c4.metric("Cobertura Media", f"{cob_m:.1f} meses")

    # TABLA
    st.subheader("📋 Análisis de Performance")
    
    def color_cob(val):
        if val == 12: return 'color: gray'
        return f"color: {'#E53935' if val < 1 else '#FB8C00' if val < 2 else '#43A047'}; font-weight: bold"

    st.dataframe(
        df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'Venta_Mensual', 'Cobertura', 'Sugerido_Compra']]
        .sort_values(by='Sugerido_Compra', ascending=False)
        .style.map(color_cob, subset=['Cobertura'])
        .format({'Stock_Actual': '{:,.0f}', 'Venta_Mensual': '{:,.0f}', 'Cobertura': '{:,.1f} m', 'Sugerido_Compra': '{:,.0f}'}),
        use_container_width=True
    )

    # --- IA OPTIMIZADA (Sin Error de InvalidArgument) ---
    st.divider()
    st.subheader("🤖 Consultar Análisis de Abastecimiento")
    pregunta = st.chat_input("¿Cuáles son las prioridades de compra?")
    
    if pregunta:
        if "GEMINI_API_KEY" in st.secrets:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-1.5-flash')
            
            # FILTRADO PARA IA: Solo enviamos los 15 SKUs con más Sugerido de Compra
            # Esto evita el error de InvalidArgument por exceso de datos
            resumen_ia = (df_f.sort_values('Sugerido_Compra', ascending=False)
                          .head(15)
                          .drop(columns=['Cliente', 'Ubicacion'])
                          .to_string(index=False))
            
            prompt = (f"Eres un experto en Supply Chain de Dass. Analiza los 15 SKUs con mayor necesidad de compra:\n"
                      f"{resumen_ia}\n\nPregunta: {pregunta}")
            
            try:
                response = model.generate_content(prompt)
                st.info(response.text)
            except Exception as e:
                st.error("La consulta es demasiado compleja para el volumen de datos seleccionado.")
        else:
            st.warning("Configura GEMINI_API_KEY en Secrets.")

else:
    st.info("Cargando datos...")
