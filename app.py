import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# 1. Configuración visual
st.set_page_config(page_title="Dass | Torre de Control Mensual", layout="wide", page_icon="👟")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 32px; color: #1E88E5; font-weight: bold; }
    .main { background-color: #f8f9fa; }
    div[data-testid="stMetric"] { background-color: white; padding: 15px; border-radius: 10px; box-shadow: 2px 2px 5px rgba(0,0,0,0.05); }
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
            
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            
            name = item['name'].replace('.csv', '')
            if 'Sell_out' in name:
                # Análisis Mensual: Agrupamos por mes (asumiendo que el archivo Sell_out es el último mes)
                df = df.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum().reset_index()
                df = df.rename(columns={'Unidades': 'Venta_Mensual'})
                name = 'Sell_out'
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error de carga: {e}")
        return None

data = load_data()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    df_main = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    df_main = df_main.merge(data['Maestro_Productos'], on='SKU', how='left')
    df_main = df_main.merge(data['Sell_out'][['SKU', 'Cliente', 'Ubicacion', 'Venta_Mensual']], 
                            on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # Cálculo de Meses de Cobertura (Stock / Venta Mensual)
    df_main['Meses_Cobertura'] = df_main.apply(lambda x: x['Stock_Actual'] / x['Venta_Mensual'] if x['Venta_Mensual'] > 0 else 12, axis=1)

    st.title("👟 Torre de Control Dass - Análisis Mensual")
    st.markdown("---")

    # --- FILTROS ---
    st.sidebar.header("Panel Mensual")
    clientes = sorted(df_main['Cliente'].unique())
    f_cliente = st.sidebar.multiselect("Seleccionar Clientes", clientes, default=clientes[:1])
    
    df_f = df_main[df_main['Cliente'].isin(f_cliente)] if f_cliente else df_main

    # --- MÉTRICAS MENSUALES ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock Total", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Mensual", f"{df_f['Venta_Mensual'].sum():,.0f}")
    meses_prom = df_f[df_f['Meses_Cobertura'] < 12]['Meses_Cobertura'].mean()
    c3.metric("Cobertura Media", f"{meses_prom:.1f} meses" if not pd.isna(meses_prom) else "N/A")
    c4.metric("SKUs Activos", f"{df_f['SKU'].nunique():,}")

    # --- TABLA ---
    st.subheader("📋 Detalle de Inventario y Cobertura Mensual")
    
    # Semáforo Mensual: Rojo (Crítico < 1 mes), Naranja (1-2 meses), Verde (> 2 meses)
    def style_cobertura(val):
        if val == 12: return 'color: #D3D3D3'
        color = '#E53935' if val < 1 else '#FB8C00' if val < 2 else '#43A047'
        return f'color: {color}; font-weight: bold'

    st.dataframe(
        df_f[['SKU', 'Cliente', 'Ubicacion', 'Stock_Actual', 'Venta_Mensual', 'Meses_Cobertura']]
        .sort_values('Meses_Cobertura')
        .style.map(style_cobertura, subset=['Meses_Cobertura'])
        .format({'Stock_Actual': '{:,.0f}', 'Venta_Mensual': '{:,.0f}', 'Meses_Cobertura': '{:,.1f}'}),
        use_container_width=True, height=450
    )

    # --- IA ---
    st.divider()
    st.subheader("🤖 Analista IA (Análisis Mensual)")
    chat = st.chat_input("Ej: ¿Qué clientes tienen stock para menos de un mes?")
    
    if chat:
        with st.spinner("Analizando ciclo mensual..."):
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel('gemini-1.5-flash')
            resumen_ia = df_f.groupby('Cliente').agg({'Stock_Actual': 'sum', 'Venta_Mensual': 'sum', 'Meses_Cobertura': 'mean'}).to_string()
            prompt = f"Analiza el inventario mensual de Dass. Cobertura en meses:\n{resumen_ia}\nPregunta: {chat}"
            response = model.generate_content(prompt)
            st.info(response.text)
else:
    st.warning("Cargando datos mensuales...")

else:
    st.info("Esperando que los archivos CSV se procesen correctamente...")


