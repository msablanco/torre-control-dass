import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Dass Performance Calzado v5.0", layout="wide")

# Estilos CSS para simular el tablero profesional
st.markdown("""
    <style>
    .reportview-container { background: #f0f2f6; }
    .metric-card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    </style>
    """, unsafe_allow_html=True)

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
            # Normalizar nombres de columnas
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            
            name = item['name'].replace('.csv', '')
            if 'Sell_out' in name:
                # Agrupación Mensual (Venta Total 30 días)
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
    # LÓGICA DE PERFORMANCE (Basada en tu archivo HTML)
    df = data['Stock'].rename(columns={'Cantidad': 'Stock_Actual'})
    df = df.merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(data['Sell_out'], on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # 1. Sugerencia de Compra (Target: 3 meses de cobertura)
    MESES_TARGET = 3
    df['Sugerido_Compra'] = df.apply(lambda x: max(0, (x['Venta_Mensual'] * MESES_TARGET) - x['Stock_Actual']), axis=1)
    
    # 2. Cobertura (Meses)
    df['Cobertura'] = df.apply(lambda x: x['Stock_Actual'] / x['Venta_Mensual'] if x['Venta_Mensual'] > 0 else 12, axis=1)

    # --- DASHBOARD ---
    st.title("👟 Dass Performance - Calzado v5.0")
    
    # Filtros Pro
    st.sidebar.header("Segmentación")
    f_cliente = st.sidebar.multiselect("Clientes", sorted(df['Cliente'].unique()), default=df['Cliente'].unique()[:1])
    f_cat = st.sidebar.multiselect("Categoría", sorted(df['Disciplina'].unique()) if 'Disciplina' in df.columns else [])
    
    df_f = df[df['Cliente'].isin(f_cliente)]
    if f_cat: df_f = df_f[df_f['Disciplina'].isin(f_cat)]

    # MÉTRICAS DE ALTO IMPACTO
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock Físico", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Sell Out Mensual", f"{df_f['Venta_Mensual'].sum():,.0f}")
    c3.metric("Sugerido Compra", f"{df_f['Sugerido_Compra'].sum():,.0f}", delta_color="inverse")
    c4.metric("SKUs en Ruptura", len(df_f[(df_f['Stock_Actual'] == 0) & (df_f['Venta_Mensual'] > 0)]))

    # TABLA DE PERFORMANCE
    st.subheader("📊 Análisis de Cobertura y Sugerencia de Reposición")
    
    # Estilo de semáforo
    def color_status(val):
        color = '#ff4b4b' if val < 1 else '#ffa500' if val < 2 else '#28a745'
        return f'background-color: {color}; color: white; font-weight: bold'

    st.dataframe(
        df_f[['SKU', 'Articulo', 'Cliente', 'Stock_Actual', 'Venta_Mensual', 'Cobertura', 'Sugerido_Compra']]
        .sort_values(by='Sugerido_Compra', ascending=False)
        .style.map(color_status, subset=['Cobertura'])
        .format({'Stock_Actual': '{:,.0f}', 'Venta_Mensual': '{:,.0f}', 'Cobertura': '{:,.1f} mes', 'Sugerido_Compra': '{:,.0f}'}),
        use_container_width=True
    )

    # IA ANALISTA (Integrando el contexto de performance)
    st.divider()
    st.subheader("🤖 Consultar Estrategia a la IA")
    user_q = st.chat_input("Ej: ¿Qué modelos de la categoría 'Running' necesitan reposición urgente?")
    
    if user_q:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        # Le pasamos un resumen ejecutivo a la IA
        resumen = df_f.groupby('Articulo').agg({'Stock_Actual': 'sum', 'Venta_Mensual': 'sum', 'Sugerido_Compra': 'sum'}).head(10).to_string()
        response = model.generate_content(f"Datos de Performance Calzado Dass:\n{resumen}\nPregunta: {user_q}")
        st.info(response.text)

else:
    st.info("Esperando carga de archivos desde Google Drive...")
