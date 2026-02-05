import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Torre de Control Dass", layout="wide", page_icon="👟")

# --- CONEXIÓN A GOOGLE DRIVE ---
@st.cache_data(ttl=600)  # Cache de 10 min para permitir actualizaciones rápidas
def load_data_from_drive():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'",
            fields="files(id, name)"
        ).execute()
        items = results.get('files', [])

        dfs = {}
        for item in items:
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            name = item['name'].replace('.csv', '')
            
            # MEJORA CLAVE: Lectura robusta para archivos de Excel (Latin-1 y separador automático)
            try:
                dfs[name] = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', on_bad_lines='skip')
            except Exception as e:
                st.error(f"Error procesando {item['name']}: {e}")
        return dfs
    except Exception as e:
        st.error(f"Error conectando a Drive: {e}")
        return None

# --- PROCESAMIENTO DE DATOS ---
data = load_data_from_drive()

if data and all(k in data for k in ['Stock', 'Maestro_Productos', 'Sell_out']):
    # Limpieza de columnas (quitar espacios en blanco si los hay)
    for k in data: data[k].columns = data[k].columns.str.strip()

    # 1. Sell Out y VPS
    df_out = data['Sell_out']
    vps = df_out.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum() / 4
    vps = vps.reset_index().rename(columns={'Unidades': 'VPS'})

    # 2. Unificación
    df = data['Stock'].merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(vps, on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    if 'Ingresos' in data:
        data['Ingresos'].columns = data['Ingresos'].columns.str.strip()
        ingresos_sum = data['Ingresos'].groupby('SKU')['Cantidad'].sum().reset_index()
        df = df.merge(ingresos_sum, on='SKU', how='left').rename(columns={'Cantidad_x': 'Stock_Actual', 'Cantidad_y': 'Ingresos_Futuros'}).fillna(0)

    # 3. Canales
    df['Canal'] = df['Ubicacion'].apply(lambda x: 'Mayorista' if 'MAYORISTA' in str(x).upper() else ('E-com' if any(w in str(x).upper() for w in ['ECOM', 'WEB']) else 'Retail'))
    
    # 4. WOS
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    # --- SIDEBAR ---
    st.sidebar.title("Filtros de Control")
    if st.sidebar.button("🔄 Sincronizar Drive"):
        st.cache_data.clear()
        st.rerun()

    f_canal = st.sidebar.multiselect("Canal", df['Canal'].unique(), default=df['Canal'].unique())
    f_cliente = st.sidebar.multiselect("Cliente", df['Cliente'].unique(), default=df['Cliente'].unique())
    f_disciplina = st.sidebar.multiselect("Disciplina", df['Disciplina'].unique() if 'Disciplina' in df.columns else [], default=df['Disciplina'].unique() if 'Disciplina' in df.columns else [])
    f_sku = st.sidebar.text_input("Buscador SKU/Desc")

    # Filtrado
    df_f = df[(df['Canal'].isin(f_canal)) & (df['Cliente'].isin(f_cliente))]
    if f_disciplina: df_f = df_f[df_f['Disciplina'].isin(f_disciplina)]
    if f_sku: df_f = df_f[df_f.apply(lambda r: f_sku.lower() in str(r).lower(), axis=1)]

    # --- DASHBOARD ---
    st.title("👟 Torre de Control Dass")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock Físico", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Semanal", f"{df_f['VPS'].sum():,.0f}")
    c3.metric("WOS Promedio", f"{df_f['WOS'].replace(99, 0).mean():.1f}")
    c4.metric("Ingresos", f"{df_f['Ingresos_Futuros'].sum() if 'Ingresos_Futuros' in df_f.columns else 0:,.0f}")

    st.dataframe(df_f, use_container_width=True, hide_index=True)

    # --- IA ---
    st.divider()
    user_input = st.chat_input("Consulta a la IA sobre tu stock...")
    if user_input:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        contexto = df_f[['SKU', 'Cliente', 'Stock_Actual', 'WOS']].head(30).to_string()
        response = model.generate_content(f"Datos: {contexto}\nPregunta: {user_input}")
        st.info(response.text)
else:
    st.info("Esperando archivos CSV correctos en Drive (Stock, Maestro_Productos, Sell_out)...")

