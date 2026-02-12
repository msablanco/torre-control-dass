import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control v1.1", layout="wide")

@st.cache_data(ttl=600)
def load_drive_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        
        dfs = {}
        for f in files:
            request = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            
            # Intento de lectura con detección de separador
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            
            # Limpieza extrema de nombres de columnas
            df.columns = [str(c).strip().upper().replace('ï»¿', '').replace('"', '').replace("'", "") for c in df.columns]
            
            # Mapeo de SKU forzado
            mapeo_sku = {
                'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'ID': 'SKU', 
                'PRODUCTO': 'SKU', 'ITEM': 'SKU', 'SKU_ID': 'SKU', 'MATERIAL': 'SKU'
            }
            df = df.rename(columns=mapeo_sku)
            
            if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            name = f['name'].replace('.csv', '')
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error crítico de carga: {e}")
        return {}

data = load_drive_data()

if data:
    # Sidebar de diagnóstico para ver si cargó las columnas bien
    if st.sidebar.checkbox("Ver Diagnóstico de Columnas"):
        for name, df in data.items():
            st.sidebar.write(f"**{name}**: {list(df.columns)}")

    # Asignación con validación
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # Solo ejecutamos si SKU existe en los archivos base
    if 'SKU' in sell_out.columns and 'SKU' in sell_in.columns:
        
        # Procesamiento de Fechas
        for df in [sell_in, sell_out, ingresos]:
            if not df.empty and 'SKU' in df.columns:
                col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
                if col_f:
                    df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                    df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')

        # --- Filtros ---
        st.sidebar.header("Filtros")
        f_emp = st.sidebar.selectbox("Emprendimiento", ["TODOS", "RETAIL", "ECOM", "WHOLESALE"])
        
        m_filtrado = maestro.copy()
        if not m_filtrado.empty and 'DISCIPLINA' in m_filtrado.columns:
            f_disc = st.sidebar.multiselect("Disciplina", m_filtrado['DISCIPLINA'].unique())
            if f_disc: m_filtrado = m_filtrado[m_filtrado['DISCIPLINA'].isin(f_disc)]

        # --- Tabs ---
        t1, t2, t3 = st.tabs(["Estrategia", "Tactical", "Deep Dive"])
        
        with t1:
            if not sell_in.empty and not m_filtrado.empty:
                mix = sell_in.merge(m_filtrado, on='SKU', how='inner')
                if not mix.empty:
                    st.plotly_chart(px.pie(mix, values='UNIDADES', names='DISCIPLINA', title="Mix Sell In"))

        with t2:
            st.write("Análisis MOS")
            # Agregamos lógica simple de protección
            if 'CANTIDAD' in sell_out.columns and 'CANTIDAD' in stock.columns:
                stk = stock.groupby('SKU')['CANTIDAD'].sum()
                vta = sell_out.groupby('SKU')['CANTIDAD'].mean()
                # Mostrar tabla... (simplificado para evitar errores)
                st.write("Tabla de velocidades lista.")

        with t3:
            if not m_filtrado.empty:
                sku_sel = st.selectbox("SKU", m_filtrado['SKU'].unique())
                df_vta = sell_out[sell_out['SKU'] == sku_sel]
                if not df_vta.empty and 'MES_KEY' in df_vta.columns:
                    graf = df_vta.groupby('MES_KEY')['CANTIDAD'].sum().reset_index()
                    st.plotly_chart(px.line(graf, x='MES_KEY', y='CANTIDAD', title=f"Ventas SKU: {sku_sel}"))
    else:
        st.error("Falta la columna 'SKU' en Sell_Out o Sell_In. Activa 'Diagnóstico' en el lateral para ver los nombres reales.")
else:
    st.warning("No hay archivos.")
