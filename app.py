import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control v1.2", layout="wide")

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
        
        # Nombres exactos que queremos permitir
        archivos_permitidos = ['Maestro_Productos', 'Sell_In_Ventas', 'Sell_Out', 'Stock', 'Ingresos']
        
        dfs = {}
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            
            # FILTRO ESTRICTO: Solo procesa si el nombre coincide exactamente
            if name in archivos_permitidos:
                request = service.files().get_media(fileId=f['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                
                df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
                
                # Normalización de columnas
                df.columns = [str(c).strip().upper().replace('ï»¿', '') for c in df.columns]
                
                # Forzar columna SKU
                mapeo_sku = {'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'PRODUCTO': 'SKU', 'ITEM': 'SKU'}
                df = df.rename(columns=mapeo_sku)
                
                if 'SKU' in df.columns:
                    df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
                
                dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error crítico: {e}")
        return {}

data = load_drive_data()

if data:
    # Sidebar de estado para que veas qué leyó
    st.sidebar.success(f"Archivos cargados: {', '.join(data.keys())}")

    # Asignación de variables
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # --- Verificación de Columnas Clave ---
    if not sell_in.empty and 'SKU' in sell_in.columns:
        
        # Procesamiento de Fechas
        for df in [sell_in, sell_out, ingresos]:
            if not df.empty and 'SKU' in df.columns:
                # Buscamos FECHA o MES
                col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
                if col_f:
                    df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                    df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')

        # --- Filtros de Marca ---
        st.sidebar.header("Segmentación")
        f_emp = st.sidebar.selectbox("Canal/Emprendimiento", ["TODOS", "RETAIL", "ECOM", "WHOLESALE"])
        
        m_filt = maestro.copy()
        if not m_filt.empty:
            f_disc = st.sidebar.multiselect("Disciplina", m_filt['DISCIPLINA'].unique() if 'DISCIPLINA' in m_filt.columns else [])
            if f_disc: m_filt = m_filt[m_filt['DISCIPLINA'].isin(f_disc)]

        # --- Dashboard ---
        tab1, tab2, tab3 = st.tabs(["📊 Estrategia", "⚡ Tactical", "👟 Producto"])

        with tab1:
            st.subheader("Mix de Ventas (Sell In)")
            if not sell_in.empty and not m_filt.empty:
                mix = sell_in.merge(m_filt, on='SKU', how='inner')
                if not mix.empty:
                    c1, c2 = st.columns(2)
                    c1.plotly_chart(px.pie(mix, values='UNIDADES', names='DISCIPLINA', title="Por Disciplina"))
                    c2.plotly_chart(px.pie(mix, values='UNIDADES', names='GENERO', title="Por Género"))

        with tab2:
            st.subheader("Velocidad de Inventario (MOS)")
            # Lógica de MOS simplificada
            if not stock.empty and not sell_out.empty:
                stk_total = stock.groupby('SKU')['CANTIDAD'].sum().reset_index()
                vta_avg = sell_out.groupby('SKU')['CANTIDAD'].mean().reset_index()
                
                tabla_mos = m_filt[['SKU', 'DESCRIPCION']].merge(stk_total, on='SKU', how='left')
                tabla_mos = tabla_mos.merge(vta_avg, on='SKU', how='left').fillna(0)
                # MOS = Stock / Venta Media
                tabla_mos['MOS'] = (tabla_mos['CANTIDAD_x'] / tabla_mos['CANTIDAD_y']).replace([float('inf')], 99).fillna(0)
                st.dataframe(tabla_mos.sort_values('CANTIDAD_y', ascending=False), use_container_width=True)

        with tab3:
            st.subheader("Análisis por Producto")
            if not m_filt.empty:
                sku_sel = st.selectbox("Busca un SKU", m_filt['SKU'].unique())
                
                # Gráfico de tendencias
                vta_sku = sell_out[sell_out['SKU'] == sku_sel].groupby('MES_KEY')['CANTIDAD'].sum().reset_index()
                if not vta_sku.empty:
                    st.plotly_chart(px.line(vta_sku, x='MES_KEY', y='CANTIDAD', title=f"Evolución Sell Out: {sku_sel}"))
                else:
                    st.info("No hay datos de Sell Out para este SKU.")
    else:
        st.error("Error: 'Sell_In_Ventas.csv' no tiene la columna 'SKU' o el archivo no se llama exactamente así.")
else:
    st.info("Sube los archivos 'Maestro_Productos.csv', 'Sell_In_Ventas.csv', etc. a tu carpeta de Drive.")
