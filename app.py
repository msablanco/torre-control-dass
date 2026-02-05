import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Dass Performance v5.0", layout="wide", page_icon="👟")

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
            
            # Leemos todo como string inicialmente para evitar el error .str accessor
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            
            # Limpiamos nombres de columnas (quitar tildes y espacios)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error en Drive: {e}")
        return None

data = load_data()

if data:
    # 1. BASE: Maestro de Productos (Columnas: SKU, Descripcion, Disciplina, Color, Genero)
    df_final = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # 2. PROCESAR SELL OUT (Venta Mensual)
    if 'Sell_out' in data:
        so = data['Sell_out'].copy()
        so['Unidades'] = pd.to_numeric(so['Unidades'], errors='coerce').fillna(0)
        so_grouped = so.groupby('SKU')['Unidades'].sum().reset_index()
        so_grouped.columns = ['SKU', 'Sell Out']
        df_final = df_final.merge(so_grouped, on='SKU', how='left')

    # 3. PROCESAR SELL IN (Compras)
    if 'Sell_in' in data:
        si = data['Sell_in'].copy()
        si['Unidades'] = pd.to_numeric(si['Unidades'], errors='coerce').fillna(0)
        si_grouped = si.groupby('SKU')['Unidades'].sum().reset_index()
        si_grouped.columns = ['SKU', 'Sell In']
        df_final = df_final.merge(si_grouped, on='SKU', how='left')

    # 4. PROCESAR STOCKS (Dass vs Clientes)
    if 'Stock' in data:
        stock = data['Stock'].copy()
        stock['Cantidad'] = pd.to_numeric(stock['Cantidad'], errors='coerce').fillna(0)
        # Forzamos Ubicacion a texto para evitar el AttributeError
        stock['Ubicacion'] = stock['Ubicacion'].astype(str).str.upper()
        
        # Separamos Stock Dass de Stock Clientes
        # Ajusta 'DASS' por el nombre real de tu depósito en el CSV si es distinto
        mask_dass = stock['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)
        
        s_dass = stock[mask_dass].groupby('SKU')['Cantidad'].sum().reset_index()
        s_dass.columns = ['SKU', 'Stock Dass']
        
        s_cli = stock[~mask_dass].groupby('SKU')['Cantidad'].sum().reset_index()
        s_cli.columns = ['SKU', 'Stock Clientes']
        
        df_final = df_final.merge(s_dass, on='SKU', how='left').merge(s_cli, on='SKU', how='left')

    # 5. ORDENAR Y LIMPIAR
    df_final = df_final.fillna(0)
    
    # Estructura de tabla solicitada
    cols_pedidas = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass']
    # Solo mostramos las que existan para evitar errores
    df_display = df_final[[c for c in cols_pedidas if c in df_final.columns]]

    # --- INTERFAZ ---
    st.title("👟 Desaborad Performance - Calzado v5.0")
    st.markdown("---")

    # Filtros Pro en Sidebar
    st.sidebar.header("Filtros de Segmentación")
    for col in ['Disciplina', 'Genero', 'Color']:
        if col in df_display.columns:
            opciones = sorted(df_display[col].unique().astype(str))
            sel = st.sidebar.multiselect(f"Filtrar {col}", opciones)
            if sel:
                df_display = df_display[df_display[col].isin(sel)]

    # Tabla Principal
    st.subheader("Análisis de Pipeline: Sell In / Sell Out / Stocks")
    
    # Formato numérico para la tabla
    st.dataframe(
        df_display.style.format({
            'Sell In': '{:,.0f}',
            'Sell Out': '{:,.0f}',
            'Stock Clientes': '{:,.0f}',
            'Stock Dass': '{:,.0f}'
        }),
        use_container_width=True,
        height=500
    )

    # Resumen Ejecutivo
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Sell Out", f"{df_display['Sell Out'].sum():,.0f}")
    with col2:
        st.metric("Total Sell In", f"{df_display['Sell In'].sum():,.0f}")
    with col3:
        st.metric("Stock en Clientes", f"{df_display['Stock Clientes'].sum():,.0f}")
    with col4:
        st.metric("Stock en Dass", f"{df_display['Stock Dass'].sum():,.0f}")

else:
    st.info("Conectando con Drive y procesando archivos... Asegúrate de que los nombres de los CSV sean correctos.")

