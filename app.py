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
            
            # Cargamos como string para evitar errores de tipo de dato (AttributeError)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            # Limpieza de encabezados
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return None

data = load_data()

if data:
    # 1. BASE: Maestro de Productos
    df_final = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # Función para convertir a número de forma segura
    def to_num(df, col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors='coerce').fillna(0)
        return 0

    # 2. UNIFICACIÓN DE SELL IN Y SELL OUT
    if 'Sell_out' in data:
        so = data['Sell_out'].copy()
        so['Cant'] = to_num(so, 'Unidades')
        so_grouped = so.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out'})
        df_final = df_final.merge(so_grouped, on='SKU', how='left')

    if 'Sell_in' in data:
        si = data['Sell_in'].copy()
        si['Cant'] = to_num(si, 'Unidades')
        si_grouped = si.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In'})
        df_final = df_final.merge(si_grouped, on='SKU', how='left')

    # 3. LÓGICA DE STOCKS (Dass vs Clientes)
    if 'Stock' in data:
        stk = data['Stock'].copy()
        stk['Cant'] = to_num(stk, 'Cantidad')
        stk['Ubicacion'] = stk['Ubicacion'].fillna('SIN DATOS').astype(str).str.upper()
        
        # Filtramos por palabra clave para identificar stock propio de Dass
        mask_dass = stk['Ubicacion'].str.contains('DASS|DEPOSITO|CENTRAL', na=False)
        
        stk_dass = stk[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        stk_cli = stk[~mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
        
        df_final = df_final.merge(stk_dass, on='SKU', how='left').merge(stk_cli, on='SKU', how='left')

    # 4. LIMPIEZA Y FORMATO
    df_final = df_final.fillna(0)
    
    # Estructura exacta solicitada
    cols_layout = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass']
    # Filtrar solo columnas que realmente existan para evitar errores
    df_display = df_final[[c for c in cols_layout if c in df_final.columns]]

    # --- INTERFAZ STREAMLIT ---
    st.title("👟 Performance Dashboard - Calzado v5.0")
    
    # Filtros en Sidebar
    st.sidebar.header("Filtros Globales")
    for col_filtro in ['Disciplina', 'Genero', 'Color']:
        if col_filtro in df_display.columns:
            lista = sorted(df_display[col_filtro].unique())
            seleccion = st.sidebar.multiselect(f"Filtrar {col_filtro}", lista)
            if seleccion:
                df_display = df_display[df_display[col_filtro].isin(seleccion)]

    # Tabla Principal
    st.subheader("Análisis Consolidado de Inventario")
    st.dataframe(
        df_display.style.format({
            'Sell In': '{:,.0f}', 'Sell Out': '{:,.0f}',
            'Stock Clientes': '{:,.0f}', 'Stock Dass': '{:,.0f}'
        }),
        use_container_width=True,
        height=600
    )

    # Resumen de Totales
    st.markdown("---")
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Sell In Total", f"{df_display['Sell In'].sum():,.0f}")
    t2.metric("Sell Out Total", f"{df_display['Sell Out'].sum():,.0f}")
    t3.metric("Stock en Mercado", f"{df_display['Stock Clientes'].sum():,.0f}")
    t4.metric("Stock en Dass", f"{df_display['Stock Dass'].sum():,.0f}")

else:
    st.warning("Aguardando carga de datos desde Google Drive...")
