import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Dass Performance v5.1", layout="wide")

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
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error: {e}")
        return None

data = load_data()

if data:
    # --- 1. PROCESAMIENTO Y UNIFICACIÓN ---
    # Maestro como base
    df_base = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    def prep_num(df, col_name, val_name):
        if df is not None and col_name in df.columns:
            temp = df.copy()
            temp[val_name] = pd.to_numeric(temp[col_name], errors='coerce').fillna(0)
            # AGRUPAMOS POR SKU PARA UNIFICAR FILAS REPETIDAS
            return temp.groupby('SKU')[val_name].sum().reset_index()
        return pd.DataFrame(columns=['SKU', val_name])

    # Unificamos Sell In, Sell Out y Stocks
    si = prep_num(data.get('Sell_in'), 'Unidades', 'Sell In')
    so = prep_num(data.get('Sell_out'), 'Unidades', 'Sell Out')
    
    # Procesar Stock separando Ubicaciones
    stk_df = data.get('Stock')
    if stk_df is not None:
        stk_df['Cant'] = pd.to_numeric(stk_df['Cantidad'], errors='coerce').fillna(0)
        stk_df['Ubicacion'] = stk_df['Ubicacion'].astype(str).str.upper()
        
        mask_dass = stk_df['Ubicacion'].str.contains('DASS|DEPOSITO|CENTRAL', na=False)
        stk_dass = stk_df[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        stk_cli = stk_df[~mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    else:
        stk_dass = stk_cli = pd.DataFrame(columns=['SKU', 'Stock Dass'])

    # MERGE FINAL
    df_final = df_base.merge(si, on='SKU', how='left').merge(so, on='SKU', how='left')
    df_final = df_final.merge(stk_dass, on='SKU', how='left').merge(stk_cli, on='SKU', how='left').fillna(0)

    # --- 2. CÁLCULO DE COLUMNAS DE INTELIGENCIA ---
    # Sell Through = (Sell Out / Sell In) * 100
    df_final['Sell Through %'] = (df_final['Sell Out'] / df_final['Sell In'] * 100).replace([float('inf'), -float('inf')], 0).fillna(0)
    
    # Rotación (WOS) = Stock Clientes / (Sell Out / 4) -> Semanas de inventario
    df_final['Rotacion (Meses)'] = (df_final['Stock Clientes'] / df_final['Sell Out']).replace([float('inf'), -float('inf')], 0).fillna(0)

    # --- 3. INTERFAZ Y FILTROS SUPERIORES ---
    st.title("👟 Performance Dashboard Dass v5.1")
    
    # Filtros arriba
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        # Si tienes columna Cliente en alguno de los archivos, podrías traerla al maestro o filtrar aquí
        filtro_sku = st.multiselect("Filtrar por Disciplina", sorted(df_final['Disciplina'].unique()) if 'Disciplina' in df_final.columns else [])
    
    if filtro_sku:
        df_final = df_final[df_final['Disciplina'].isin(filtro_sku)]

    # TABLA CONSOLIDADA
    st.subheader("Análisis Consolidado por SKU")
    
    columnas_orden = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion (Meses)']
    df_display = df_final[[c for c in columnas_orden if c in df_final.columns]]

    st.dataframe(
        df_display.style.format({
            'Sell In': '{:,.0f}', 'Sell Out': '{:,.0f}',
            'Stock Clientes': '{:,.0f}', 'Stock Dass': '{:,.0f}',
            'Sell Through %': '{:.1f}%', 'Rotacion (Meses)': '{:.2f} m'
        }),
        use_container_width=True, height=500
    )

    # Resumen inferior
    st.divider()
    m1, m2, m3 = st.columns(3)
    total_si = df_display['Sell In'].sum()
    total_so = df_display['Sell Out'].sum()
    m1.metric("Sell In Total", f"{total_si:,.0f}")
    m2.metric("Sell Out Total", f"{total_so:,.0f}")
    m3.metric("Sell Through Global", f"{(total_so/total_si*100 if total_si>0 else 0):.1f}%")

else:
    st.warning("Cargando datos...")
