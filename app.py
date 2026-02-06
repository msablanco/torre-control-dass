import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Ranking Performance Dass", layout="wide")

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
        st.error(f"Error de conexión: {e}")
        return None

data = load_data()

if data:
    # --- 1. PROCESAMIENTO CON LÓGICA DE FOTO VS FLUJO ---
    df_base = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # FLUJO: Sell In y Sell Out se SUMAN
    def get_flujo(df_name, col_val, new_name):
        if df_name in data:
            temp = data[df_name].copy()
            temp[new_name] = pd.to_numeric(temp[col_val], errors='coerce').fillna(0)
            return temp.groupby('SKU')[new_name].sum().reset_index()
        return pd.DataFrame(columns=['SKU', new_name])

    # FOTO: El Stock NO se suma, se toma la última posición (last)
    def get_foto(df_name, col_val, new_name, filter_dass=None):
        if df_name in data:
            temp = data[df_name].copy()
            temp[new_name] = pd.to_numeric(temp[col_val], errors='coerce').fillna(0)
            temp['Ubicacion'] = temp['Ubicacion'].fillna('').astype(str).str.upper()
            
            if filter_dass is True:
                temp = temp[temp['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)]
            elif filter_dass is False:
                temp = temp[~temp['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)]
            
            # Tomamos la última foto por SKU (no sumamos ubicaciones/meses)
            return temp.groupby('SKU')[new_name].last().reset_index()
        return pd.DataFrame(columns=['SKU', new_name])

    si = get_flujo('Sell_in', 'Unidades', 'Sell in')
    so_cli = get_flujo('Sell_out', 'Unidades', 'Sell out Clientes')
    so_tnd = get_flujo('Sell_out_Tiendas', 'Unidades', 'Sell out tiendas')
    
    st_dass = get_foto('Stock', 'Cantidad', 'Stock Dass', filter_dass=True)
    st_cli = get_foto('Stock', 'Cantidad', 'Stock Clientes', filter_dass=False)

    # MERGE FINAL
    df = df_base.merge(si, on='SKU', how='left').merge(so_cli, on='SKU', how='left')
    df = df.merge(so_tnd, on='SKU', how='left').merge(st_dass, on='SKU', how='left').merge(st_cli, on='SKU', how='left')
    df = df.fillna(0)

    # --- 2. CÁLCULOS DE RATIOS ---
    df['Relacion Stock/Sell in'] = np.where(df['Sell in'] > 0, (df['Stock Dass'] + df['Stock Clientes']) / df['Sell in'], 0)
    df['Relacion stock clientes/Sell out'] = np.where(df['Sell out Clientes'] > 0, df['Stock Clientes'] / df['Sell out Clientes'], 0)

    # --- 3. FILTROS ---
    st.sidebar.header("🔍 Filtros Dinámicos")
    for col in ['Disciplina', 'Genero']:
        if col in df.columns:
            opts = sorted([str(x) for x in df[col].unique() if x != 0])
            sel = st.sidebar.multiselect(f"Filtrar {col}", opts)
            if sel: df = df[df[col].isin(sel)]

    # --- 4. VISUALIZACIÓN ---
    st.title("📊 Ranking de Performance Dass v5.8")
    
    # Gráficos de Torta
    c1, c2, c3 = st.columns(3)
    with c1: st.plotly_chart(px.pie(df[df['Stock Dass']>0], values='Stock Dass', names='Disciplina', title="Stock Dass (Foto)"), use_container_width=True)
    with c2: st.plotly_chart(px.pie(df[df['Sell out Clientes']>0], values='Sell out Clientes', names='Disciplina', title="Sell Out (Flujo)"), use_container_width=True)
    with c3: st.plotly_chart(px.pie(df[df['Sell in']>0], values='Sell in', names='Disciplina', title="Ingresos (Flujo)", hole=0.4), use_container_width=True)

    # --- 5. TABLA DE RANKING CON SEMÁFOROS ---
    st.subheader("🏆 Ranking de Productos y Alertas de Rotación")

    def color_rotacion(val):
        color = 'background-color: #ffcccc; color: #990000' if val > 3 else ''
        return color

    cols_order = ['SKU', 'Descripcion', 'Sell in', 'Sell out Clientes', 'Sell out tiendas', 'Stock Dass', 'Stock Clientes', 'Relacion Stock/Sell in', 'Relacion stock clientes/Sell out']
    df_ranking = df[[c for c in cols_order if c in df.columns]].sort_values('Sell out Clientes', ascending=False)

    st.dataframe(
        df_ranking.style.format({
            'Sell in': '{:,.0f}', 'Sell out Clientes': '{:,.0f}', 'Sell out tiendas': '{:,.0f}',
            'Stock Dass': '{:,.0f}', 'Stock Clientes': '{:,.0f}',
            'Relacion Stock/Sell in': '{:.2f}', 'Relacion stock clientes/Sell out': '{:.2f}'
        }).map(color_rotacion, subset=['Relacion stock clientes/Sell out']),
        use_container_width=True, height=600
    )

else:
    st.info("Aguardando archivos de Drive...")
