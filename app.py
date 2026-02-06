import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v6.7", layout="wide")

@st.cache_data(ttl=600)
def load_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        results = service.files().list(q=f"'{folder_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
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
        st.error(f"Error Drive: {e}")
        return None

data = load_data()

if data:
    # --- 1. PROCESAMIENTO MAESTRO (SEGMENTACIÓN COMERCIAL) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # Buscamos la columna "Franja" (Pinnacle, Best, etc.)
    # Si no existe con ese nombre exacto, buscamos "Segmento" o creamos una vacía para evitar errores
    if 'Franja' not in df_ma.columns:
        if 'Segmento' in df_ma.columns:
            df_ma = df_ma.rename(columns={'Segmento': 'Franja'})
        else:
            df_ma['Franja'] = 'Sin Segmento'
    
    df_ma['Franja'] = df_ma['Franja'].fillna('Sin Segmento').astype(str).str.upper()

    # --- 2. STOCK (FOTO + CLIENTE + FECHA) ---
    stk_raw = data.get('Stock', pd.DataFrame())
    st_dass_grp = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    st_cli_grp = pd.DataFrame(columns=['SKU', 'Stock Clientes', 'Cliente'])
    fecha_foto_cli = "N/A"

    if not stk_raw.empty:
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw.get('Fecha'), dayfirst=True, errors='coerce').fillna(pd.Timestamp.now())
        stk_raw['Ubicacion'] = stk_raw['Ubicacion'].fillna('').astype(str).str.upper()
        stk_raw['Cliente'] = stk_raw['Cliente'].fillna('SIN CLIENTE').astype(str).str.strip()
        
        stk_s = stk_raw.sort_values(by='Fecha_dt')
        mask_d = stk_s['Ubicacion'].str.contains('DASS|CENTRAL|DEP|PROPIO|LOG|MAYORISTA', na=False)
        
        st_dass_grp = stk_s[mask_d].groupby('SKU')['Cant'].last().reset_index().rename(columns={'Cant': 'Stock Dass'})
        st_cli_grp = stk_s[~mask_d].groupby(['SKU', 'Cliente']).agg({'Cant': 'last', 'Fecha_dt': 'max'}).reset_index().rename(columns={'Cant': 'Stock Clientes'})
        if not stk_s[~mask_d].empty:
            fecha_foto_cli = stk_s[~mask_d]['Fecha_dt'].max().strftime('%d/%m/%Y')

    # --- 3. VENTAS (IN / OUT) ---
    si_raw = data.get('Sell_in', pd.DataFrame())
    si_grp = pd.DataFrame(columns=['SKU', 'Sell in', 'Cliente'])
    if not si_raw.empty:
        si_raw['Sell in'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        si_grp = si_raw.groupby(['SKU', 'Cliente'])['Sell in'].sum().reset_index()

    so_raw = data.get('Sell_out', pd.DataFrame())
    so_final = pd.DataFrame(columns=['SKU', 'Sell out Clientes', 'Sell out tiendas', 'Cliente'])
    if not so_raw.empty:
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Tipo'] = so_raw['Tipo'].fillna('').astype(str).str.upper()
        so_c = so_raw[so_raw['Tipo'].str.contains('CLIENTE')].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Clientes'})
        so_t = so_raw[so_raw['Tipo'].str.contains('TIENDA')].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out tiendas'})
        so_final = pd.concat([so_c, so_t]).groupby(['SKU', 'Cliente']).sum().reset_index()

    # --- 4. FILTROS ---
    st.sidebar.header("🔍 Filtros de Gestión")
    clis_all = sorted(list(set(si_grp['Cliente'].dropna().unique().tolist() + st_cli_grp['Cliente'].dropna().unique().tolist())))
    f_cli = st.sidebar.multiselect("Cliente", [c for c in clis_all if str(c) not in ['0', 'nan', 'SIN CLIENTE']])
    f_dis = st.sidebar.multiselect("Disciplina", sorted(df_ma['Disciplina'].unique().tolist()) if 'Disciplina' in df_ma.columns else [])
    f_fra = st.sidebar.multiselect("Franja (Segmento)", sorted(df_ma['Franja'].unique().tolist()))

    # Filtrado lógico
    if f_cli:
        si_grp = si_grp[si_grp['Cliente'].isin(f_cli)]
        so_final = so_final[so_final['Cliente'].isin(f_cli)]
        st_cli_grp = st_cli_grp[st_cli_grp['Cliente'].isin(f_cli)]
    
    df = df_ma.merge(st_dass_grp, on='SKU', how='left')
    df = df.merge(si_grp.groupby('SKU')['Sell in'].sum().reset_index(), on='SKU', how='left')
    df = df.merge(so_final.groupby('SKU')[['Sell out Clientes', 'Sell out tiendas']].sum().reset_index(), on='SKU', how='left')
    df = df.merge(st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index(), on='SKU', how='left')
    df = df.fillna(0)

    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['Franja'].isin(f_fra)]

    # --- 5. DASHBOARD ---
    st.title("📊 Análisis de Performance por Franja Comercial")
    st.info(f"📅 Stock Clientes basado en la foto del: **{fecha_foto_cli}**")

    def safe_pie(dataframe, val_col, name_col, title_str, col_target):
        clean_df = dataframe[dataframe[val_col] > 0]
        if not clean_df.empty:
            fig = px.pie(clean_df, values=val_col, names=name_col, title=title_str)
            col_target.plotly_chart(fig, use_container_width=True)
        else:
            col_target.warning(f"Sin datos: {title_str}")

    # FILA 1: Por Disciplina
    st.subheader("📌 Participación por Disciplina")
    g1, g2, g3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Disciplina', "Stock Dass", g1)
    safe_pie(df, 'Sell in', 'Disciplina', "Ingresos (Sell In)", g2)
    safe_pie(df, 'Sell out Clientes', 'Disciplina', "Sell Out Clientes", g3)

    # FILA 2: Por Franja Comercial (Pinnacle, Best, etc.)
    st.subheader("🏆 Participación por Franja Comercial")
    p1, p2, p3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Franja', "Stock Dass por Franja", p1)
    safe_pie(df, 'Sell in', 'Franja', "Ingresos por Franja", p2)
    safe_pie(df, 'Sell out Clientes', 'Franja', "Sell Out por Franja", p3)

    # --- 6. RANKING ---
    st.divider()
    df['WOS'] = np.where(df['Sell out Clientes']>0, df['Stock Clientes']/df['Sell out Clientes'], 0)
    cols_rank = ['SKU', 'Descripcion', 'Franja', 'Sell in', 'Sell out Clientes', 'Stock Dass', 'Stock Clientes', 'WOS']
    st.dataframe(
        df[cols_rank].sort_values('Sell out Clientes', ascending=False).style.format({
            'Sell in':'{:,.0f}', 'Sell out Clientes':'{:,.0f}', 'Stock Dass':'{:,.0f}', 
            'Stock Clientes':'{:,.0f}', 'WOS':'{:.2f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else '', subset=['WOS']),
        use_container_width=True
    )
else:
    st.info("Conectando con Drive...")
