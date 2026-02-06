import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v6.5", layout="wide")

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
    # --- 1. PROCESAMIENTO MAESTRO Y PRECIOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if 'Precio' in df_ma.columns:
        df_ma['Precio_Num'] = pd.to_numeric(df_ma['Precio'], errors='coerce').fillna(0)
        # Definimos bins fijos para evitar que desaparezcan las categorías
        bins = [0, 20000, 40000, 60000, 80000, 100000, 150000, 10000000]
        labels = ['<20k', '20k-40k', '40k-60k', '60k-80k', '80k-100k', '100k-150k', '>150k']
        df_ma['Franja Precio'] = pd.cut(df_ma['Precio_Num'], bins=bins, labels=labels, include_lowest=True)
        # Convertimos a string para evitar errores de Plotly con categorías vacías
        df_ma['Franja Precio'] = df_ma['Franja Precio'].astype(str)

    # --- 2. STOCK (FOTO + CLIENTE + FECHA) ---
    stk_raw = data.get('Stock', pd.DataFrame())
    st_dass_grp = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    st_cli_grp = pd.DataFrame(columns=['SKU', 'Stock Clientes', 'Cliente'])
    fecha_foto_cli = "N/A"

    if not stk_raw.empty:
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Ubicacion'] = stk_raw['Ubicacion'].fillna('').astype(str).str.upper()
        stk_raw['Cliente'] = stk_raw['Cliente'].fillna('SIN CLIENTE').astype(str).str.strip()
        
        stk_s = stk_raw.sort_values(by='Fecha')
        mask_d = stk_s['Ubicacion'].str.contains('DASS|CENTRAL|DEP|PROPIO|LOG|MAYORISTA', na=False)
        
        st_dass_grp = stk_s[mask_d].groupby('SKU')['Cant'].last().reset_index().rename(columns={'Cant': 'Stock Dass'})
        st_cli_grp = stk_s[~mask_d].groupby(['SKU', 'Cliente']).agg({'Cant': 'last', 'Fecha': 'max'}).reset_index().rename(columns={'Cant': 'Stock Clientes'})
        if not stk_s[~mask_d].empty:
            fecha_foto_cli = stk_s[~mask_d]['Fecha'].max().strftime('%d/%m/%Y')

    # --- 3. VENTAS (SELL IN / SELL OUT) ---
    si_raw = data.get('Sell_in', pd.DataFrame())
    si_grp = si_raw.copy()
    if not si_grp.empty:
        si_grp['Sell in'] = pd.to_numeric(si_grp['Unidades'], errors='coerce').fillna(0)
        si_grp = si_grp.groupby(['SKU', 'Cliente'])['Sell in'].sum().reset_index()

    so_raw = data.get('Sell_out', pd.DataFrame())
    so_final = pd.DataFrame(columns=['SKU', 'Sell out Clientes', 'Sell out tiendas', 'Cliente'])
    if not so_raw.empty:
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Tipo'] = so_raw['Tipo'].fillna('').astype(str).str.upper()
        so_c = so_raw[so_raw['Tipo'].str.contains('CLIENTE')].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Clientes'})
        so_t = so_raw[so_raw['Tipo'].str.contains('TIENDA')].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out tiendas'})
        so_final = so_c.merge(so_t, on=['SKU', 'Cliente'], how='outer').fillna(0)

    # --- 4. FILTROS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros de Gestión")
    clis_all = sorted(list(set(si_grp['Cliente'].dropna().unique().tolist() + st_cli_grp['Cliente'].dropna().unique().tolist())))
    f_cli = st.sidebar.multiselect("Cliente", [c for c in clis_all if str(c) != '0'])
    f_dis = st.sidebar.multiselect("Disciplina", sorted(df_ma['Disciplina'].unique().tolist()))
    f_pre = st.sidebar.multiselect("Franja de Precio", sorted(df_ma['Franja Precio'].unique().tolist()))

    # Aplicar Filtros
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
    if f_pre: df = df[df['Franja Precio'].isin(f_pre)]

    # --- 5. VISUALIZACIÓN ---
    st.title("📊 Torre de Control Dass v6.5")
    st.info(f"📅 Stock Clientes basado en la foto del: **{fecha_foto_cli}**")

    # Función Segura para Gráficos
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

    # FILA 2: Por Franja de Precio
    st.subheader("💰 Participación por Franja de Precio")
    p1, p2, p3 = st.columns(3)
    safe_pie(df, 'Stock Dass', 'Franja Precio', "Stock Dass por $", p1)
    safe_pie(df, 'Sell in', 'Franja Precio', "Ingresos por $", p2)
    safe_pie(df, 'Sell out Clientes', 'Franja Precio', "Sell Out por $", p3)

    # --- 6. RANKING ---
    st.divider()
    st.subheader("🏆 Ranking de Productos")
    
    df['Stock/Sellin'] = np.where(df['Sell in']>0, (df['Stock Dass']+df['Stock Clientes'])/df['Sell in'], 0)
    df['WOS'] = np.where(df['Sell out Clientes']>0, df['Stock Clientes']/df['Sell out Clientes'], 0)

    cols_rank = ['SKU', 'Descripcion', 'Sell in', 'Sell out Clientes', 'Sell out tiendas', 'Stock Dass', 'Stock Clientes', 'Stock/Sellin', 'WOS']
    st.dataframe(
        df[cols_rank].sort_values('Sell out Clientes', ascending=False).style.format({
            'Sell in':'{:,.0f}', 'Sell out Clientes':'{:,.0f}', 'Sell out tiendas':'{:,.0f}',
            'Stock Dass':'{:,.0f}', 'Stock Clientes':'{:,.0f}', 'Stock/Sellin':'{:.2f}', 'WOS':'{:.2f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else '', subset=['WOS']),
        use_container_width=True, height=500
    )
else:
    st.info("Conectando con Google Drive...")
