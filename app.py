import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v6.3", layout="wide")

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
    # --- 1. PROCESAMIENTO MAESTRO ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if 'Precio' in df_maestro.columns:
        df_maestro['Precio_Num'] = pd.to_numeric(df_maestro['Precio'], errors='coerce').fillna(0)
        bins = [0, 20000, 40000, 60000, 80000, 100000, 150000, 999999]
        labels = ['<20k', '20k-40k', '40k-60k', '60k-80k', '80k-100k', '100k-150k', '>150k']
        df_maestro['Franja Precio'] = pd.cut(df_maestro['Precio_Num'], bins=bins, labels=labels)

    # --- 2. PROCESAMIENTO DE STOCK (FOTO + CLIENTE) ---
    stk_raw = data.get('Stock', pd.DataFrame())
    st_dass_grp = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    st_cli_grp = pd.DataFrame(columns=['SKU', 'Stock Clientes', 'Cliente'])
    fecha_info = "N/A"

    if not stk_raw.empty:
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Ubicacion'] = stk_raw['Ubicacion'].fillna('').astype(str).str.upper()
        # Aseguramos que la columna cliente exista y esté limpia
        stk_raw['Cliente'] = stk_raw['Cliente'].fillna('SIN CLIENTE').astype(str).str.strip()
        
        stk_sorted = stk_raw.sort_values(by='Fecha', ascending=True)
        mask_dass = stk_sorted['Ubicacion'].str.contains('DASS|CENTRAL|DEP|PROPIO|LOG|MAYORISTA', na=False)
        
        # Stock Dass: Foto actual
        st_dass_grp = stk_sorted[mask_dass].groupby('SKU')['Cant'].last().reset_index().rename(columns={'Cant': 'Stock Dass'})
        
        # Stock Clientes: Foto por SKU y Cliente para permitir filtrado cruzado
        st_cli_grp = stk_sorted[~mask_dass].groupby(['SKU', 'Cliente']).agg({'Cant': 'last', 'Fecha': 'max'}).reset_index().rename(columns={'Cant': 'Stock Clientes'})
        fecha_info = stk_sorted[~mask_dass]['Fecha'].max().strftime('%d/%m/%Y') if not stk_sorted[~mask_dass].empty else "N/A"

    # --- 3. PROCESAMIENTO DE VENTAS ---
    # Sell In
    si_raw = data.get('Sell_in', pd.DataFrame())
    si_grp = pd.DataFrame(columns=['SKU', 'Sell in', 'Cliente'])
    if not si_raw.empty:
        si_raw['Sell in'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        si_grp = si_raw.groupby(['SKU', 'Cliente'])['Sell in'].sum().reset_index()

    # Sell Out
    so_raw = data.get('Sell_out', pd.DataFrame())
    so_final = pd.DataFrame(columns=['SKU', 'Sell out Clientes', 'Sell out tiendas', 'Cliente'])
    if not so_raw.empty:
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Tipo'] = so_raw['Tipo'].fillna('').astype(str).str.upper()
        # Agrupamos también por cliente en Sell Out
        so_c = so_raw[so_raw['Tipo'].str.contains('CLIENTE')].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Clientes'})
        so_t = so_raw[so_raw['Tipo'].str.contains('TIENDA')].groupby(['SKU', 'Cliente'])['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out tiendas'})
        so_final = so_c.merge(so_t, on=['SKU', 'Cliente'], how='outer').fillna(0)

    # --- 4. ENSAMBLE Y FILTROS ---
    st.sidebar.header("🔍 Filtros de Gestión")
    # Consolidamos lista de clientes de todas las fuentes
    clientes_list = sorted(list(set(si_grp['Cliente'].unique().tolist() + st_cli_grp['Cliente'].unique().tolist())))
    f_cli = st.sidebar.multiselect("Seleccionar Cliente/Tienda", [c for c in clientes_list if str(c) != '0'])
    
    # Aplicar filtro de cliente antes del merge final para eficiencia
    if f_cli:
        si_grp = si_grp[si_grp['Cliente'].isin(f_cli)]
        so_final = so_final[so_final['Cliente'].isin(f_cli)]
        st_cli_grp = st_cli_grp[st_cli_grp['Cliente'].isin(f_cli)]

    # Merge Final
    df = df_maestro.merge(st_dass_grp, on='SKU', how='left')
    df = df.merge(si_grp.groupby('SKU')['Sell in'].sum().reset_index(), on='SKU', how='left')
    df = df.merge(so_final.groupby('SKU')[['Sell out Clientes', 'Sell out tiendas']].sum().reset_index(), on='SKU', how='left')
    df = df.merge(st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index(), on='SKU', how='left')
    df = df.fillna(0)

    # --- 5. VISUALIZACIÓN ---
    st.title("📊 Torre de Control Dass v6.3")
    st.info(f"📅 Stock Clientes basado en la foto del: **{fecha_info}**")

    # Gráficos (Fila 1: Disciplina | Fila 2: Precio)
    # ... (Se mantienen las mismas funciones de gráficos de la v6.2) ...

    # --- 6. RANKING ---
    st.divider()
    st.subheader("🏆 Ranking Detallado")
    
    df['WOS'] = np.where(df['Sell out Clientes']>0, df['Stock Clientes']/df['Sell out Clientes'], 0)
    df['Stock/Sellin'] = np.where(df['Sell in']>0, (df['Stock Dass']+df['Stock Clientes'])/df['Sell in'], 0)

    def semaforo(v):
        if v > 3: return 'background-color: #ffcccc; color: #900'
        if 0 < v <= 1: return 'background-color: #ccffcc; color: #006400'
        return ''

    cols = ['SKU', 'Descripcion', 'Sell in', 'Sell out Clientes', 'Sell out tiendas', 'Stock Dass', 'Stock Clientes', 'Stock/Sellin', 'WOS']
    st.dataframe(
        df[cols].sort_values('Sell out Clientes', ascending=False).style.format({
            'Sell in':'{:,.0f}', 'Sell out Clientes':'{:,.0f}', 'Sell out tiendas':'{:,.0f}',
            'Stock Dass':'{:,.0f}', 'Stock Clientes':'{:,.0f}', 'Stock/Sellin':'{:.2f}', 'WOS':'{:.2f}'
        }).map(semaforo, subset=['WOS']),
        use_container_width=True, height=500
    )

else:
    st.info("Configurando motor de datos con columna Cliente...")
