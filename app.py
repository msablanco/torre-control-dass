import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="Dass Performance v5.3", layout="wide")

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
        st.error(f"Error en carga: {e}")
        return None

data = load_data()

if data:
    # --- 1. PROCESAMIENTO CON UNIFICACIÓN POR SKU ---
    # Usamos el Maestro como base de atributos
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # Procesar Sell In (Ingresos)
    si_raw = data.get('Sell_in', pd.DataFrame())
    if not si_raw.empty:
        si_raw['Unidades'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        # Guardamos mapeo de Cliente por SKU (tomamos el primero que aparezca)
        clientes_map = si_raw.groupby('SKU')['Cliente'].first().reset_index()
        si_grouped = si_raw.groupby('SKU')['Unidades'].sum().reset_index().rename(columns={'Unidades': 'Sell In'})
    else:
        clientes_map = pd.DataFrame(columns=['SKU', 'Cliente'])
        si_grouped = pd.DataFrame(columns=['SKU', 'Sell In'])

    # Procesar Sell Out
    so_raw = data.get('Sell_out', pd.DataFrame())
    if not so_raw.empty:
        so_raw['Unidades'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_grouped = so_raw.groupby('SKU')['Unidades'].sum().reset_index().rename(columns={'Unidades': 'Sell Out'})
    else:
        so_grouped = pd.DataFrame(columns=['SKU', 'Sell Out'])

    # Procesar Stocks
    stk_raw = data.get('Stock', pd.DataFrame())
    if not stk_raw.empty:
        stk_raw['Cantidad'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Ubicacion'] = stk_raw['Ubicacion'].astype(str).str.upper()
        mask_dass = stk_raw['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)
        
        st_dass = stk_raw[mask_dass].groupby('SKU')['Cantidad'].sum().reset_index().rename(columns={'Cantidad': 'Stock Dass'})
        st_cli = stk_raw[~mask_dass].groupby('SKU')['Cantidad'].sum().reset_index().rename(columns={'Cantidad': 'Stock Clientes'})
    else:
        st_dass = st_cli = pd.DataFrame(columns=['SKU', 'Stock Dass', 'Stock Clientes'])

    # --- 2. ENSAMBLE FINAL ---
    df = df_maestro.merge(clientes_map, on='SKU', how='left')
    df = df.merge(si_grouped, on='SKU', how='left').merge(so_grouped, on='SKU', how='left')
    df = df.merge(st_dass, on='SKU', how='left').merge(st_cli, on='SKU', how='left')
    
    # Limpieza absoluta de nulos antes de calcular
    df = df.fillna(0)

    # --- 3. CÁLCULOS DE PERFORMANCE ---
    df['Ingresos'] = df['Sell In']
    # Sell Through = (Vendido / Ingresado)
    df['Sell Through %'] = df.apply(lambda x: (x['Sell Out'] / x['Sell In'] * 100) if x['Sell In'] > 0 else 0, axis=1)
    # Rotación = Stock Clientes / Sell Out (Meses)
    df['Rotacion'] = df.apply(lambda x: (x['Stock Clientes'] / x['Sell Out']) if x['Sell Out'] > 0 else 0, axis=1)

    # --- 4. FILTROS SUPERIORES ---
    st.title("👟 Performance Dass v5.3")
    
    c_f1, c_f2 = st.columns(2)
    with c_f1:
        clientes_list = sorted([str(x) for x in df['Cliente'].unique() if x != 0])
        f_cli = st.multiselect("Seleccionar Clientes", clientes_list)
    with c_f2:
        disc_list = sorted([str(x) for x in df['Disciplina'].unique() if x != 0])
        f_dis = st.multiselect("Seleccionar Disciplina", disc_list)

    # Aplicar filtros
    if f_cli: df = df[df['Cliente'].isin(f_cli)]
    if f_dis: df = df[df['Disciplina'].isin(f_dis)]

    # --- 5. VISUALIZACIÓN ---
    st.subheader("📊 Análisis Unificado por SKU")
    
    columnas_finales = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion']
    df_view = df[[c for c in columnas_finales if c in df.columns]].copy()

    # Convertimos a tipos correctos para evitar errores de Styler
    cols_num = ['Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion']
    for cn in cols_num:
        if cn in df_view.columns:
            df_view[cn] = pd.to_numeric(df_view[cn], errors='coerce').fillna(0)

    # Formateo Manual (Más seguro que .style.format para grandes volúmenes)
    st.dataframe(
        df_view.style.format({
            'Ingresos': '{:,.0f}', 'Sell In': '{:,.0f}', 'Sell Out': '{:,.0f}',
            'Stock Clientes': '{:,.0f}', 'Stock Dass': '{:,.0f}',
            'Sell Through %': '{:.1f}%', 'Rotacion': '{:.2f} m'
        }),
        use_container_width=True, height=500
    )

    # Totales
    st.divider()
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Sell In Total", f"{df_view['Sell In'].sum():,.0f}")
    t2.metric("Sell Out Total", f"{df_view['Sell Out'].sum():,.0f}")
    t3.metric("Stock Dass", f"{df_view['Stock Dass'].sum():,.0f}")
    t4.metric("Stock Clientes", f"{df_view['Stock Clientes'].sum():,.0f}")

else:
    st.info("Buscando archivos en Drive...")
