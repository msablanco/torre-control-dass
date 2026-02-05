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
            # Leer como string para evitar errores de tipo
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None

data = load_data()

if data:
    # 1. PREPARACIÓN DE BASES
    maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    
    # Procesar Sell In (Ingresos) y mapear Clientes
    si_raw = data.get('Sell_in', pd.DataFrame())
    if not si_raw.empty:
        si_raw['Unidades'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        # Extraemos clientes por SKU
        cli_map = si_raw.groupby('SKU')['Cliente'].first().reset_index()
        si_grp = si_raw.groupby('SKU')['Unidades'].sum().reset_index().rename(columns={'Unidades': 'Sell In'})
    else:
        cli_map = pd.DataFrame(columns=['SKU', 'Cliente'])
        si_grp = pd.DataFrame(columns=['SKU', 'Sell In'])

    # Procesar Sell Out
    so_raw = data.get('Sell_out', pd.DataFrame())
    so_grp = pd.DataFrame(columns=['SKU', 'Sell Out'])
    if not so_raw.empty:
        so_raw['Unidades'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_grp = so_raw.groupby('SKU')['Unidades'].sum().reset_index().rename(columns={'Unidades': 'Sell Out'})

    # Procesar Stocks (Dass vs Clientes)
    stk_raw = data.get('Stock', pd.DataFrame())
    stk_d = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    stk_c = pd.DataFrame(columns=['SKU', 'Stock Clientes'])
    if not stk_raw.empty:
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Ubicacion'] = stk_raw['Ubicacion'].astype(str).str.upper()
        mask = stk_raw['Ubicacion'].str.contains('DASS|CENTRAL|DEPOSITO', na=False)
        stk_d = stk_raw[mask].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        stk_c = stk_raw[~mask].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

    # 2. UNIFICACIÓN TOTAL
    df = maestro.merge(cli_map, on='SKU', how='left')
    df = df.merge(si_grp, on='SKU', how='left').merge(so_grp, on='SKU', how='left')
    df = df.merge(stk_d, on='SKU', how='left').merge(stk_c, on='SKU', how='left')
    
    # Limpiar nulos para evitar errores de Streamlit
    df = df.fillna(0)

    # 3. CÁLCULOS
    df['Ingresos'] = df['Sell In']
    df['Sell Through %'] = df.apply(lambda x: (x['Sell Out'] / x['Sell In'] * 100) if x['Sell In'] > 0 else 0, axis=1)
    # Rotación: Stock Clientes / Sell Out (Meses de cobertura en la calle)
    df['Rotacion'] = df.apply(lambda x: (x['Stock Clientes'] / x['Sell Out']) if x['Sell Out'] > 0 else 0, axis=1)

    # 4. FILTROS SUPERIORES
    st.title("👟 Performance Dass v5.3")
    
    f1, f2 = st.columns(2)
    with f1:
        u_clientes = sorted([str(x) for x in df['Cliente'].unique() if x != 0 and x != '0'])
        sel_cli = st.multiselect("Filtrar por Clientes", u_clientes)
    with f2:
        u_disc = sorted([str(x) for x in df['Disciplina'].unique() if x != 0 and x != '0'])
        sel_disc = st.multiselect("Filtrar por Disciplina", u_disc)

    if sel_cli: df = df[df['Cliente'].isin(sel_cli)]
    if sel_disc: df = df[df['Disciplina'].isin(sel_disc)]

    # 5. TABLA FINAL
    st.subheader("📊 Análisis Consolidado por SKU")
    
    cols = ['SKU', 'Descripcion', 'Disciplina', 'Color', 'Genero', 'Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion']
    df_view = df[[c for c in cols if c in df.columns]].copy()

    # Asegurar tipos numéricos antes de formatear
    num_cols = ['Ingresos', 'Sell In', 'Sell Out', 'Stock Clientes', 'Stock Dass', 'Sell Through %', 'Rotacion']
    for nc in num_cols:
        if nc in df_view.columns:
            df_view[nc] = pd.to_numeric(df_view[nc], errors='coerce').fillna(0)

    st.dataframe(
        df_view.style.format({
            'Ingresos': '{:,.0f}', 'Sell In': '{:,.0f}', 'Sell Out': '{:,.0f}',
            'Stock Clientes': '{:,.0f}', 'Stock Dass': '{:,.0f}',
            'Sell Through %': '{:.1f}%', 'Rotacion': '{:.2f} m'
        }),
        use_container_width=True, height=600
    )

    # Resumen inferior
    st.divider()
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell In", f"{df_view['Sell In'].sum():,.0f}")
    k2.metric("Sell Out", f"{df_view['Sell Out'].sum():,.0f}")
    k3.metric("Stock Dass", f"{df_view['Stock Dass'].sum():,.0f}")
    k4.metric("Stock Mercado", f"{df_view['Stock Clientes'].sum():,.0f}")

else:
    st.info("Cargando archivos desde Google Drive...")
