import streamlit as st
import pd as pd
import io
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. CONFIGURACIÓN --- [cite: 1]
st.set_page_config(page_title="FILA - Torre de Control", layout="wide")

def fmt_p(valor):
    if pd.isna(valor): return "0"
    return f"{valor:,.0f}".replace(",", ".")

# --- 2. SIDEBAR ---
st.sidebar.header("🎯 CONTROL DE VOLUMEN")
vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 FILTROS")

@st.cache_data(ttl=600)
def load_drive_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"] [cite: 2]
        results = service.files().list(q=f"'{folder_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
        files = results.get('files', [])
        dfs = {}
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            request = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request) [cite: 3]
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = [str(c).strip().upper() for c in df.columns]
            
            # Normalización de columnas comunes [cite: 4]
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CANT': 'CANTIDAD', 'QTY': 'CANTIDAD', 'UNIDADES': 'CANTIDAD'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            # PARCHE ESPECÍFICO PARA SELL IN WHOLESALE 
            if "SELL_IN_VENTAS" in name.upper():
                if 'EMPRENDIMIENTO' not in df.columns:
                    df['EMPRENDIMIENTO'] = 'WHOLESALE'
                if 'CANTIDAD' not in df.columns and len(df.columns) >= 7:
                    df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
            
            if 'CANTIDAD' in df.columns:
                df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce').fillna(0)
                
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_drive_data()

if data:
    sell_out = data.get('Sell_Out', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', data.get('Sell_In', pd.DataFrame())) [cite: 7]
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Seleccionar Emprendimiento (Canal)", opciones_emp)
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # --- 3. PROCESAMIENTO SELL OUT ---
    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if any(x in c for x in ['FECHA', 'MES', 'DATE'])), None) [cite: 8]
        if col_f:
            sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
            sell_out['MES_NUM'] = sell_out['FECHA_DT'].dt.month
            sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    so_2025 = so_2025.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')

    df_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)] if f_emp else so_2025.copy()
    df_vista = df_canal.copy()
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query, na=False)] [cite: 9]

    # --- 4. ESCALA ---
    base_escala = df_canal['CANTIDAD'].sum() if validar_fijar else df_vista['CANTIDAD'].sum()
    factor_escala = vol_obj / base_escala if base_escala > 0 else 1

    # --- 5. SERIES TIEMPO ---
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    v_out_25 = df_vista.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_26 = (v_out_25 * factor_escala).round(0)

    # --- 6. SELL IN ---
    v_in_25 = pd.Series(0, index=meses_idx) [cite: 10]
    if not sell_in.empty:
        col_f_in = next((c for c in sell_in.columns if any(x in c for x in ['FECHA', 'MES', 'DATE'])), None)
        if col_f_in:
            si_temp = sell_in.copy()
            si_temp['FECHA_DT'] = pd.to_datetime(si_temp[col_f_in], dayfirst=True, errors='coerce')
            si_25 = si_temp[si_temp['FECHA_DT'].dt.year == 2025].copy()
            si_25['MES_NUM'] = si_25['FECHA_DT'].dt.month [cite: 11]
            si_25 = si_25.merge(maestro[['SKU', 'DESCRIPCION']], on='SKU', how='left')
            if f_emp:
                si_25 = si_25[si_25['EMPRENDIMIENTO'].isin(f_emp)] if 'EMPRENDIMIENTO' in si_25.columns else si_25
            if query:
                si_25 = si_25[si_25['SKU'].str.contains(query) | si_25['DESCRIPCION'].str.contains(query, na=False)] [cite: 12, 13]
            v_in_25 = si_25.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # --- 7. INTERFAZ ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)"])

    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyección en Vista", fmt_p(v_proy_26.sum()))
        c2.metric("Objetivo", fmt_p(vol_obj))
        c3.metric("Escala", f"{factor_escala:.4f}")

        fig = go.Figure() [cite: 14]
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25.values, name="Sell In 2025", line=dict(color='#3366CC', width=3)))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25.values, name="Sell Out 2025", line=dict(dash='dot', color='#FF9900')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26.values, name="Proyección 2026", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Detalle Mensual")
        df_m = pd.DataFrame({"Sell In 2025": v_in_25.values, "Sell Out 2025": v_out_25.values, "Proy 2026": v_proy_26.values}, index=meses_labels).T
        st.dataframe(df_m.style.format(lambda x: fmt_p(x)), use_container_width=True) [cite: 15]

        if not df_vista.empty:
            st.subheader("🧪 Proyección por Disciplina")
            disc_proy = (df_vista.groupby(['DISCIPLINA', 'MES_NUM'])['CANTIDAD'].sum().unstack(fill_value=0) * factor_escala).round(0)
            disc_proy.columns = [meses_labels[int(i)-1] for i in disc_proy.columns if i in range(1,13)]
            st.dataframe(disc_proy.style.format(lambda x: fmt_p(x)), use_container_width=True) [cite: 16]

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        vta_sku_25 = df_canal.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'V25'})
        tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['V_PROY_26'] = (tactical['V25'] * factor_escala).round(0)
        tactical['V_MENSUAL'] = (tactical['V_PROY_26'] / 12).round(0)
        tactical['MOS'] = (tactical['STK'] / (tactical['V_MENSUAL'].replace(0, 1))).round(1) [cite: 17]
        if query: tactical = tactical[tactical['SKU'].str.contains(query) | tactical['DESCRIPCION'].str.contains(query, na=False)] [cite: 18]
        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STK', 'V25', 'V_MENSUAL', 'MOS']].sort_values('V_MENSUAL', ascending=False).style.format({
            'STK': lambda x: fmt_p(x), 'V25': lambda x: fmt_p(x), 'V_MENSUAL': lambda x: fmt_p(x), 'MOS': "{:.1f}"
        }), use_container_width=True)
else:
    st.info("Cargando datos...")import streamlit as st
import pandas as pd
import io
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="FILA - Torre de Control", layout="wide")

def fmt_p(valor):
    if pd.isna(valor): return "0"
    return f"{valor:,.0f}".replace(",", ".")

# --- 2. CARGA DE DATOS ---
@st.cache_data(ttl=600)
def load_drive_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        results = service.files().list(q=f"'{folder_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
        files = results.get('files', [])
        dfs = {}
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            request = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = [str(c).strip().upper() for c in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Normalización
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CANT': 'CANTIDAD', 'QTY': 'CANTIDAD', 'UNIDADES': 'CANTIDAD'})
            if 'SKU' in df.columns: 
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            # Corrección de la lógica de Sell In
            if "SELL_IN_VENTAS" in name.upper():
                if 'EMPRENDIMIENTO' not in df.columns: 
                    df['EMPRENDIMIENTO'] = 'WHOLESALE'
                # Buscar columna de fecha en Sell In (usualmente la 2da)
                if len(df.columns) >= 2:
                    df = df.rename(columns={df.columns[1]: 'F_REF'})
                # Buscar columna de cantidad en Sell In (usualmente la 7ma)
                if len(df.columns) >= 7:
                    df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
            
            if 'CANTIDAD' in df.columns:
                df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce').fillna(0)
            
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_drive_data()

if data:
    # 3. ASIGNACIÓN
    sell_out = data.get('Sell_Out', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    # 4. SIDEBAR
    st.sidebar.header("🎯 CONTROL DE VOLUMEN")
    vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)
    validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 FILTROS")
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Canal", opciones_emp)
    query = st.sidebar.text_input("Buscar SKU/Desc", "").upper()

    # 5. PROCESAMIENTO FECHAS (Blindado contra errores de lista)
    def process_dates(df, keywords):
        if df.empty: return df
        col = next((c for c in df.columns if any(k in c for k in keywords)), None)
        if col:
            df['FECHA_DT'] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            df['MES_NUM'] = df['FECHA_DT'].dt.month
            df['AÑO'] = df['FECHA_DT'].dt.year
        else:
            df['FECHA_DT'], df['MES_NUM'], df['AÑO'] = pd.NaT, 0, 0
        return df

    sell_out = process_dates(sell_out, ['FECHA', 'DATE', 'MES'])
    sell_in = process_dates(sell_in, ['F_REF', 'FECHA', 'DATE'])

    # 6. FILTROS Y ESCALA
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    so_2025 = so_2025.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')
    df_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)] if f_emp else so_2025.copy()
    
    df_vista = df_canal.copy()
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query, na=False)]

    base_escala = df_canal['CANTIDAD'].sum() if validar_fijar else df_vista['CANTIDAD'].sum()
    factor_escala = vol_obj / base_escala if base_escala > 0 else 1

    # 7. SERIES TEMPORALES
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    v_out_25 = df_vista.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_26 = (v_out_25 * factor_escala).round(0)

    si_25_filt = sell_in[sell_in['AÑO'] == 2025].copy()
    if f_emp and 'EMPRENDIMIENTO' in si_25_filt.columns: 
        si_25_filt = si_25_filt[si_25_filt['EMPRENDIMIENTO'].isin(f_emp)]
    si_25_filt = si_25_filt.merge(maestro[['SKU', 'DESCRIPCION']], on='SKU', how='left')
    if query:
        si_25_filt = si_25_filt[si_25_filt['SKU'].str.contains(query) | si_25_filt['DESCRIPCION'].str.contains(query, na=False)]
    v_in_25 = si_25_filt.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # 8. TABS
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "⚡ ESTRATEGIA (MOS)"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        col1.metric("Proyección Anual", fmt_p(v_proy_26.sum()))
        col2.metric("Factor Escala", f"{factor_escala:.4f}")
        col3.metric("Sell Out 25", fmt_p(v_out_25.sum()))

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25.values, name="Sell In 25", line=dict(color='#3366CC', width=3)))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25.values, name="Sell Out 25", line=dict(dash='dot', color='#FF9900')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26.values, name="Proyección 26", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        st.write("### 📋 Resumen Mensual")
        df_m = pd.DataFrame({"Sell In 25": v_in_25.values, "Sell Out 25": v_out_25.values, "Proy 26": v_proy_26.values}, index=meses_labels).T
        st.dataframe(df_m.style.format(fmt_p), use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        vta_sku_25 = df_canal.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'V25'})
        tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['V_PROY_26'] = (tactical['V25'] * factor_escala).round(0)
        tactical['V_MENSUAL'] = (tactical['V_PROY_26'] / 12).round(0)
        tactical['MOS'] = (tactical['STK'] / (tactical['V_MENSUAL'].replace(0, 1))).round(1)
        
        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STK', 'V25', 'V_MENSUAL', 'MOS']].sort_values('V_MENSUAL', ascending=False).style.format({
            'STK': fmt_p, 'V25': fmt_p, 'V_MENSUAL': fmt_p, 'MOS': "{:.1f}"
        }), use_container_width=True)
else:
    st.info("Conectando con Google Drive...")

