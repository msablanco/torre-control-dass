import streamlit as st
import pandas as pd
import io
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="FILA - Torre de Control", layout="wide")

# --- 2. SIDEBAR ---
st.sidebar.header("🎯 CONTROL DE VOLUMEN")
vol_obj = st.sidebar.number_input("Volumen Total Objetivo 2026", value=1000000, step=50000)
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 FILTROS DE VISTA")
query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

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
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_drive_data()

if data:
    sell_out = data.get('Sell_Out', pd.DataFrame())
    sell_in = data.get('Sell_In', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    # Filtro de Emprendimiento solicitado
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)

    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year
        sell_out['MES_NUM'] = sell_out['FECHA_DT'].dt.month

    # --- 3. LÓGICA DE ESCALA Y COINCIDENCIA CON OBJETIVO ---
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    so_2025 = so_2025.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')
    
    # Aplicar filtros antes de calcular escala si no está fijada
    df_filtrado = so_2025.copy()
    if f_emp: df_filtrado = df_filtrado[df_filtrado['EMPRENDIMIENTO'].isin(f_emp)]
    if query: df_filtrado = df_filtrado[df_filtrado['SKU'].str.contains(query) | df_filtrado['DESCRIPCION'].str.contains(query, na=False)]

    if validar_fijar:
        factor_escala = vol_obj / so_2025['CANTIDAD'].sum() if not so_2025.empty else 1
    else:
        factor_escala = vol_obj / df_filtrado['CANTIDAD'].sum() if not df_filtrado.empty else 1

    # --- 4. DATA PARA PERFORMANCE ---
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    v_out_25 = df_filtrado.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    
    # Procesar Sell In 2025 para el gráfico
    si_2025 = sell_in.copy()
    if 'FECHA' in si_2025.columns:
        si_2025['FECHA_DT'] = pd.to_datetime(si_2025['FECHA'], dayfirst=True, errors='coerce')
        si_2025 = si_2025[si_2025['FECHA_DT'].dt.year == 2025]
        si_2025['MES_NUM'] = si_2025['FECHA_DT'].dt.month
        si_2025 = si_2025.merge(maestro[['SKU', 'DESCRIPCION']], on='SKU', how='left')
        if f_emp and 'EMPRENDIMIENTO' in si_2025.columns: si_2025 = si_2025[si_2025['EMPRENDIMIENTO'].isin(f_emp)]
        if query: si_2025 = si_2025[si_2025['SKU'].str.contains(query) | si_2025['DESCRIPCION'].str.contains(query, na=False)]
        v_in_25 = si_2025.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    else:
        v_in_25 = pd.Series(0, index=meses_idx)

    v_proy_26 = (v_out_25 * factor_escala).round(0)

    # --- 5. INTERFAZ ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)"])

    with tab1:
        st.subheader("📈 Curva de Proyección 2026")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25, name="Sell In 2025", line=dict(color='#3366CC')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25, name="Sell Out 2025", line=dict(dash='dot', color='#FF9900')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26, name="Proyección 2026", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Detalle de Valores Mensuales")
        df_mensual = pd.DataFrame({
            "Mes": meses_labels,
            "Sell In 2025": v_in_25.values,
            "Sell Out 2025": v_out_25.values,
            "Proyección 2026": v_proy_26.values
        }).set_index("Mes")
        # Fila de totales para validar coincidencia con objetivo
        totales = df_mensual.sum()
        df_mensual.loc['TOTAL'] = totales
        st.dataframe(df_mensual.T, use_container_width=True)

        st.subheader("🧪 Proyección 2026 por Disciplina")
        disc_data = df_filtrado.groupby(['DISCIPLINA', 'MES_NUM'])['CANTIDAD'].sum().unstack(fill_value=0)
        disc_proy = (disc_data * factor_escala).round(0)
        disc_proy.columns = meses_labels
        disc_proy['TOTAL'] = disc_proy.sum(axis=1)
        st.dataframe(disc_proy, use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})
        vta_sku_25 = so_2025.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
        tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['VTA_PROY_26'] = (tactical['VTA_25'] * factor_escala).round(0)
        tactical['VTA_MENSUAL'] = (tactical['VTA_PROY_26'] / 12).round(0)
        tactical['MOS'] = (tactical['STK_ACTUAL'] / tactical['VTA_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)
        if query:
            tactical = tactical[tactical['SKU'].str.contains(query) | tactical['DESCRIPCION'].str.contains(query, na=False)]
        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STK_ACTUAL', 'VTA_25', 'VTA_MENSUAL', 'MOS']].sort_values('VTA_MENSUAL', ascending=False), use_container_width=True)
else:
    st.info("Cargando datos...")
