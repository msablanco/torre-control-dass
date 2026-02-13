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
vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)

# Botón de validación
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False, 
                                    help="Fija el factor de escala basándose en el Emprendimiento seleccionado.")

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 FILTROS")

# Cargamos datos primero para obtener los emprendimientos
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

    # Filtro Emprendimiento (Canal)
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Seleccionar Emprendimiento (Canal)", opciones_emp)
    
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # Preparación base
    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year
        sell_out['MES_NUM'] = sell_out['FECHA_DT'].dt.month

    so_base = sell_out[sell_out['AÑO'] == 2025].copy()
    so_base = so_base.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')

    # --- LÓGICA DE ESCALA POR CANAL ---
    # 1. Filtramos por Emprendimiento para definir la BASE del objetivo
    df_canal = so_base.copy()
    if f_emp:
        df_canal = df_canal[df_canal['EMPRENDIMIENTO'].isin(f_emp)]
    
    venta_total_canal = df_canal['CANTIDAD'].sum()

    # 2. Filtramos por búsqueda para la VISTA
    df_vista = df_canal.copy()
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query, na=False)]
    
    venta_en_vista = df_vista['CANTIDAD'].sum()

    if validar_fijar:
        # ESCALA FIJA: El objetivo se reparte sobre el TOTAL DEL CANAL seleccionado
        factor_escala = vol_obj / venta_total_canal if venta_total_canal > 0 else 1
    else:
        # ESCALA DINÁMICA: El objetivo se fuerza sobre lo que hay en PANTALLA
        factor_escala = vol_obj / venta_en_vista if venta_en_vista > 0 else 1

    # --- CÁLCULOS ---
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    v_out_25 = df_vista.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_26 = (v_out_25 * factor_escala).round(0)

    # Sell In (Filtrado por canal y búsqueda)
    si_base = sell_in.copy()
    v_in_25 = pd.Series(0, index=meses_idx)
    if not si_base.empty and 'FECHA' in si_base.columns:
        si_base['FECHA_DT'] = pd.to_datetime(si_base['FECHA'], dayfirst=True, errors='coerce')
        si_25 = si_base[si_base['FECHA_DT'].dt.year == 2025].copy()
        si_25['MES_NUM'] = si_25['FECHA_DT'].dt.month
        si_25 = si_25.merge(maestro[['SKU', 'DESCRIPCION']], on='SKU', how='left')
        if f_emp and 'EMPRENDIMIENTO' in si_25.columns: si_25 = si_25[si_25['EMPRENDIMIENTO'].isin(f_emp)]
        if query: si_25 = si_25[si_25['SKU'].str.contains(query) | si_25['DESCRIPCION'].str.contains(query, na=False)]
        v_in_25 = si_25.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # --- INTERFAZ ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "⚡ TACTICAL (MOS)"])

    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyección en Vista", f"{v_proy_26.sum():,.0f} u.")
        c2.metric("Objetivo Canal", f"{vol_obj:,.0f} u.")
        c3.metric("Factor Escala", f"{factor_escala:.4f}")

        if validar_fijar:
            st.success(f"🔒 Escala bloqueada para el canal: {', '.join(f_emp) if f_emp else 'Todos'}")

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25, name="Sell In 2025", line=dict(color='#3366CC')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25, name="Sell Out 2025", line=dict(dash='dot', color='#FF9900')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26, name="Proyección 2026", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Detalle Mensual")
        df_m = pd.DataFrame({"Mes": meses_labels, "Sell In 2025": v_in_25.values, "Sell Out 2025": v_out_25.values, "Proy 2026": v_proy_26.values}).set_index("Mes")
        df_m.loc['TOTAL'] = df_m.sum()
        st.dataframe(df_m.T, use_container_width=True)

        st.subheader("🧪 Proyección por Disciplina")
        if not df_vista.empty:
            disc_proy = (df_vista.groupby(['DISCIPLINA', 'MES_NUM'])['CANTIDAD'].sum().unstack(fill_value=0) * factor_escala).round(0)
            disc_proy.columns = [meses_labels[i-1] for i in disc_proy.columns]
            disc_proy['TOTAL'] = disc_proy.sum(axis=1)
            st.dataframe(disc_proy, use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        vta_sku_25 = df_canal.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'V25'})
        tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['V_PROY_26'] = (tactical['V25'] * factor_escala).round(0)
        tactical['V_MENSUAL'] = (tactical['V_PROY_26'] / 12).round(0)
        tactical['MOS'] = (tactical['STK'] / tactical['V_MENSUAL']).replace([float('inf'), float('-inf')], 0).fillna(0).round(1)
        if query: tactical = tactical[tactical['SKU'].str.contains(query) | tactical['DESCRIPCION'].str.contains(query, na=False)]
        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STK', 'V25', 'V_MENSUAL', 'MOS']].sort_values('V_MENSUAL', ascending=False), use_container_width=True)
else:
    st.info("Cargando datos...")
