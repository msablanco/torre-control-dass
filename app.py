import streamlit as st
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
            # Normalización de columnas [cite: 4]
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CANT': 'CANTIDAD', 'QTY': 'CANTIDAD', 'UNIDADES': 'CANTIDAD'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            if "SELL_IN_VENTAS" in name.upper(): [cite: 5]
                if 'EMPRENDIMIENTO' not in df.columns: df['EMPRENDIMIENTO'] = 'WHOLESALE'
                if 'CANTIDAD' not in df.columns and len(df.columns) >= 7: [cite: 6]
                    df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
            if 'CANTIDAD' in df.columns:
                df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce').fillna(0)
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

# --- LÓGICA DE PROCESAMIENTO ---
data = load_drive_data()

if data:
    sell_out = data.get('Sell_Out', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', data.get('Sell_In', pd.DataFrame())) [cite: 7]
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    # Sidebar
    st.sidebar.header("🎯 CONTROL DE VOLUMEN")
    vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)
    validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)
    st.sidebar.markdown("---")
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Seleccionar Canal", opciones_emp)
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # Función Segura para Fechas (Evita el error 'arg must be a list') [cite: 8]
    def safe_date_process(df, keywords):
        if df.empty: return df
        col = next((c for c in df.columns if any(k in c for k in keywords)), None)
        if col:
            df['FECHA_DT'] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            df['MES_NUM'] = df['FECHA_DT'].dt.month
            df['AÑO'] = df['FECHA_DT'].dt.year
        else:
            df['FECHA_DT'], df['MES_NUM'], df['AÑO'] = pd.NaT, 0, 0
        return df

    sell_out = safe_date_process(sell_out, ['FECHA', 'MES', 'DATE'])
    sell_in = safe_date_process(sell_in, ['F_REF', 'FECHA', 'DATE'])

    # --- FILTROS ---
    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    so_2025 = so_2025.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')
    df_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)] if f_emp else so_2025.copy()
    
    df_vista = df_canal.copy()
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query, na=False)] [cite: 9]

    # --- ESCALA Y SERIES ---
    base_escala = df_canal['CANTIDAD'].sum() if validar_fijar else df_vista['CANTIDAD'].sum()
    factor_escala = vol_obj / base_escala if base_escala > 0 else 1

    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    v_out_25 = df_vista.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0) [cite: 10]
    v_proy_26 = (v_out_25 * factor_escala).round(0)

    # Sell In Procesamiento
    si_25_filt = sell_in[sell_in['AÑO'] == 2025].copy()
    if f_emp and 'EMPRENDIMIENTO' in si_25_filt.columns: 
        si_25_filt = si_25_filt[si_25_filt['EMPRENDIMIENTO'].isin(f_emp)] [cite: 11]
    si_25_filt = si_25_filt.merge(maestro[['SKU', 'DESCRIPCION']], on='SKU', how='left')
    if query:
        si_25_filt = si_25_filt[si_25_filt['SKU'].str.contains(query) | si_25_filt['DESCRIPCION'].str.contains(query, na=False)] [cite: 12, 13]
    v_in_25 = si_25_filt.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # --- INTERFAZ ---
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
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        st.write("### 📋 Detalle Mensual")
        df_m = pd.DataFrame({"Sell In 2025": v_in_25.values, "Sell Out 2025": v_out_25.values, "Proy 2026": v_proy_26.values}, index=meses_labels).T [cite: 15]
        st.dataframe(df_m.style.format(fmt_p), use_container_width=True)

        if not df_vista.empty:
            st.write("### 🧪 Proyección por Disciplina")
            disc_proy = (df_vista.groupby(['DISCIPLINA', 'MES_NUM'])['CANTIDAD'].sum().unstack(fill_value=0) * factor_escala).round(0) [cite: 16]
            disc_proy.columns = [meses_labels[int(i)-1] for i in disc_proy.columns]
            st.dataframe(disc_proy.style.format(fmt_p), use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        vta_sku_25 = df_canal.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'V25'})
        tactical = maestro.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['V_PROY_26'] = (tactical['V25'] * factor_escala).round(0)
        tactical['V_MENSUAL'] = (tactical['V_PROY_26'] / 12).round(0)
        tactical['MOS'] = (tactical['STK'] / (tactical['V_MENSUAL'].replace(0, 1))).round(1) [cite: 17, 18]
        st.dataframe(tactical[['SKU', 'DESCRIPCION', 'STK', 'V25', 'V_MENSUAL', 'MOS']].sort_values('V_MENSUAL', ascending=False).style.format({
            'STK': fmt_p, 'V25': fmt_p, 'V_MENSUAL': fmt_p, 'MOS': "{:.1f}"
        }), use_container_width=True)
else:
    st.info("Esperando conexión con Drive...")
