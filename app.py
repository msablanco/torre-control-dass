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
    if pd.isna(valor) or valor == 0: return "0"
    return f"{valor:,.0f}".replace(",", ".")

# --- 2. SIDEBAR ---
st.sidebar.header("🎯 CONTROL DE VOLUMEN")
vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)
mos_objetivo = st.sidebar.slider("MOS Objetivo (Meses)", 1, 8, 3)
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=False)

st.sidebar.markdown("---")
st.sidebar.subheader("🔍 FILTROS")

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
            
            # Normalización
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CANT': 'CANTIDAD', 'QTY': 'CANTIDAD', 'UNIDADES': 'CANTIDAD'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            # PARCHE SELL IN
            if "SELL_IN_VENTAS" in name.upper():
                if 'EMPRENDIMIENTO' not in df.columns: df['EMPRENDIMIENTO'] = 'WHOLESALE'
                if len(df.columns) >= 2: df = df.rename(columns={df.columns[1]: 'FECHA_REF'})
                if len(df.columns) >= 7: df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
            
            # ELIMINAR COLUMNAS DUPLICADAS SI EXISTEN
            df = df.loc[:, ~df.columns.duplicated()]
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_drive_data()

if data:
    sell_out = data.get('Sell_Out', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', data.get('Sell_In', pd.DataFrame()))
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Seleccionar Emprendimiento (Canal)", opciones_emp)
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # --- 3. PROCESAMIENTO SELL OUT ---
    if not sell_out.empty:
        col_f = next((c for c in sell_out.columns if any(x in c for x in ['FECHA', 'MES', 'DATE'])), None)
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f], dayfirst=True, errors='coerce')
        sell_out['MES_NUM'] = sell_out['FECHA_DT'].dt.month
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    so_2025 = sell_out[sell_out['AÑO'] == 2025].copy()
    
    # Aseguramos que el maestro no traiga columnas que ya existen en so_2025 excepto SKU
    cols_to_use = ['SKU'] + [c for c in ['DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO'] if c in maestro.columns]
    so_2025 = so_2025.merge(maestro[cols_to_use], on='SKU', how='left')

    df_canal = so_2025[so_2025['EMPRENDIMIENTO'].isin(f_emp)] if f_emp else so_2025.copy()
    df_vista = df_canal.copy()
    if query:
        df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query, na=False)]

    base_escala = df_canal['CANTIDAD'].sum() if validar_fijar else df_vista['CANTIDAD'].sum()
    factor_escala = vol_obj / base_escala if base_escala > 0 else 1

    # --- 4. SERIES TIEMPO ---
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    v_out_25 = df_vista.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_26 = (v_out_25 * factor_escala).round(0)

    v_in_25 = pd.Series(0, index=meses_idx)
    if not sell_in.empty:
        col_f_in = next((c for c in sell_in.columns if any(x in c for x in ['FECHA', 'MES', 'DATE', 'REF'])), None)
        if col_f_in:
            si_temp = sell_in.copy()
            si_temp['FECHA_DT'] = pd.to_datetime(si_temp[col_f_in], dayfirst=True, errors='coerce')
            si_25 = si_temp[si_temp['FECHA_DT'].dt.year == 2025].copy()
            si_25['MES_NUM'] = si_25['FECHA_DT'].dt.month
            
            if f_emp: si_25 = si_25[si_25['EMPRENDIMIENTO'].isin(f_emp)]
            if query:
                # Evitar duplicados en el merge temporal para filtro
                si_25 = si_25.merge(maestro[['SKU','DESCRIPCION']], on='SKU', how='left')
                si_25 = si_25[si_25['SKU'].str.contains(query) | si_25['DESCRIPCION'].str.contains(query, na=False)]
            
            v_in_25 = si_25.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # --- 5. INTERFAZ ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE", "🎯 ESTRATEGIA DE COMPRA"])

    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyección en Vista", fmt_p(v_proy_26.sum()))
        c2.metric("Objetivo", fmt_p(vol_obj))
        c3.metric("Escala", f"{factor_escala:.4f}")

        fig = go.Figure()
        # Convertimos a lista simple para evitar problemas de índices duplicados en el objeto Series
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25.tolist(), name="Sell In 2025", line=dict(color='#3366CC', width=3)))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25.tolist(), name="Sell Out 2025", line=dict(dash='dot', color='#FF9900')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26.tolist(), name="Proyección 2026", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        df_m = pd.DataFrame({"Sell In": v_in_25.values, "Sell Out": v_out_25.values, "Proy 2026": v_proy_26.values}, index=meses_labels)
        st.dataframe(df_m.T.style.format(fmt_p), use_container_width=True)

    with tab2:
        st.subheader("🏢 Resumen por Segmento (Disciplina y Franja)")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK'})
        vta_sku_25 = df_canal.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'V25'})
        
        tactical = maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').fillna(0)
        tactical['V_PROY_26'] = (tactical['V25'] * factor_escala).round(0)
        tactical['V_MENSUAL'] = (tactical['V_PROY_26'] / 12)
        tactical['MOS'] = (tactical['STK'] / (tactical['V_MENSUAL'].replace(0, 1))).round(1)
        tactical['SUGERIDO'] = ((tactical['V_MENSUAL'] * mos_objetivo) - tactical['STK']).clip(lower=0).round(0)

        resumen_estrategico = tactical.groupby(['DISCIPLINA', 'FRANJA_PRECIO']).agg({
            'V25': 'sum', 'STK': 'sum', 'V_PROY_26': 'sum', 'SUGERIDO': 'sum'
        }).reset_index()
        resumen_estrategico['MOS_PROMEDIO'] = (resumen_estrategico['STK'] / (resumen_estrategico['V_PROY_26'] / 12).replace(0,1)).round(1)

        st.dataframe(resumen_estrategico.sort_values(['DISCIPLINA', 'SUGERIDO'], ascending=[True, False]).style.format({
            'V25': fmt_p, 'STK': fmt_p, 'V_PROY_26': fmt_p, 'SUGERIDO': fmt_p, 'MOS_PROMEDIO': '{:.1f}'
        }), use_container_width=True)

        st.markdown("---")
        st.subheader("📝 Detalle por SKU")
        if query:
            tactical = tactical[tactical['SKU'].str.contains(query) | tactical['DESCRIPCION'].str.contains(query, na=False)]
        st.dataframe(tactical.sort_values('V_PROY_26', ascending=False).style.format({
            'STK': fmt_p, 'V25': fmt_p, 'V_PROY_26': fmt_p, 'SUGERIDO': fmt_p, 'MOS': '{:.1f}'
        }), use_container_width=True)
else:
    st.info("Cargando datos...")
