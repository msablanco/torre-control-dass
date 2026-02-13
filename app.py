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

# --- 2. SIDEBAR ---
st.sidebar.header("🎯 CONTROL DE VOLUMEN")
vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)
validar_fijar = st.sidebar.checkbox("✅ VALIDAR Y FIJAR ESCALA", value=True)

st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ PARÁMETROS DE COMPRA")
mos_objetivo = st.sidebar.slider("MOS Objetivo (Meses)", 1, 6, 3)

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
            
            df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CANT': 'CANTIDAD', 'QTY': 'CANTIDAD', 'UNIDADES': 'CANTIDAD'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            if "SELL_IN_VENTAS" in name.upper():
                df['EMPRENDIMIENTO'] = 'WHOLESALE'
                if 'CANTIDAD' not in df.columns and len(df.columns) >= 7:
                    df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
                if 'FECHA' not in df.columns and len(df.columns) >= 2:
                    df = df.rename(columns={df.columns[1]: 'FECHA'})
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_drive_data()

if data:
    sell_out = data.get('Sell_Out', pd.DataFrame())
    sell_in_ws = data.get('Sell_In_Ventas', pd.DataFrame())
    maestro = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stock = data.get('Stock', pd.DataFrame())

    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Canal", opciones_emp, default=["WHOLESALE"] if "WHOLESALE" in opciones_emp else [])
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # --- 3. PROCESAMIENTO SELL OUT ---
    col_f_so = next((c for c in sell_out.columns if 'FECHA' in c or 'MES' in c), None)
    if col_f_so:
        sell_out['FECHA_DT'] = pd.to_datetime(sell_out[col_f_so], dayfirst=True, errors='coerce')
        sell_out['MES_NUM'] = sell_out['FECHA_DT'].dt.month
        sell_out['AÑO'] = sell_out['FECHA_DT'].dt.year

    so_25 = sell_out[sell_out['AÑO'] == 2025].copy()
    so_25 = so_25.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')
    df_canal = so_25[so_25['EMPRENDIMIENTO'].isin(f_emp)] if f_emp else so_25.copy()
    
    df_vista = df_canal.copy()
    if query: df_vista = df_vista[df_vista['SKU'].str.contains(query) | df_vista['DESCRIPCION'].str.contains(query, na=False)]

    base_escala = df_canal['CANTIDAD'].sum() if validar_fijar else df_vista['CANTIDAD'].sum()
    factor_escala = vol_obj / base_escala if base_escala > 0 else 1
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    v_out_25 = df_vista.groupby('MES_NUM')['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_26 = (v_out_25 * factor_escala).round(0)

    # --- 4. SELL IN (FORZAR COLUMNA B Y FORMATO) ---
    v_in_25 = pd.Series(0, index=meses_idx)
    if not sell_in_ws.empty:
        si_temp = sell_in_ws.copy()
        if 'FECHA' in si_temp.columns:
            si_temp['FECHA_DT'] = pd.to_datetime(si_temp['FECHA'], dayfirst=True, errors='coerce')
            si_25 = si_temp[si_temp['FECHA_DT'].dt.year == 2025].copy()
            si_25 = si_25.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')
            if f_emp and 'EMPRENDIMIENTO' in si_25.columns: si_25 = si_25[si_25['EMPRENDIMIENTO'].isin(f_emp)]
            if query: si_25 = si_25[si_25['SKU'].str.contains(query) | si_25['DESCRIPCION'].str.contains(query, na=False)]
            v_in_25 = si_25.groupby(si_25['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # --- 5. TABS ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE (2025)", "🎯 PLANEAMIENTO (2026)"])

    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyección 2026", fmt_p(v_proy_26.sum()))
        c2.metric("Objetivo", fmt_p(vol_obj))
        c3.metric("Escala", f"{factor_escala:.4f}")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25, name="Sell In 2025", line=dict(color='#3366CC', width=3)))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25, name="Sell Out 2025", line=dict(color='#FF9900', dash='dot')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26, name="Proy. 2026", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        df_m = pd.DataFrame({"Mes": meses_labels, "Sell In 2025": v_in_25.values, "Sell Out 2025": v_out_25.values, "Proy 2026": v_proy_26.values}).set_index("Mes")
        df_m.loc['TOTAL'] = df_m.sum()
        st.dataframe(df_m.T.style.format(lambda x: fmt_p(x)), use_container_width=True)

    with tab2:
        st.subheader("📝 Matriz S&OP")
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK_DASS'})
        si_25_total = pd.DataFrame(columns=['SKU', 'SELL_IN_2025'])
        if not sell_in_ws.empty and 'FECHA' in sell_in_ws.columns:
            si_calc = sell_in_ws.copy()
            si_calc['FECHA_DT'] = pd.to_datetime(si_calc['FECHA'], dayfirst=True, errors='coerce')
            si_25_total = si_calc[si_calc['FECHA_DT'].dt.year == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SELL_IN_2025'})

        so_25_total = df_canal.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SELL_OUT_2025'})
        matriz = maestro[['SKU', 'DESCRIPCION']].merge(stk_sku, on='SKU', how='left')
        matriz = matriz.merge(si_25_total, on='SKU', how='left').merge(so_25_total, on='SKU', how='left').fillna(0)
        matriz['SELL_OUT_2026'] = (matriz['SELL_OUT_2025'] * factor_escala).round(0)
        matriz['SELL_IN_2026'] = 0 
        matriz['INGRESOS_FUTUROS'] = 0 
        matriz['V_MENSUAL_26'] = matriz['SELL_OUT_2026'] / 12
        matriz['MOS'] = (matriz['STOCK_DASS'] / matriz['V_MENSUAL_26'].replace(0, 1)).round(1)
        matriz['COMPRA_SUGERIDA'] = ((matriz['V_MENSUAL_26'] * mos_objetivo) - matriz['STOCK_DASS']).clip(lower=0).round(0)
        if query: matriz = matriz[matriz['SKU'].str.contains(query) | matriz['DESCRIPCION'].str.contains(query, na=False)]
        
        cols = ['SKU', 'DESCRIPCION', 'STOCK_DASS', 'SELL_IN_2025', 'SELL_OUT_2025', 'SELL_IN_2026', 'SELL_OUT_2026', 'INGRESOS_FUTUROS', 'MOS', 'COMPRA_SUGERIDA']
        st.dataframe(matriz[cols].sort_values('SELL_OUT_2026', ascending=False).style.format({
            'STOCK_DASS': '{:,.0f}', 'SELL_IN_2025': '{:,.0f}', 'SELL_OUT_2025': '{:,.0f}', 
            'SELL_IN_2026': '{:,.0f}', 'SELL_OUT_2026': '{:,.0f}', 'INGRESOS_FUTUROS': '{:,.0f}',
            'COMPRA_SUGERIDA': '{:,.0f}', 'MOS': '{:.1f}'
        }), use_container_width=True)

        # EXPORTACIÓN SEGURA
        try:
            output = io.BytesIO()
            matriz[cols].to_excel(output, index=False)
            st.download_button("📥 Descargar Excel S&OP", output.getvalue(), "Fila_SOP_2026.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except:
            st.download_button("📥 Descargar CSV (Alternativo)", matriz[cols].to_csv(index=False).encode('utf-8'), "Fila_SOP_2026.csv", "text/csv")
else:
    st.info("Cargando datos de Drive...")
