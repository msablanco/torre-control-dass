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
            
            # Normalización SKU
            for col in ['ARTICULO', 'CODIGO', 'SKU_ID']:
                if col in df.columns: df = df.rename(columns={col: 'SKU'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            # SELL IN: B es Fecha (1), G es Cantidad (6)
            if "SELL_IN_VENTAS" in name.upper():
                df['EMPRENDIMIENTO'] = 'WHOLESALE'
                if len(df.columns) >= 2: df = df.rename(columns={df.columns[1]: 'FECHA_REF'})
                if len(df.columns) >= 7: df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
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

    # --- 3. FILTROS ---
    st.sidebar.header("🎯 CONTROL SOP")
    vol_obj = st.sidebar.number_input("Volumen Objetivo 2026", value=1000000, step=50000)
    mos_objetivo = st.sidebar.slider("MOS Objetivo (Meses)", 1, 6, 3)
    
    opciones_emp = sorted(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else []
    f_emp = st.sidebar.multiselect("Canal", opciones_emp, default=["WHOLESALE"] if "WHOLESALE" in opciones_emp else [])
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # --- 4. PROCESAMIENTO ---
    # Procesar Sell Out
    sell_out['FECHA_DT'] = pd.to_datetime(sell_out.filter(like='FECHA').iloc[:,0], dayfirst=True, errors='coerce')
    so_25 = sell_out[(sell_out['FECHA_DT'].dt.year == 2025) & (sell_out['EMPRENDIMIENTO'].isin(f_emp))].copy()
    so_25 = so_25.merge(maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']], on='SKU', how='left')
    if query: so_25 = so_25[so_25['SKU'].str.contains(query) | so_25['DESCRIPCION'].str.contains(query, na=False)]
    
    factor = vol_obj / so_25['CANTIDAD'].sum() if so_25['CANTIDAD'].sum() > 0 else 1
    meses_idx = range(1, 13)
    meses_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    v_out_25 = so_25.groupby(so_25['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_26 = (v_out_25 * factor).round(0)

    # Procesar Sell In
    v_in_25 = pd.Series(0, index=meses_idx)
    if not sell_in_ws.empty:
        si_temp = sell_in_ws.copy()
        si_temp['FECHA_DT'] = pd.to_datetime(si_temp['FECHA_REF'], dayfirst=True, errors='coerce')
        si_25 = si_temp[si_temp['FECHA_DT'].dt.year == 2025].copy()
        si_25 = si_25.merge(maestro[['SKU', 'DESCRIPCION']], on='SKU', how='left')
        if query: si_25 = si_25[si_25['SKU'].str.contains(query) | si_25['DESCRIPCION'].str.contains(query, na=False)]
        v_in_25 = si_25.groupby(si_25['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # --- 5. TABS ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE 2025", "🎯 PLANEAMIENTO 2026"])

    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyección 2026", fmt_p(v_proy_26.sum()))
        c2.metric("Objetivo Global", fmt_p(vol_obj))
        c3.metric("Factor Escala", f"{factor:.4f}")
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses_labels, y=v_in_25, name="Sell In 2025", line=dict(color='#3366CC', width=3)))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_out_25, name="Sell Out 2025", line=dict(color='#FF9900', dash='dot')))
        fig.add_trace(go.Scatter(x=meses_labels, y=v_proy_26, name="Proy. 2026", line=dict(width=4, color='#00FF00')))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Detalle Mensual Histórico vs Proyectado")
        df_m = pd.DataFrame({"Mes": meses_labels, "Sell In 2025": v_in_25.values, "Sell Out 2025": v_out_25.values, "Proy 2026": v_proy_26.values}).set_index("Mes")
        df_m.loc['TOTAL'] = df_m.sum()
        st.dataframe(df_m.T.style.format(lambda x: fmt_p(x)), use_container_width=True)
        
    with tab2:
        st.subheader("📝 Matriz de Planeamiento S&OP")
        
        # 1. Consolidación de Stock (DASS)
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK_DASS'})
        
        # 2. Consolidación de Sell In 2025 (desde los datos procesados en Parte 1)
        si_25_total = si_temp[si_temp['FECHA_DT'].dt.year == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SELL_IN_2025'})
        
        # 3. Consolidación de Sell Out 2025 (desde los datos filtrados en Parte 1)
        so_25_total = so_25.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'SELL_OUT_2025'})

        # 4. Cruce de datos con el Maestro
        matriz = maestro[['SKU', 'DESCRIPCION']].merge(stk_sku, on='SKU', how='left')
        matriz = matriz.merge(si_25_total, on='SKU', how='left').merge(so_25_total, on='SKU', how='left').fillna(0)
        
        # 5. Cálculos 2026
        matriz['SELL_OUT_2026'] = (matriz['SELL_OUT_2025'] * factor).round(0)
        matriz['SELL_IN_2026'] = 0  # Placeholder para ingresos confirmados
        matriz['INGRESOS_FUTUROS'] = 0  # Placeholder para tránsitos/OC
        
        # Venta mensual proyectada para el cálculo de MOS
        matriz['V_MENSUAL_26'] = matriz['SELL_OUT_2026'] / 12
        
        # Cálculo de MOS (Meses de Cobertura)
        matriz['MOS'] = (matriz['STOCK_DASS'] / matriz['V_MENSUAL_26'].replace(0, 1)).round(1)
        
        # Lógica de Compra Sugerida: (Venta Mensual * MOS Objetivo) - Stock - Ingresos Futuros
        matriz['COMPRA_SUGERIDA'] = ((matriz['V_MENSUAL_26'] * mos_objetivo) - matriz['STOCK_DASS'] - matriz['INGRESOS_FUTUROS']).clip(lower=0).round(0)
        
        # Filtro de búsqueda en la tabla
        if query:
            matriz = matriz[matriz['SKU'].str.contains(query) | matriz['DESCRIPCION'].str.contains(query, na=False)]
        
        # Columnas finales solicitadas
        cols_finales = [
            'SKU', 'DESCRIPCION', 'STOCK_DASS', 'SELL_IN_2025', 'SELL_OUT_2025', 
            'SELL_IN_2026', 'SELL_OUT_2026', 'INGRESOS_FUTUROS', 'MOS', 'COMPRA_SUGERIDA'
        ]
        
        # Mostrar Tabla con formato de miles
        st.dataframe(
            matriz[cols_finales].sort_values('SELL_OUT_2026', ascending=False).style.format({
                'STOCK_DASS': '{:,.0f}', 'SELL_IN_2025': '{:,.0f}', 'SELL_OUT_2025': '{:,.0f}', 
                'SELL_IN_2026': '{:,.0f}', 'SELL_OUT_2026': '{:,.0f}', 'INGRESOS_FUTUROS': '{:,.0f}',
                'COMPRA_SUGERIDA': '{:,.0f}', 'MOS': '{:.1f}'
            }), 
            use_container_width=True
        )

        # 6. Botón de Exportación Segura a CSV
        st.markdown("---")
        csv_buffer = io.StringIO()
        matriz[cols_finales].to_csv(csv_buffer, index=False, sep=';', encoding='utf-8-sig')
        
        st.download_button(
            label="📥 Descargar Matriz S&OP (Excel/CSV)",
            data=csv_buffer.getvalue(),
            file_name="Fila_SOP_Planeamiento_2026.csv",
            mime="text/csv",
            help="Descarga el planeamiento completo compatible con Excel"
        )
else:
    # Mensaje de espera si los datos no cargan
    st.warning("⚠️ No se detectaron archivos válidos en la carpeta de Google Drive configurada.")
    st.info("Asegúrate de que los archivos .csv tengan los nombres: 'Sell_Out', 'Sell_In_Ventas', 'Stock' y 'Maestro_Productos'.")

