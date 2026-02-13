import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control Forecast", layout="wide")

# --- 1. CARGA DE DATOS ---
@st.cache_data(ttl=600)
def load_drive_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        archivos_permitidos = ['Maestro_Productos', 'Sell_In_Ventas', 'Sell_Out', 'Stock', 'Ingresos']
        dfs = {}
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            if name in archivos_permitidos:
                request = service.files().get_media(fileId=f['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done: _, done = downloader.next_chunk()
                fh.seek(0)
                df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
                df.columns = [str(c).strip().upper() for c in df.columns]
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'CLIENTE': 'CLIENTE_NAME'})
                if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
                dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return {}

data = load_drive_data()

if data:
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # Procesamiento de Fechas
    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_STR'] = df['FECHA_DT'].dt.strftime('%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- 2. SIDEBAR: PARÁMETROS ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    st.sidebar.markdown("---")
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_in.columns else []) | 
                          set(sell_out['EMPRENDIMIENTO'].dropna().unique()) if 'EMPRENDIMIENTO' in sell_out.columns else [])
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- 3. LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_emp: so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]

    # --- 4. MOTOR DE CÁLCULO (UNIFICADO) ---
    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}
    
    vta_tot_25 = so_filt[so_filt['AÑO'] == 2025]['CANTIDAD'].sum()
    factor_escala = target_vol / vta_tot_25 if vta_tot_25 > 0 else 1
    
    vta_sku_25 = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index()
    stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})
    
    if not ingresos.empty:
        ing_futuros = ingresos.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'ING_FUTUROS'})
    else:
        ing_futuros = pd.DataFrame(columns=['SKU', 'ING_FUTUROS'])

    tactical = m_filt.merge(stk_sku, on='SKU', how='left').merge(vta_sku_25, on='SKU', how='left').merge(ing_futuros, on='SKU', how='left').fillna(0)
    tactical['VTA_PROY_MENSUAL'] = ((tactical['CANTIDAD'] * factor_escala) / 12).round(0)
    
    def calcular_mos_safe(row):
        if row['VTA_PROY_MENSUAL'] <= 0: return 0.0
        return round(row['STK_ACTUAL'] / row['VTA_PROY_MENSUAL'], 1)
    
    tactical['MOS'] = tactical.apply(calcular_mos_safe, axis=1)

    def clasificar_salud(row):
        if row['VTA_PROY_MENSUAL'] == 0: return "⚪ SIN VENTA"
        if row['MOS'] < 2.5: return "🔥 QUIEBRE"
        if row['MOS'] > 8: return "⚠️ SOBRE-STOCK"
        return "✅ SALUDABLE"
    
    tactical['ESTADO'] = tactical.apply(clasificar_salud, axis=1)

    # --- 5. RENDERIZADO DE TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS SKU"])

    with tab1:
        st.subheader("Curva de Demanda y Forecast 2026")
        si_25_g = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25_g = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        
        if vta_tot_25 > 0:
            so_25_g['PROY_2026'] = ((so_25_g['CANTIDAD'] / vta_tot_25) * target_vol).round(0)
        else:
            so_25_g['PROY_2026'] = 0

        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25_g, on='MES_STR', how='left').merge(so_25_g, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        fig_perf = go.Figure()
        fig_perf.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        fig_perf.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig_perf.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        
        st.plotly_chart(fig_perf, use_container_width=True, key="grafico_performance_principal")

        st.markdown("### 📋 Detalle Mensual")
        df_t1 = df_plot[['MES_NOM', 'UNIDADES', 'CANTIDAD', 'PROY_2026']].copy()
        df_t1.columns = ['Mes', 'Sell In 2025', 'Sell Out 2025', 'Proyección 2026']
        df_t1 = df_t1.set_index('Mes').T
        df_t1['TOTAL'] = df_t1.sum(axis=1)
        st.dataframe(df_t1.style.format("{:,.0f}"), use_container_width=True)

    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs en Riesgo de Quiebre", len(tactical[tactical['ESTADO'] == "🔥 QUIEBRE"]))
        c2.metric("SKUs con Sobre-Stock", len(tactical[tactical['ESTADO'] == "⚠️ SOBRE-STOCK"]))
        mos_m = tactical[tactical['VTA_PROY_MENSUAL'] > 0]['MOS'].mean()
        c3.metric("MOS Promedio", f"{mos_m:.1f} meses")

        cols_f = ['SKU', 'DESCRIPCION', 'DISCIPLINA', 'STK_ACTUAL', 'ING_FUTUROS', 'VTA_PROY_MENSUAL', 'MOS', 'ESTADO']
        st.dataframe(tactical[cols_f].sort_values('VTA_PROY_MENSUAL', ascending=False).set_index('SKU'), use_container_width=True)

    with tab3:
        st.subheader("🔮 Línea de Tiempo de Oportunidad")
        sku_list = tactical.sort_values('VTA_PROY_MENSUAL', ascending=False)['SKU'].unique()
        sku_sel = st.selectbox("Seleccionar SKU para análisis", sku_list, key="selector_sku_tab3")
        
        if sku_sel:
            dat = tactical[tactical['SKU'] == sku_sel].iloc[0]
            ing_m = ingresos[ingresos['SKU'] == sku_sel].groupby('MES_STR')['UNIDADES'].sum()
            
            mes_eje = [meses_nombres[str(i).zfill(2)] for i in range(1, 13)]
            stk_ev = []
            curr = dat['STK_ACTUAL']
            
            for i in range(1, 13):
                m_code = str(i).zfill(2)
                arribo = ing_m.get(m_code, 0)
                curr = (curr + arribo) - dat['VTA_PROY_MENSUAL']
                stk_ev.append(max(0, curr))
            
            fig_stk = go.Figure()
            fig_stk.add_trace(go.Scatter(x=mes_eje, y=stk_ev, name="Stock", line=dict(color='#e74c3c', width=4), fill='tozeroy', fillcolor='rgba(231, 76, 60, 0.1)'))
            fig_stk.add_trace(go.Bar(x=mes_eje, y=[ing_m.get(str(i).zfill(2), 0) for i in range(1, 13)], name="Ingresos", marker_color='#2ecc71', opacity=0.7))
            fig_stk.add_hline(y=dat['VTA_PROY_MENSUAL']*2, line_dash="dash", line_color="gray", annotation_text="Seguridad")
            
            fig_stk.update_layout(title=f"Evolución Proyectada: {sku_sel}", hovermode="x unified")
            st.plotly_chart(fig_stk, use_container_width=True, key="grafico_agotamiento_sku_final")
            
            if min(stk_ev) == 0:
                st.error(f"⚠️ El SKU {sku_sel} entrará en quiebre total.")
            else:
                st.success(f"✅ Abastecimiento cubierto para {sku_sel}.")
else:
    st.warning("No se detectaron archivos en la carpeta de Drive o hubo un error en la conexión.")
