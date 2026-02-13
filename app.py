import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control Forecast", layout="wide")

# --- CARGA DE DATOS ---
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
        st.error(f"Error: {e}")
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

    # --- SIDEBAR: FILTROS UNIFICADOS ---
    st.sidebar.title("🎮 PARÁMETROS")
    search_query = st.sidebar.text_input("🔍 Buscar SKU o Descripción", "").upper()
    target_vol = st.sidebar.slider("Volumen Total Objetivo 2026", 500000, 1500000, 1000000, step=50000)
    
    st.sidebar.markdown("---")
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique()))) if 'EMPRENDIMIENTO' in sell_in.columns else []
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    
    f_cli = st.sidebar.multiselect("Clientes", sorted(sell_in['CLIENTE_NAME'].unique()) if 'CLIENTE_NAME' in sell_in.columns else [])
    
    if 'FRANJA_PRECIO' in maestro.columns:
        opciones_franja = sorted([str(x) for x in maestro['FRANJA_PRECIO'].dropna().unique()])
    else:
        opciones_franja = []
    f_franja = st.sidebar.multiselect("Franja de Precio", opciones_franja)

    # --- LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_emp: so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]

    # --- TABS (DEFINICIÓN DE LAS 3 SOLAPAS) ---
    tab1, tab2, tab3 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)", "🔮 ESCENARIOS"])

    meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}

    # =========================================================
    # SOLAPA 1: PERFORMANCE & PROYECCIÓN (MANTENIDA TAL CUAL)
    # =========================================================
    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index() if 'UNIDADES' in si_filt.columns else pd.DataFrame(columns=['MES_STR', 'UNIDADES'])
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index() if 'CANTIDAD' in so_filt.columns else pd.DataFrame(columns=['MES_STR', 'CANTIDAD'])
        
        total_so_25 = so_25['CANTIDAD'].sum() if not so_25.empty else 0
        if total_so_25 > 0:
            so_25['PROY_2026'] = ((so_25['CANTIDAD'] / total_so_25) * target_vol).round(0)
        else:
            so_25['PROY_2026'] = 0

        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        st.plotly_chart(fig, use_container_width=True, key="grafico_tab1")

        st.markdown("### 📋 Detalle de Valores Mensuales")
        df_t1 = df_plot[['MES_NOM', 'UNIDADES', 'CANTIDAD', 'PROY_2026']].copy()
        df_t1.columns = ['Mes', 'Sell In 2025', 'Sell Out 2025', 'Proyección 2026']
        df_t1 = df_t1.set_index('Mes').T
        df_t1['TOTAL'] = df_t1.sum(axis=1)
        st.dataframe(df_t1.style.format("{:,.0f}"), use_container_width=True)

        st.markdown("### 🧪 Proyección 2026 por Disciplina")
        if not so_filt.empty and 'DISCIPLINA' in m_filt.columns:
            so_disc = so_filt[so_filt['AÑO'] == 2025].merge(m_filt[['SKU', 'DISCIPLINA']], on='SKU')
            total_ref = so_disc['CANTIDAD'].sum()
            if total_ref > 0:
                disc_pivot = so_disc.groupby(['DISCIPLINA', 'MES_STR'])['CANTIDAD'].sum().reset_index()
                disc_pivot['PROY_2026'] = ((disc_pivot['CANTIDAD'] / total_ref) * target_vol).round(0)
                tabla_disc = disc_pivot.pivot(index='DISCIPLINA', columns='MES_STR', values='PROY_2026').fillna(0)
                tabla_disc.columns = [meses_nombres.get(col, col) for col in tabla_disc.columns]
                tabla_disc['TOTAL'] = tabla_disc.sum(axis=1)
                st.dataframe(tabla_disc.sort_values('TOTAL', ascending=False).style.format("{:,.0f}"), use_container_width=True)

    # =========================================================
    # SOLAPA 2: TACTICAL (UNIFICADA POR SKU + INGRESOS + FIX INF)
    # =========================================================
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        # 1. Agrupación por SKU para evitar duplicados
        stk_sku = stock.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STK_ACTUAL'})
        ing_sku = ingresos.groupby('SKU')['UNIDADES'].sum().reset_index().rename(columns={'UNIDADES': 'INGRESOS_FUTUROS'})
        
        # 2. Venta proyectada por SKU (basada en el share 2025 del SKU)
        vta_sku_25 = so_filt[so_filt['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].sum().reset_index()
        total_vta_25 = vta_sku_25['CANTIDAD'].sum()
        
        if total_vta_25 > 0:
            vta_sku_25['VTA_PROY_MENSUAL'] = (vta_sku_25['CANTIDAD'] / total_vta_25 * target_vol / 12).round(0)
        else:
            vta_sku_25['VTA_PROY_MENSUAL'] = 0

        # 3. Consolidación (Unificar a una fila por SKU usando el Maestro filtrado)
        tactical = m_filt.drop_duplicates('SKU')[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']] \
                         .merge(stk_sku, on='SKU', how='left') \
                         .merge(ing_sku, on='SKU', how='left') \
                         .merge(vta_sku_25[['SKU', 'VTA_PROY_MENSUAL']], on='SKU', how='left').fillna(0)
        
        # 4. Cálculo de MOS (Fix -inf y +inf)
        # Si no hay venta, el MOS es 99 (indicando que el stock no se mueve)
        tactical['MOS'] = (tactical['STK_ACTUAL'] / tactical['VTA_PROY_MENSUAL']).replace([float('inf'), -float('inf')], 99).fillna(0).round(1)
        
        def color_mos(val):
            if val < 2.5: return 'background-color: #ffcccc' # Rojo: Quiebre
            if val > 8: return 'background-color: #ffffcc'  # Amarillo: Exceso
            return 'background-color: #ccffcc'              # Verde: Saludable

        st.dataframe(
            tactical[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'STK_ACTUAL', 'INGRESOS_FUTUROS', 'VTA_PROY_MENSUAL', 'MOS']]
            .sort_values('VTA_PROY_MENSUAL', ascending=False)
            .style.applymap(color_mos, subset=['MOS'])
            .format({'STK_ACTUAL': '{:,.0f}', 'INGRESOS_FUTUROS': '{:,.0f}', 'VTA_PROY_MENSUAL': '{:,.0f}'}),
            use_container_width=True
        )

    # =========================================================
    # SOLAPA 3: ESCENARIOS (LÍNEA DE TIEMPO)
    # =========================================================
    with tab3:
        st.subheader("🔮 Línea de Tiempo Dinámica de Oportunidad")
        if not tactical.empty:
            sku_sel = st.selectbox("Seleccionar SKU para análisis 360", tactical['SKU'].unique(), key="sel_sku_tab3")
            
            # Datos del SKU seleccionado
            sku_data = tactical[tactical['SKU'] == sku_sel].iloc[0]
            stk_ini = sku_data['STK_ACTUAL']
            vta_m = sku_data['VTA_PROY_MENSUAL']
            
            # Simulación de agotamiento mes a mes
            stk_evol = []
            curr = stk_ini
            for _ in range(12):
                curr = max(0, curr - vta_m)
                stk_evol.append(curr)
                
            fig_evol = go.Figure()
            fig_evol.add_trace(go.Scatter(x=list(meses_nombres.values()), y=stk_evol, fill='tozeroy', name="Stock Proyectado", line=dict(color='#2ecc71')))
            fig_evol.add_hline(y=vta_m * 2.5, line_dash="dash", line_color="red", annotation_text="Punto Crítico (2.5 MOS)")
            
            fig_evol.update_layout(title=f"Agotamiento Proyectado 2026: {sku_sel}", hovermode="x unified")
            st.plotly_chart(fig_evol, use_container_width=True)

else:
    st.info("Cargando datos desde Google Drive...")
