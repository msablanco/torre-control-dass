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
                # Normalización de nombres de columnas comunes
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
    # El filtro de emprendimiento ahora afecta a ambos archivos
    opciones_emp = sorted(list(set(sell_in['EMPRENDIMIENTO'].dropna().unique()) | set(sell_out['EMPRENDIMIENTO'].dropna().unique())))
    f_emp = st.sidebar.multiselect("Emprendimiento (Canal)", opciones_emp)
    
    f_cli = st.sidebar.multiselect("Clientes", sell_in['CLIENTE_NAME'].unique() if 'CLIENTE_NAME' in sell_in.columns else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    # --- LÓGICA DE FILTRADO ---
    m_filt = maestro.copy()
    if search_query: m_filt = m_filt[m_filt['SKU'].str.contains(search_query) | m_filt['DESCRIPCION'].str.contains(search_query)]
    if f_franja: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_franja)]

    # Aplicamos filtro de emprendimiento y cliente a Sell In
    si_filt = sell_in[sell_in['SKU'].isin(m_filt['SKU'])]
    if f_emp: si_filt = si_filt[si_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: si_filt = si_filt[si_filt['CLIENTE_NAME'].isin(f_cli)]

    # Aplicamos filtro de emprendimiento y cliente a Sell Out
    so_filt = sell_out[sell_out['SKU'].isin(m_filt['SKU'])]
    if f_emp: so_filt = so_filt[so_filt['EMPRENDIMIENTO'].isin(f_emp)]
    if f_cli: so_filt = so_filt[so_filt['CLIENTE_NAME'].isin(f_cli)]

    # --- TABS ---
    tab1, tab2 = st.tabs(["📊 PERFORMANCE & PROYECCIÓN", "⚡ TACTICAL (MOS)"])

    with tab1:
        st.subheader("Análisis de Demanda y Proyección Unificada")
        meses_nombres = {'01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun','07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'}
        
        # Agrupaciones
        si_25 = si_filt[si_filt['AÑO'] == 2025].groupby('MES_STR')['UNIDADES'].sum().reset_index()
        so_25 = so_filt[so_filt['AÑO'] == 2025].groupby('MES_STR')['CANTIDAD'].sum().reset_index()
        
        # Proyección basada en el Volumen Objetivo (Target Vol)
        total_so_25 = so_25['CANTIDAD'].sum()
        if total_so_25 > 0:
            so_25['PROY_2026'] = ((so_25['CANTIDAD'] / total_so_25) * target_vol).round(0)
        else:
            so_25['PROY_2026'] = 0

        # Preparación de tabla de visualización
        base_meses = pd.DataFrame({'MES_STR': [str(i).zfill(2) for i in range(1, 13)]})
        df_plot = base_meses.merge(si_25, on='MES_STR', how='left').merge(so_25, on='MES_STR', how='left').fillna(0)
        df_plot['MES_NOM'] = df_plot['MES_STR'].map(meses_nombres)

        # Gráfico principal
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['UNIDADES'], name="Sell In 2025", line=dict(color='#1f77b4', width=2)))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['CANTIDAD'], name="Sell Out 2025", line=dict(color='#ff7f0e', dash='dot')))
        fig.add_trace(go.Scatter(x=df_plot['MES_NOM'], y=df_plot['PROY_2026'], name="Proyección 2026", line=dict(color='#2ecc71', width=4)))
        st.plotly_chart(fig, use_container_width=True)

        # TABLA 1: DATOS MENSUALES CON TOTAL
        st.markdown("### 📋 Detalle de Valores Mensuales")
        df_t1 = df_plot[['MES_NOM', 'UNIDADES', 'CANTIDAD', 'PROY_2026']].copy()
        df_t1.columns = ['Mes', 'Sell In 2025', 'Sell Out 2025', 'Proyección 2026']
        df_t1 = df_t1.set_index('Mes').T
        df_t1['TOTAL'] = df_t1.sum(axis=1)
        st.dataframe(df_t1.style.format("{:,.0f}"), use_container_width=True)

        # TABLA 2: DISCIPLINA CON TOTAL
        st.markdown("### 🧪 Proyección 2026 por Disciplina")
        if not so_filt.empty:
            so_disc = so_filt[so_filt['AÑO'] == 2025].merge(m_filt[['SKU', 'DISCIPLINA']], on='SKU')
            total_ref = so_disc['CANTIDAD'].sum()
            disc_pivot = so_disc.groupby(['DISCIPLINA', 'MES_STR'])['CANTIDAD'].sum().reset_index()
            if total_ref > 0:
                disc_pivot['PROY_2026'] = ((disc_pivot['CANTIDAD'] / total_ref) * target_vol).round(0)
                tabla_disc = disc_pivot.pivot(index='DISCIPLINA', columns='MES_STR', values='PROY_2026').fillna(0)
                tabla_disc.columns = [meses_nombres.get(col, col) for col in tabla_disc.columns]
                tabla_disc['TOTAL'] = tabla_disc.sum(axis=1)
                st.dataframe(tabla_disc.sort_values('TOTAL', ascending=False).style.format("{:,.0f}"), use_container_width=True)

# =========================================================
    # SOLAPA 2: TACTICAL (MOS & MIX 360)
    # =========================================================
    with tab2:
        st.subheader("⚡ Matriz de Salud de Inventario (MOS)")
        
        if not stock.empty:
            # 1. Preparar el Stock Actual por SKU
            col_stk = next((c for c in ['CANTIDAD', 'PARES', 'STOCK'] if c in stock.columns), None)
            stk_res = stock.groupby('SKU')[col_stk].sum().reset_index().rename(columns={col_stk: 'STOCK_ACTUAL'})
            
            # 2. Obtener la Proyección Mensual Promedio (de lo que calculamos en Tab 1)
            # Calculamos la venta mensual promedio proyectada para 2026
            vta_prom_proy = df_plot['PROY_2026'].mean()
            
            # 3. Cruzar Maestro con Stock
            tactical_df = m_filt[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(stk_res, on='SKU', how='left').fillna(0)
            
            # 4. Calcular MOS (Months on Hand) 
            # Si no hay venta proyectada para ese SKU, usamos el promedio del grupo
            tactical_df['VTA_PROY_MES'] = (target_vol / 12) * (1 / len(m_filt) if len(m_filt)>0 else 0) # Estimado simple por SKU
            tactical_df['MOS'] = (tactical_df['STOCK_ACTUAL'] / tactical_df['VTA_PROY_MES']).round(1)
            
            # 5. Semáforo de Salud
            def color_mos(val):
                if val < 1: return 'background-color: #ffcccc' # Quiebre inminente
                if 1 <= val <= 3: return 'background-color: #ccffcc' # Saludable
                return 'background-color: #ffffcc' # Exceso
            
            st.dataframe(
                tactical_df.style.applymap(color_mos, subset=['MOS']).format({'STOCK_ACTUAL': '{:,.0f}', 'VTA_PROY_MES': '{:,.0f}'}),
                use_container_width=True
            )
        else:
            st.warning("⚠️ No se encontró el archivo de Stock para calcular el MOS.")

    # =========================================================
    # SOLAPA 3: ESCENARIOS (LÍNEA DE TIEMPO DE OPORTUNIDAD)
    # =========================================================
    with tab3:
        st.subheader("🔮 Línea de Tiempo Dinámica de Oportunidad")
        
        if not m_filt.empty:
            col_sku = st.selectbox("Seleccionar SKU para análisis 360", m_filt['SKU'].unique())
            
            # Simulación de agotamiento de stock
            stk_inicial = tactical_df[tactical_df['SKU'] == col_sku]['STOCK_ACTUAL'].values[0] if col_sku in tactical_df['SKU'].values else 0
            
            # Curva de demanda (usamos la estacionalidad de la Tab 1)
            curva_demanda = df_plot['PROY_2026'].values
            meses = df_plot['MES_NOM'].values
            
            stk_evolutivo = []
            current_stk = stk_inicial
            for vta in curva_demanda:
                current_stk = max(0, current_stk - vta)
                stk_evolutivo.append(current_stk)
            
            # Gráfico de Oportunidad
            fig_opp = go.Figure()
            # Área de Stock
            fig_opp.add_trace(go.Scatter(x=meses, y=stk_evolutivo, fill='tozeroy', name='Stock Proyectado', line=dict(color='#2ecc71')))
            # Línea de Demanda
            fig_opp.add_trace(go.Scatter(x=meses, y=curva_demanda, name='Demanda Mensual', line=dict(color='#e74c3c', dash='dot')))
            
            fig_opp.update_layout(
                title=f"¿Cuándo se agota el {col_sku}?",
                xaxis_title="Meses 2026",
                yaxis_title="Pares",
                hovermode="x unified"
            )
            st.plotly_chart(fig_opp, use_container_width=True)
            
            # Cálculo de Fecha de Quiebre
            mes_quiebre = "Sin quiebre"
            for i, s in enumerate(stk_evolutivo):
                if s <= 0:
                    mes_quiebre = meses[i]
                    break
            
            st.metric("Fecha estimada de quiebre", mes_quiebre)
            st.write(f"Para cubrir la demanda de 2026, necesitas comprar: **{max(0, curva_demanda.sum() - stk_inicial):,.0f} pares adicionales.**")
