import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control v2.0", layout="wide")

# --- 1. CARGA DE DATOS (ESTRICTA) ---
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
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
                df.columns = [str(c).strip().upper().replace('ï»¿', '') for c in df.columns]
                mapeo_sku = {'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'PRODUCTO': 'SKU'}
                df = df.rename(columns=mapeo_sku)
                if 'SKU' in df.columns:
                    df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
                dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error crítico: {e}")
        return {}

data = load_drive_data()

if data:
    # --- 2. PREPARACIÓN DE DATOS ---
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    for df in [sell_in, sell_out, ingresos]:
        if not df.empty:
            col_f = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
            if col_f:
                df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
                df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')
                df['AÑO'] = df['FECHA_DT'].dt.year

    # --- 3. SIDEBAR: INTELIGENCIA COMERCIAL ---
    st.sidebar.header("🕹️ CONTROL DE ESCENARIO")
    f_emp = st.sidebar.selectbox("Emprendimiento", ["TODOS"] + list(sell_in['EMPRENDIMIENTO'].unique() if 'EMPRENDIMIENTO' in sell_in.columns else []))
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Ajuste Presupuesto 2026")
    ajuste_venta = st.sidebar.slider("Incremento Venta s/ 2025 (%)", -50, 100, 20)
    
    # Filtros de Producto
    st.sidebar.subheader("Filtros de Mix")
    f_disc = st.sidebar.multiselect("Disciplina", maestro['DISCIPLINA'].unique() if 'DISCIPLINA' in maestro.columns else [])
    f_gen = st.sidebar.multiselect("Género", maestro['GENERO'].unique() if 'GENERO' in maestro.columns else [])
    f_fran = st.sidebar.multiselect("Franja Precio", maestro['FRANJA_PRECIO'].unique() if 'FRANJA_PRECIO' in maestro.columns else [])

    m_filt = maestro.copy()
    if f_disc: m_filt = m_filt[m_filt['DISCIPLINA'].isin(f_disc)]
    if f_gen: m_filt = m_filt[m_filt['GENERO'].isin(f_gen)]
    if f_fran: m_filt = m_filt[m_filt['FRANJA_PRECIO'].isin(f_fran)]

    # --- 4. DASHBOARD ---
    tab1, tab2, tab3 = st.tabs(["📊 Estrategia de Mix", "⚡ Tactical & MOS", "📈 Línea de Tiempo SKU"])

    with tab1:
        st.subheader("Análisis de Salud del Mix (Sell In vs Sell Out)")
        c1, c2, c3 = st.columns(3)
        
        # Lógica para comparar pesos de segmentos
        def get_mix_data(df_base, col):
            temp = df_base.merge(m_filt, on='SKU', how='inner')
            return temp.groupby(col)['UNIDADES' if 'UNIDADES' in temp.columns else 'CANTIDAD'].sum().reset_index()

        with c1:
            st.plotly_chart(px.pie(get_mix_data(sell_in, 'DISCIPLINA'), values='UNIDADES', names='DISCIPLINA', title="Mix Sell In (Venta Dass)", hole=0.4))
        with c2:
            st.plotly_chart(px.pie(get_mix_data(sell_out, 'CANTIDAD'), values='CANTIDAD', names='GENERO', title="Mix Sell Out (Consumidor)", hole=0.4))
        with c3:
            st.plotly_chart(px.pie(get_mix_data(sell_in, 'FRANJA_PRECIO'), values='UNIDADES', names='FRANJA_PRECIO', title="Mix Franja de Precio", hole=0.4))

    with tab2:
        st.subheader("Ranking de Velocidad (MOS)")
        # Calcular venta promedio mensual 2025 para proyectar
        vta_2025 = sell_out[sell_out['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].mean().reset_index()
        stk_total = stock.groupby('SKU')['CANTIDAD'].sum().reset_index()
        
        res = m_filt.merge(stk_total, on='SKU', how='left').merge(vta_2025, on='SKU', how='left').fillna(0)
        res['VENTA_PROY_2026'] = res['CANTIDAD_y'] * (1 + ajuste_venta/100)
        res['MOS'] = (res['CANTIDAD_x'] / res['VENTA_PROY_2026']).replace([float('inf')], 99).round(1)
        
        # Semáforo
        def color_mos(val):
            color = 'red' if val < 2 else 'green' if val < 6 else 'orange'
            return f'color: {color}'
        
        st.dataframe(res[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'CANTIDAD_x', 'VENTA_PROY_2026', 'MOS']]
                     .rename(columns={'CANTIDAD_x': 'Stock Actual', 'VENTA_PROY_2026': 'Venta Proyectada Mensual'})
                     .sort_values('Venta Proyectada Mensual', ascending=False)
                     .style.applymap(color_mos, subset=['MOS']), use_container_width=True)

    with tab3:
        st.subheader("Análisis de Oportunidad y Futuro")
        sku_sel = st.selectbox("Seleccionar Producto para ver Línea de Tiempo", m_filt['SKU'].unique())
        
        if sku_sel:
            # Data Pasada (2025)
            past_so = sell_out[(sell_out['SKU'] == sku_sel) & (sell_out['AÑO'] == 2025)].groupby('MES_KEY')['CANTIDAD'].sum()
            # Data Futura (2026)
            fut_ing = ingresos[(ingresos['SKU'] == sku_sel) & (ingresos['AÑO'] == 2026)].groupby('MES_KEY')['UNIDADES'].sum()
            
            # Stock Actual y Proyección de Stock
            stk_ini = stock[stock['SKU'] == sku_sel]['CANTIDAD'].sum()
            
            fig = go.Figure()
            # Barras de Venta Pasada
            fig.add_trace(go.Bar(x=past_so.index, y=past_so.values, name="Sell Out 2025 (Real)", marker_color='#3498db'))
            # Barras de Ingresos Futuros
            fig.add_trace(go.Bar(x=fut_ing.index, y=fut_ing.values, name="Ingresos 2026 (Plan)", marker_color='#2ecc71'))
            
            # Línea de Stock Proyectado
            meses_2026 = pd.date_range(start='2026-01-01', periods=12, freq='MS').strftime('%Y-%m')
            vta_mensual_proy = (past_so.mean() if not past_so.empty else 0) * (1 + ajuste_venta/100)
            
            stk_evol = []
            curr_stk = stk_ini
            for m in meses_2026:
                ing = fut_ing.get(m, 0)
                curr_stk = curr_stk + ing - vta_mensual_proy
                stk_evol.append(max(0, curr_stk))
                
            fig.add_trace(go.Scatter(x=meses_2026, y=stk_evol, name="Stock Proyectado 2026", line=dict(color='#e74c3c', width=4, dash='dot')))
            
            fig.update_layout(title=f"Cronograma de Disponibilidad: {sku_sel}", barmode='group')
            st.plotly_chart(fig, use_container_width=True)
            
            # Alerta de Salud
            cond = sell_in[sell_in['SKU'] == sku_sel]['CONDICION'].iloc[-1] if sku_sel in sell_in['SKU'].values else "LÍNEA"
            st.info(f"Estado en Dass: **{cond}** | Stock Actual: **{stk_ini}** | Venta Estimada 2026: **{vta_mensual_proy:.0f} u/mes**")

else:
    st.info("Esperando archivos en Drive...")
