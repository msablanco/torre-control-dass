import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control v1.0", layout="wide")

# --- 1. CONEXIÓN Y CARGA DE DATOS ---
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
        
        dfs = {}
        for f in files:
            request = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            name = f['name'].replace('.csv', '')
            # Lectura flexible
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = df.columns.str.strip().str.upper()
            if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper()
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en carga: {e}")
        return {}

data = load_drive_data()

# --- 2. PROCESAMIENTO CORE ---
if data:
    # Asignación de DataFrames
    maestro = data.get('Maestro_Productos', pd.DataFrame())
    sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
    sell_out = data.get('Sell_Out', pd.DataFrame())
    stock = data.get('Stock', pd.DataFrame())
    ingresos = data.get('Ingresos', pd.DataFrame())

    # Formateo de fechas y periodos
    for df in [sell_in, sell_out, ingresos]:
        col_fecha = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
        if col_fecha:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')

    # --- 3. FILTROS LATERALES ---
    st.sidebar.header("🎯 SEGMENTACIÓN ESTRATÉGICA")
    
    # Filtro de Emprendimiento (Punto 12)
    emp_list = ["TODOS", "RETAIL", "ECOM", "WHOLESALE"]
    f_emp = st.sidebar.selectbox("Emprendimiento", emp_list)
    
    # Filtros de Maestro
    f_disciplina = st.sidebar.multiselect("Disciplina", maestro['DISCIPLINA'].unique() if not maestro.empty else [])
    f_genero = st.sidebar.multiselect("Género", maestro['GENERO'].unique() if not maestro.empty else [])
    f_franja = st.sidebar.multiselect("Franja de Precio", maestro['FRANJA_PRECIO'].unique() if not maestro.empty else [])
    
    # Aplicar filtros al Maestro para cascada
    m_filtrado = maestro.copy()
    if f_disciplina: m_filtrado = m_filtrado[m_filtrado['DISCIPLINA'].isin(f_disciplina)]
    if f_genero: m_filtrado = m_filtrado[m_filtrado['GENERO'].isin(f_genero)]
    if f_franja: m_filtrado = m_filtrado[m_filtrado['FRANJA_PRECIO'].isin(f_franja)]

    # --- 4. LÓGICA DE EXTRAPOLACIÓN (Factor de Expansión) ---
    # Si es Wholesale, calculamos el factor basado en el Sell In de quienes reportan
    if f_emp == "WHOLESALE":
        clientes_reportan = sell_out[sell_out['EMPRENDIMIENTO'] == 'WHOLESALE']['CLIENTE'].unique()
        si_total = sell_in[sell_in['EMPRENDIMIENTO'] == 'WHOLESALE']['UNIDADES'].sum()
        si_reportan = sell_in[(sell_in['EMPRENDIMIENTO'] == 'WHOLESALE') & (sell_in['CLIENTE'].isin(clientes_reportan))]['UNIDADES'].sum()
        factor_expansion = (si_total / si_reportan) if si_reportan > 0 else 1
    else:
        factor_expansion = 1

    # --- 5. PESTAÑAS DE ANÁLISIS ---
    tab1, tab2, tab3 = st.tabs(["📊 Estrategia General", "⚡ Tactical (MOS & Rankings)", "👟 SKU Deep Dive"])

    with tab1:
        st.subheader("Mix de Negocio: Sell In vs Sell Out")
        c1, c2, c3 = st.columns(3)
        
        # Agrupamiento para gráficos de torta
        def plot_mix(df_v, col_attr, title):
            temp = df_v.merge(maestro, on='SKU', how='inner')
            fig = px.pie(temp, values='UNIDADES' if 'UNIDADES' in temp.columns else 'CANTIDAD', 
                         names=col_attr, title=title, hole=0.4, color_discrete_sequence=px.colors.qualitative.Safe)
            return fig

        with c1: st.plotly_chart(plot_mix(sell_in, 'DISCIPLINA', "Mix Sell In por Disciplina"), use_container_width=True)
        with c2: st.plotly_chart(plot_mix(sell_in, 'GENERO', "Mix Sell In por Género"), use_container_width=True)
        with c3: st.plotly_chart(plot_mix(sell_in, 'FRANJA_PRECIO', "Mix Sell In por Franja"), use_container_width=True)

    with tab2:
        st.subheader("Análisis de Velocidad y Ranking")
        
        # Cálculo de MOS
        so_agrupado = sell_out.groupby('SKU')['CANTIDAD'].mean().reset_index() # Promedio mensual simplificado
        stk_agrupado = stock.groupby('SKU')['CANTIDAD'].sum().reset_index()
        
        mos_df = m_filtrado[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(stk_agrupado, on='SKU', how='left')
        mos_df = mos_df.merge(so_agrupado, on='SKU', how='left').fillna(0)
        mos_df['CANTIDAD_y'] = mos_df['CANTIDAD_y'] * factor_expansion # Aplicamos el factor
        mos_df['MOS'] = (mos_df['CANTIDAD_x'] / mos_df['CANTIDAD_y']).replace([float('inf')], 99).fillna(0)
        
        st.dataframe(mos_df.sort_values('CANTIDAD_y', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("Línea de Tiempo de Oportunidad")
        sku_select = st.selectbox("Seleccionar SKU para análisis detallado", m_filtrado['SKU'].unique())
        
        if sku_select:
            # Datos históricos y futuros del SKU
            hist_so = sell_out[sell_out['SKU'] == sku_select].groupby('MES_KEY')['CANTIDAD'].sum()
            hist_si = sell_in[sell_in['SKU'] == sku_select].groupby('MES_KEY')['UNIDADES'].sum()
            fut_ing = ingresos[ingresos['SKU'] == sku_select].groupby('MES_KEY')['UNIDADES'].sum()
            
            # Gráfico de Proyección
            fig_proj = go.Figure()
            fig_proj.add_trace(go.Scatter(x=hist_so.index, y=hist_so.values, name="Sell Out Real", line=dict(color='blue', width=3)))
            fig_proj.add_trace(go.Bar(x=fut_ing.index, y=fut_ing.values, name="Ingresos (Plan/Real)", marker_color='green', opacity=0.5))
            
            # Línea de Stock (Acumulativa simplificada)
            stk_inicial = stock[stock['SKU'] == sku_select]['CANTIDAD'].sum()
            st.metric("Stock Físico Actual", f"{stk_inicial:,.0f} unidades")
            
            st.plotly_chart(fig_proj, use_container_width=True)
            
            # Alerta de Salud
            condicion_sku = sell_in[sell_in['SKU'] == sku_select]['CONDICION'].iloc[-1] if sku_select in sell_in['SKU'].values else "LINEA"
            if condicion_sku == "OFF":
                st.warning(f"⚠️ El SKU {sku_select} está operando bajo condición OFF. Revisar rentabilidad.")
            else:
                st.success(f"✅ El SKU {sku_select} se mantiene como producto de LINEA.")

else:
    st.info("Esperando archivos CSV en la carpeta de Google Drive...")
