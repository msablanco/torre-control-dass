import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control v1.0", layout="wide")

# --- 1. FUNCIÓN DE CARGA (AISLADA) ---
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
            
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = [str(c).strip().upper().replace('ï»¿', '') for c in df.columns]
            
            # Unificación de columna SKU
            posibles_sku = ['SKU', 'ARTICULO', 'CODIGO', 'PRODUCTO', 'ITEM']
            for p in posibles_sku:
                if p in df.columns:
                    df = df.rename(columns={p: 'SKU'})
                    break
            
            if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            name = f['name'].replace('.csv', '')
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error cargando desde Drive: {e}")
        return {}

# --- 2. CARGA DE DATOS ---
data = load_drive_data()

if not data:
    st.warning("⚠️ No se encontraron archivos en la carpeta de Drive o falta configuración de secrets.")
    st.stop()

# Asignación segura de DataFrames
maestro = data.get('Maestro_Productos', pd.DataFrame())
sell_in = data.get('Sell_In_Ventas', pd.DataFrame())
sell_out = data.get('Sell_Out', pd.DataFrame())
stock = data.get('Stock', pd.DataFrame())
ingresos = data.get('Ingresos', pd.DataFrame())

# --- 3. PROCESAMIENTO DE FECHAS ---
for df in [sell_in, sell_out, ingresos]:
    if not df.empty:
        col_fecha = next((c for c in df.columns if 'FECHA' in c or 'MES' in c), None)
        if col_fecha:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES_KEY'] = df['FECHA_DT'].dt.strftime('%Y-%m')

# --- 4. SIDEBAR (FILTROS) ---
st.sidebar.header("🎯 FILTROS ESTRATÉGICOS")

emp_list = ["TODOS", "RETAIL", "ECOM", "WHOLESALE"]
f_emp = st.sidebar.selectbox("Emprendimiento", emp_list)

f_disciplina = st.sidebar.multiselect("Disciplina", maestro['DISCIPLINA'].unique() if not maestro.empty else [])
f_genero = st.sidebar.multiselect("Género", maestro['GENERO'].unique() if not maestro.empty else [])

# Aplicar filtros al Maestro
m_filtrado = maestro.copy()
if f_disciplina: m_filtrado = m_filtrado[m_filtrado['DISCIPLINA'].isin(f_disciplina)]
if f_genero: m_filtrado = m_filtrado[m_filtrado['GENERO'].isin(f_genero)]

# --- 5. LÓGICA DE EXTRAPOLACIÓN ---
factor_expansion = 1.0
if f_emp == "WHOLESALE" and not sell_out.empty and not sell_in.empty:
    if 'EMPRENDIMIENTO' in sell_out.columns and 'EMPRENDIMIENTO' in sell_in.columns:
        clientes_reportan = sell_out[sell_out['EMPRENDIMIENTO'] == 'WHOLESALE']['CLIENTE'].unique()
        si_wh = sell_in[sell_in['EMPRENDIMIENTO'] == 'WHOLESALE']
        total_si = si_wh['UNIDADES'].sum()
        si_repo = si_wh[si_wh['CLIENTE'].isin(clientes_reportan)]['UNIDADES'].sum()
        factor_expansion = (total_si / si_repo) if si_repo > 0 else 1.0

# --- 6. PESTAÑAS ---
tab1, tab2, tab3 = st.tabs(["📊 Estrategia", "⚡ Tactical", "👟 SKU Deep Dive"])

with tab1:
    st.subheader("Performance de Mix")
    if not sell_in.empty and not m_filtrado.empty:
        df_mix = sell_in.merge(m_filtrado, on='SKU', how='inner')
        if not df_mix.empty:
            c1, c2 = st.columns(2)
            fig1 = px.pie(df_mix, values='UNIDADES', names='DISCIPLINA', title="Ventas por Disciplina")
            fig2 = px.pie(df_mix, values='UNIDADES', names='GENERO', title="Ventas por Género")
            c1.plotly_chart(fig1, use_container_width=True)
            c2.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Cargue Maestro y Sell In para ver el mix.")

with tab2:
    st.subheader("Months of Stock (MOS)")
    # Cálculo simplificado de MOS
    if not stock.empty and not sell_out.empty:
        stk_sum = stock.groupby('SKU')['CANTIDAD'].sum().reset_index()
        so_avg = sell_out.groupby('SKU')['CANTIDAD'].mean().reset_index() # Promedio mensual
        
        res = m_filtrado[['SKU', 'DESCRIPCION']].merge(stk_sum, on='SKU', how='left')
        res = res.merge(so_avg, on='SKU', how='left').fillna(0)
        res['VENTA_PROY'] = res['CANTIDAD_y'] * factor_expansion
        res['MOS'] = (res['CANTIDAD_x'] / res['VENTA_PROY']).replace([float('inf')], 99).fillna(0)
        
        st.dataframe(res.sort_values('VENTA_PROY', ascending=False), use_container_width=True)

with tab3:
    st.subheader("Línea de Tiempo de Oportunidad")
    if not m_filtrado.empty:
        sku_select = st.selectbox("Seleccionar SKU", m_filtrado['SKU'].unique())
        
        # Gráfico simple de tendencia
        sku_so = sell_out[sell_out['SKU'] == sku_select].groupby('MES_KEY')['CANTIDAD'].sum().reset_index()
        sku_si = sell_in[sell_in['SKU'] == sku_select].groupby('MES_KEY')['UNIDADES'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=sku_so['MES_KEY'], y=sku_so['CANTIDAD'], name="Sell Out", line=dict(color='blue')))
        fig.add_trace(go.Bar(x=sku_si['MES_KEY'], y=sku_si['UNIDADES'], name="Sell In", marker_color='orange', opacity=0.4))
        st.plotly_chart(fig, use_container_width=True)
