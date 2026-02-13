import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="FILA - Torre de Control v2.1", layout="wide")

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
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
                df.columns = [str(c).strip().upper().replace('ï»¿', '') for c in df.columns]
                df = df.rename(columns={'ARTICULO': 'SKU', 'CODIGO': 'SKU', 'PRODUCTO': 'SKU'})
                if 'SKU' in df.columns:
                    df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
                dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error crítico: {e}")
        return {}

data = load_drive_data()

if data:
    # --- 2. ASIGNACIÓN Y PREPARACIÓN ---
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

    # --- 3. SIDEBAR ---
    st.sidebar.header("🕹️ CONTROL COMERCIAL")
    ajuste_venta = st.sidebar.slider("Ajuste Presupuesto 2026 (%)", -50, 100, 20)
    
    m_filt = maestro.copy()
    if not m_filt.empty:
        f_disc = st.sidebar.multiselect("Disciplina", m_filt['DISCIPLINA'].unique() if 'DISCIPLINA' in m_filt.columns else [])
        if f_disc: m_filt = m_filt[m_filt['DISCIPLINA'].isin(f_disc)]
        f_gen = st.sidebar.multiselect("Género", m_filt['GENERO'].unique() if 'GENERO' in m_filt.columns else [])
        if f_gen: m_filt = m_filt[m_filt['GENERO'].isin(f_gen)]

    # --- 4. FUNCIONES DE AYUDA ---
    def get_mix_data_safe(df_base, col_maestro):
        if df_base.empty or m_filt.empty: return pd.DataFrame()
        cols_to_use = [c for c in m_filt.columns if c not in df_base.columns or c == 'SKU']
        temp = df_base.merge(m_filt[cols_to_use], on='SKU', how='inner')
        if temp.empty: return pd.DataFrame()
        val_col = 'UNIDADES' if 'UNIDADES' in temp.columns else 'CANTIDAD'
        return temp.groupby(col_maestro)[val_col].sum().reset_index()

    # --- 5. TABS ---
    tab1, tab2, tab3 = st.tabs(["📊 Estrategia", "⚡ Tactical", "📈 SKU Deep Dive"])

    with tab1:
        st.subheader("Análisis de Mix Sell In vs Sell Out")
        c1, c2 = st.columns(2)
        with c1:
            df_si = get_mix_data_safe(sell_in, 'DISCIPLINA')
            if not df_si.empty:
                st.plotly_chart(px.pie(df_si, values=df_si.columns[1], names='DISCIPLINA', title="Mix Sell In por Disciplina"), use_container_width=True)
        with c2:
            df_so = get_mix_data_safe(sell_out, 'GENERO')
            if not df_so.empty:
                st.plotly_chart(px.pie(df_so, values=df_so.columns[1], names='GENERO', title="Mix Sell Out por Género"), use_container_width=True)

    with tab2:
        st.subheader("Months of Stock (MOS) Proyectado")
        vta_25 = sell_out[sell_out['AÑO'] == 2025].groupby('SKU')['CANTIDAD'].mean().reset_index()
        stk_act = stock.groupby('SKU')['CANTIDAD'].sum().reset_index()
        
        if not stk_act.empty:
            res = m_filt.merge(stk_act, on='SKU', how='left').merge(vta_25, on='SKU', how='left').fillna(0)
            res['VENTA_26'] = res['CANTIDAD_y'] * (1 + ajuste_venta/100)
            res['MOS'] = (res['CANTIDAD_x'] / res['VENTA_26']).replace([float('inf')], 99).round(1)
            st.dataframe(res[['SKU', 'DESCRIPCION', 'CANTIDAD_x', 'VENTA_26', 'MOS']].sort_values('VENTA_26', ascending=False), use_container_width=True)

    with tab3:
        st.subheader("Línea de Tiempo de Oportunidad")
        if not m_filt.empty:
            sku_sel = st.selectbox("Seleccionar SKU", m_filt['SKU'].unique())
            if sku_sel:
                stk_ini = stock[stock['SKU'] == sku_sel]['CANTIDAD'].sum()
                vta_hist = sell_out[(sell_out['SKU'] == sku_sel) & (sell_out['AÑO'] == 2025)].groupby('MES_KEY')['CANTIDAD'].sum()
                ing_fut = ingresos[(ingresos['SKU'] == sku_sel) & (ingresos['AÑO'] == 2026)].groupby('MES_KEY')['UNIDADES'].sum()
                
                fig = go.Figure()
                fig.add_trace(go.Bar(x=vta_hist.index, y=vta_hist.values, name="Venta 2025", marker_color='blue', opacity=0.4))
                fig.add_trace(go.Bar(x=ing_fut.index, y=ing_fut.values, name="Ingresos 2026", marker_color='green'))
                
                # Proyección Stock
                meses_26 = pd.date_range(start='2026-01-01', periods=12, freq='MS').strftime('%Y-%m')
                v_p = (vta_hist.mean() if not vta_hist.empty else 0) * (1 + ajuste_venta/100)
                stk_e = []
                c_s = stk_ini
                for m in meses_26:
                    c_s = c_s + ing_fut.get(m, 0) - v_p
                    stk_e.append(max(0, c_s))
                
                fig.add_trace(go.Scatter(x=meses_26, y=stk_e, name="Stock Proyectado", line=dict(color='red', dash='dot')))
                st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Cargando datos de Google Drive...")
