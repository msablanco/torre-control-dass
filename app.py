import streamlit as st
import pandas as pd
import io
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. CONFIGURACIÓN ---
st.set_page_config(page_title="FILA - Torre de Control", layout="wide")

def fmt(v):
    return f"{v:,.0f}".replace(",", ".") if v and not pd.isna(v) else "0"

# --- 2. CARGA DE DATOS ---
@st.cache_data(ttl=600)
def load_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        f_id = st.secrets["google_drive_folder_id"]
        res = service.files().list(q=f"'{f_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
        dfs = {}
        for f in res.get('files', []):
            name = f['name'].replace('.csv', '').strip()
            req = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, req)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            df.columns = [str(c).strip().upper() for c in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Normalización
            df = df.rename(columns={'ARTICULO':'SKU','CODIGO':'SKU','CANT':'CANTIDAD','QTY':'CANTIDAD','UNIDADES':'CANTIDAD'})
            if 'SKU' in df.columns: df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            if "SELL_IN_VENTAS" in name.upper():
                if len(df.columns) >= 2: df = df.rename(columns={df.columns[1]: 'F_REF'})
                if len(df.columns) >= 7: df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
            
            if 'CANTIDAD' in df.columns:
                df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce').fillna(0)
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en conexión: {e}")
        return {}

data = load_data()

if data:
    so_raw = data.get('Sell_Out', pd.DataFrame())
    si_raw = data.get('Sell_In_Ventas', pd.DataFrame())
    mae = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stk_raw = data.get('Stock', pd.DataFrame())

    # --- 3. SIDEBAR ---
    st.sidebar.header("🎯 PARÁMETROS SOP")
    obj_26 = st.sidebar.number_input("Objetivo 2026", value=700000)
    mos_obj = st.sidebar.slider("MOS Objetivo", 1, 8, 3)
    
    canales = sorted(so_raw['EMPRENDIMIENTO'].unique()) if 'EMPRENDIMIENTO' in so_raw.columns else []
    f_emp = st.sidebar.multiselect("Canal", canales)
    q = st.sidebar.text_input("🔍 Buscar SKU/Desc").upper()

    # --- 4. PROCESAMIENTO FECHAS (BLINDADO) ---
    def get_date_col(df):
        for c in df.columns:
            if any(x in c for x in ['FECHA', 'DATE', 'F_REF', 'MES']): return c
        return None

    # Procesar Sell Out
    c_f_out = get_date_col(so_raw)
    if c_f_out:
        so_raw['FECHA_DT'] = pd.to_datetime(so_raw[c_f_out], dayfirst=True, errors='coerce')
        so_25 = so_raw[so_raw['FECHA_DT'].dt.year == 2025].copy()
    else:
        so_25 = pd.DataFrame()

    # Procesar Sell In
    c_f_in = get_date_col(si_raw)
    if c_f_in:
        si_raw['FECHA_DT'] = pd.to_datetime(si_raw[c_f_in], dayfirst=True, errors='coerce')
        si_25 = si_raw[si_raw['FECHA_DT'].dt.year == 2025].copy()
    else:
        si_25 = pd.DataFrame()

    # --- 5. FILTROS Y FACTOR ---
    so_25 = so_25.merge(mae[['SKU','DESCRIPCION','DISCIPLINA','FRANJA_PRECIO']], on='SKU', how='left')
    df_c = so_25[so_25['EMPRENDIMIENTO'].isin(f_emp)] if f_emp else so_25.copy()
    
    # Factor sobre el canal
    total_25 = df_c['CANTIDAD'].sum()
    factor = obj_26 / total_25 if total_25 > 0 else 1
    
    # Vista filtrada por búsqueda
    df_v = df_c[df_c['SKU'].str.contains(q) | df_c['DESCRIPCION'].str.contains(q, na=False)] if q else df_c.copy()

    # --- 6. AGRUPACIÓN POR MES ---
    m_idx = range(1, 13)
    m_lbl = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]
    
    v_out = df_v.groupby(df_v['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(m_idx, fill_value=0)
    v_proy = (v_out * factor).round(0)

    # Sell In filtrado
    si_v = si_25.copy()
    if f_emp and 'EMPRENDIMIENTO' in si_v.columns: si_v = si_v[si_v['EMPRENDIMIENTO'].isin(f_emp)]
    si_v = si_v.merge(mae[['SKU','DESCRIPCION']], on='SKU', how='left')
    if q: si_v = si_v[si_v['SKU'].str.contains(q) | si_v['DESCRIPCION'].str.contains(q, na=False)]
    v_in = si_v.groupby(si_v['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(m_idx, fill_value=0)

    # --- 7. TABS ---
    t1, t2 = st.tabs(["📊 PERFORMANCE", "🎯 ESTRATEGIA"])
    
    with t1:
        st.metric("Factor Ajuste", f"{factor:.4f}")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=m_lbl, y=v_in.values, name="Sell In 25", line=dict(color='#3366CC', width=2)))
        fig.add_trace(go.Scatter(x=m_lbl, y=v_out.values, name="Sell Out 25", line=dict(color='#FF9900', dash='dot')))
        fig.add_trace(go.Scatter(x=m_lbl, y=v_proy.values, name="Proy 26", line=dict(color='#00FF00', width=4)))
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla Detalle
        df_res = pd.DataFrame({"Sell In 2025": v_in.values, "Sell Out 2025": v_out.values, "Proy 2026": v_proy.values}, index=m_lbl).T
        df_res['TOTAL'] = df_res.sum(axis=1)
        st.write("### 📋 Detalle Mensual")
        st.dataframe(df_res.style.format(fmt), use_container_width=True)

    with t2:
        # Matriz de compra
        stk = stk_raw.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD':'STK'})
        v25 = df_c.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD':'V25'})
        mat = mae[['SKU','DESCRIPCION','DISCIPLINA','FRANJA_PRECIO']].merge(stk,on='SKU',how='left').merge(v25,on='SKU',how='left').fillna(0)
        mat['V26'] = (mat['V25']*factor).round(0)
        mat['SUG'] = ((mat['V26']/12*mos_obj)-mat['STK']).clip(lower=0).round(0)
        
        st.subheader("🏢 Resumen por Disciplina")
        res = mat.groupby(['DISCIPLINA','FRANJA_PRECIO']).agg({'V25':'sum','STK':'sum','V26':'sum','SUG':'sum'}).reset_index()
        st.dataframe(res.sort_values('SUG',ascending=False).style.format({c:fmt for c in res.columns if c not in ['DISCIPLINA','FRANJA_PRECIO']}), use_container_width=True)

else:
    st.info("Esperando conexión con Drive...")
