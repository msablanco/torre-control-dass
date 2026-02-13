import streamlit as st
import pandas as pd
import io
import plotly.graph_objects as go
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="FILA - Torre de Control S&OP", layout="wide")

def fmt(v):
    if pd.isna(v) or v == 0: return "0"
    return f"{v:,.0f}".replace(",", ".")

# --- 2. CARGA DE DATOS DESDE GOOGLE DRIVE ---
@st.cache_data(ttl=600)
def load_data_from_drive():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'",
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        dfs = {}
        
        for f in files:
            name = f['name'].replace('.csv', '').strip()
            request = service.files().get_media(fileId=f['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python')
            
            # Normalización de Columnas
            df.columns = [str(c).strip().upper() for c in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            df = df.rename(columns={
                'ARTICULO': 'SKU', 'CODIGO': 'SKU', 
                'CANT': 'CANTIDAD', 'QTY': 'CANTIDAD', 'UNIDADES': 'CANTIDAD'
            })
            
            if 'SKU' in df.columns:
                df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
            
            # Lógica específica para Sell In Wholesale
            if "SELL_IN_VENTAS" in name.upper():
                if 'EMPRENDIMIENTO' not in df.columns:
                    df['EMPRENDIMIENTO'] = 'WHOLESALE'
                if len(df.columns) >= 2:
                    df = df.rename(columns={df.columns[1]: 'F_REF'})
                if len(df.columns) >= 7:
                    df = df.rename(columns={df.columns[6]: 'CANTIDAD'})
            
            if 'CANTIDAD' in df.columns:
                df['CANTIDAD'] = pd.to_numeric(df['CANTIDAD'], errors='coerce').fillna(0)
                
            dfs[name] = df
        return dfs
    except Exception as e:
        st.error(f"Error en conexión: {e}")
        return {}

data = load_data_from_drive()

if data:
    # 3. ASIGNACIÓN DE DATAFRAMES
    so_raw = data.get('Sell_Out', pd.DataFrame())
    si_raw = data.get('Sell_In_Ventas', data.get('Sell_In', pd.DataFrame()))
    mae = data.get('Maestro_Productos', pd.DataFrame()).drop_duplicates('SKU')
    stk_raw = data.get('Stock', pd.DataFrame())

    # 4. SIDEBAR - CONTROL DE PARÁMETROS
    st.sidebar.header("🎯 OBJETIVOS 2026")
    vol_obj = st.sidebar.number_input("Volumen Objetivo Anual", value=1000000, step=50000)
    mos_target = st.sidebar.slider("MOS Objetivo (Meses de Stock)", 1, 8, 3)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 FILTROS DINÁMICOS")
    
    canales = sorted(so_raw['EMPRENDIMIENTO'].unique()) if 'EMPRENDIMIENTO' in so_raw.columns else []
    f_canal = st.sidebar.multiselect("Filtrar por Canal", canales)
    
    query = st.sidebar.text_input("Buscar SKU o Descripción", "").upper()

    # 5. PROCESAMIENTO SELL OUT (BASE 2025)
    col_fecha = next((c for c in so_raw.columns if any(x in c for x in ['FECHA', 'DATE', 'MES'])), None)
    so_raw['FECHA_DT'] = pd.to_datetime(so_raw[col_fecha], dayfirst=True, errors='coerce')
    
    # Filtro Año 2025
    so_25 = so_raw[so_raw['FECHA_DT'].dt.year == 2025].copy()
    so_25 = so_25.merge(mae[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']], on='SKU', how='left')
    
    # Aplicar Filtro de Canal
    df_c = so_25[so_25['EMPRENDIMIENTO'].isin(f_canal)] if f_canal else so_25.copy()
    
    # Cálculo de Factor de Escala Global
    total_25 = df_c['CANTIDAD'].sum()
    factor = vol_obj / total_25 if total_25 > 0 else 1
    
    # Filtro de Búsqueda para la Vista
    df_v = df_c[df_c['SKU'].str.contains(query) | df_c['DESCRIPCION'].str.contains(query, na=False)] if query else df_c.copy()

    # 6. SERIES TEMPORALES
    meses_idx = range(1, 13)
    meses_lbl = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    v_out_m = df_v.groupby(df_v['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)
    v_proy_m = (v_out_m * factor).round(0)

    # Sell In Procesamiento
    si_raw['FECHA_DT'] = pd.to_datetime(si_raw['F_REF'] if 'F_REF' in si_raw.columns else si_raw.filter(like='FECHA').iloc[:,0], dayfirst=True, errors='coerce')
    si_25 = si_raw[si_raw['FECHA_DT'].dt.year == 2025].copy()
    if f_canal:
        si_25 = si_25[si_25['EMPRENDIMIENTO'].isin(f_canal)] if 'EMPRENDIMIENTO' in si_25.columns else si_25
    si_v = si_25.merge(mae[['SKU', 'DESCRIPCION']], on='SKU', how='left')
    if query:
        si_v = si_v[si_v['SKU'].str.contains(query) | si_v['DESCRIPCION'].str.contains(query, na=False)]
    v_in_m = si_v.groupby(si_v['FECHA_DT'].dt.month)['CANTIDAD'].sum().reindex(meses_idx, fill_value=0)

    # 7. INTERFAZ DE USUARIO (TABS)
    tab1, tab2 = st.tabs(["📊 PERFORMANCE DE VENTAS", "🎯 MATRIZ DE COMPRA S&OP"])

    with tab1:
        c1, c2, c3 = st.columns(3)
        c1.metric("Proyección en Vista", fmt(v_proy_m.sum()))
        c2.metric("Factor de Ajuste", f"{factor:.4f}")
        c3.metric("Sell Out Base 2025", fmt(v_out_m.sum()))

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=meses_lbl, y=v_in_m.tolist(), name="Sell In 2025", line=dict(color='#3366CC', width=2)))
        fig.add_trace(go.Scatter(x=meses_lbl, y=v_out_m.tolist(), name="Sell Out 2025", line=dict(color='#FF9900', dash='dot')))
        fig.add_trace(go.Scatter(x=meses_lbl, y=v_proy_m.tolist(), name="Proyección 2026", line=dict(color='#00FF00', width=4)))
        
        fig.update_layout(title="Curva de Demanda: Real 2025 vs Proyectado 2026", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Resumen Mensual (Unidades)")
        df_table = pd.DataFrame({
            "Sell In 25": v_in_m.values,
            "Sell Out 25": v_out_m.values,
            "Proyección 26": v_proy_m.values
        }, index=meses_lbl).T
        st.dataframe(df_table.style.format(fmt), use_container_width=True)

    with tab2:
        st.subheader("🏢 Estrategia por Disciplina y Franja de Precio")
        
        # Agregación para Matriz
        stk_agg = stk_raw.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'STOCK_ACTUAL'})
        vta_agg = df_c.groupby('SKU')['CANTIDAD'].sum().reset_index().rename(columns={'CANTIDAD': 'VTA_25'})
        
        # Unir todo al Maestro
        matrix = mae[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(stk_agg, on='SKU', how='left').merge(vta_agg, on='SKU', how='left').fillna(0)
        
        matrix['VTA_PROY_26'] = (matrix['VTA_25'] * factor).round(0)
        matrix['VENTA_MENSUAL'] = matrix['VTA_PROY_26'] / 12
        matrix['MOS_ACTUAL'] = (matrix['STOCK_ACTUAL'] / matrix['VENTA_MENSUAL'].replace(0, 1)).round(1)
        matrix['SUGERIDO_COMPRA'] = ((matrix['VENTA_MENSUAL'] * mos_target) - matrix['STOCK_ACTUAL']).clip(lower=0).round(0)

        # Tabla Resumen
        resumen_sop = matrix.groupby(['DISCIPLINA', 'FRANJA_PRECIO']).agg({
            'VTA_25': 'sum',
            'STOCK_ACTUAL': 'sum',
            'VTA_PROY_26': 'sum',
            'SUGERIDO_COMPRA': 'sum'
        }).reset_index()
        
        st.dataframe(resumen_sop.sort_values('SUGERIDO_COMPRA', ascending=False).style.format({
            'VTA_25': fmt, 'STOCK_ACTUAL': fmt, 'VTA_PROY_26': fmt, 'SUGERIDO_COMPRA': fmt
        }), use_container_width=True)

        st.markdown("---")
        st.subheader("🔍 Detalle por SKU (Salud de Inventario)")
        if query:
            matrix = matrix[matrix['SKU'].str.contains(query) | matrix['DESCRIPCION'].str.contains(query, na=False)]
        
        st.dataframe(matrix.sort_values('VTA_PROY_26', ascending=False).style.format({
            'STOCK_ACTUAL': fmt, 'VTA_25': fmt, 'VTA_PROY_26': fmt, 'SUGERIDO_COMPRA': fmt, 'MOS_ACTUAL': '{:.1f}'
        }), use_container_width=True)

else:
    st.info("Esperando conexión con los archivos de Google Drive...")

# Fin del código
