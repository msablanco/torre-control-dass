import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v10.2", layout="wide")

# --- 1. CARGA DE DATOS CON CACHÉ ---
@st.cache_data(ttl=600)
def load_data():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        results = service.files().list(q=f"'{folder_id}' in parents and mimeType='text/csv'", fields="files(id, name)").execute()
        dfs = {}
        for item in results.get('files', []):
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_data()

if data:
    # --- 2. PROCESAMIENTO MAESTRO (Unicidad) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    df_ma = df_ma.drop_duplicates(subset=['SKU'])
    
    col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
    col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
    df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. PROCESAMIENTO TRANSACCIONES ---
    def clean_trans(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return df
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        df['Cant'] = pd.to_numeric(df.get('Unidades', df.get('Cantidad', 0)), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        return df

    so_raw = clean_trans('Sell_out')
    si_raw = clean_trans('Sell_in')
    stk_raw = clean_trans('Stock')
    stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

    # --- 4. SIDEBAR Y FILTROS CRONOLÓGICOS ---
    st.sidebar.header("🔍 Filtros de Control")
    
    raw_months = sorted(list(set(so_raw['Mes'].dropna()) | set(si_raw['Mes'].dropna()) | set(stk_raw['Mes'].dropna())), 
                        key=lambda x: pd.to_datetime(x, format='%Y-%m'), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Periodo (Mes)", ["Todos"] + raw_months)
    
    search_query = st.sidebar.text_input("🔎 Buscador (SKU / Descripción)").upper()

    # Filtros dinámicos (Solo disciplinas con datos)
    skus_con_mov = set(so_raw['SKU']) | set(si_raw['SKU']) | set(stk_raw['SKU'])
    maestro_act = df_ma[df_ma['SKU'].isin(skus_con_mov)]
    f_dis = st.sidebar.multiselect("👟 Disciplinas Activas", sorted(maestro_act['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas Activas", sorted(maestro_act['FRANJA_PRECIO'].unique()))

    # --- 5. LÓGICA DE FILTRADO ---
    def apply_filters(df_target, is_maestro=False):
        t = df_target.copy()
        if not is_maestro:
            if f_periodo != "Todos": t = t[t['Mes'] == f_periodo]
            # Unir con maestro para filtrar por categoria
            t = t.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')
        if f_dis: t = t[t['Disciplina'].isin(f_dis)]
        if f_fra: t = t[t['FRANJA_PRECIO'].isin(f_fra)]
        if search_query:
            mask = t.apply(lambda row: row.astype(str).str.contains(search_query).any(), axis=1)
            t = t[mask]
        return t

    so_final = apply_filters(so_raw)
    si_final = apply_filters(si_raw)
    stk_final = apply_filters(stk_raw)
    df_ma_filt = apply_filters(df_ma, is_maestro=True)

    # --- 6. DASHBOARD ---
    st.title(f"📊 Torre de Control Dass v10.2")

    # --- DETECTOR DE PICO HISTÓRICO (Optimizado) ---
    if search_query and len(search_query) > 3:
        # Buscamos el histórico sin el filtro de mes del sidebar
        hist_so = so_raw[so_raw['SKU'].str.contains(search_query)].groupby('Mes')['Cant'].sum().reset_index()
        if not hist_so.empty:
            st.subheader(f"📈 Tendencia de Venta: {search_query}")
            m1, m2 = st.columns([1, 3])
            mes_pico = hist_so.loc[hist_so['Cant'].idxmax()]
            m1.metric("Mes Pico Histórico", mes_pico['Mes'], f"{mes_pico['Cant']:,.0f} uds")
            m2.plotly_chart(px.line(hist_so.sort_values('Mes'), x='Mes', y='Cant', markers=True, height=250), use_container_width=True)
            st.divider()

    # KPIs
    max_f = stk_final['Fecha_dt'].max()
    stk_snap = stk_final[stk_final['Fecha_dt'] == max_f]
    
    k_so = so_final['Cant'].sum()
    k_si = si_final['Cant'].sum()
    k_sc = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum()
    k_sd = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum()

    col_k1, col_k2, col_k3, col_k4 = st.columns(4)
    col_k1.metric("Sell Out", f"{k_so:,.0f}")
    col_k2.metric("Sell In", f"{k_si:,.0f}")
    col_k3.metric("Stock Cliente", f"{k_sc:,.0f}")
    col_k4.metric("Stock Dass", f"{k_sd:,.0f}")

    # --- 7. BLOQUES DE GRÁFICOS (3 FILAS x 4 COLUMNAS) ---
    def render_row(title, dataframe_filt, si_data, group_col, color_map=None):
        st.subheader(title)
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        
        # Preparar data para tortas
        so_p = dataframe_filt.groupby(group_col)['Cant'].sum().reset_index()
        sd_p = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')].merge(df_ma[['SKU', group_col]], on='SKU').groupby(group_col)['Cant'].sum().reset_index()
        sc_p = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')].merge(df_ma[['SKU', group_col]], on='SKU').groupby(group_col)['Cant'].sum().reset_index()

        if not sd_p.empty: c1.plotly_chart(px.pie(sd_p, values='Cant', names=group_col, title="Stk Dass", color=group_col, color_discrete_map=color_map), use_container_width=True)
        if not so_p.empty: c2.plotly_chart(px.pie(so_p, values='Cant', names=group_col, title="Sell Out", color=group_col, color_discrete_map=color_map), use_container_width=True)
        if not sc_p.empty: c3.plotly_chart(px.pie(sc_p, values='Cant', names=group_col, title="Stk Cliente", color=group_col, color_discrete_map=color_map), use_container_width=True)
        
        # Barras Sell In Mensual
        si_m = si_data.groupby(['Mes', group_col])['Cant'].sum().reset_index()
        c4.plotly_chart(px.bar(si_m, x='Mes', y='Cant', color=group_col, title=f"Evolución {group_col}", color_discrete_map=color_map), use_container_width=True)

    render_row("📌 Análisis por Disciplina", so_final, si_final, 'Disciplina', COLOR_MAP_DIS)
    render_row("🏷️ Análisis por Franja Comercial", so_final, si_final, 'FRANJA_PRECIO')
    
    # Fila 3: Ingresos (Usamos si_final para los Arribos)
    st.subheader("🚚 Análisis de Arribos (Ingresos)")
    i1, i2, i3, i4 = st.columns([1, 1, 1, 2])
    ing_dis = si_final.groupby('Disciplina')['Cant'].sum().reset_index()
    ing_fra = si_final.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index()
    
    i1.plotly_chart(px.pie(ing_dis, values='Cant', names='Disciplina', title="Ingresos x Dis", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    i2.plotly_chart(px.pie(ing_fra, values='Cant', names='FRANJA_PRECIO', title="Ingresos x Franja"), use_container_width=True)
    # i3 repetimos stock Dass para mantener simetría
    sd_dis = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')].merge(df_ma[['SKU', 'Disciplina']], on='SKU').groupby('Disciplina')['Cant'].sum().reset_index()
    i3.plotly_chart(px.pie(sd_dis, values='Cant', names='Disciplina', title="Stock Dass Part.", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    i4.plotly_chart(px.bar(si_final.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Arribos x Franja Mensual"), use_container_width=True)

    # --- 8. TABLA FINAL ---
    st.divider()
    # Construcción de la tabla resumen por SKU
    so_sum = so_final.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SO'})
    si_sum = si_final.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SI'})
    sc_sum = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SC'})
    sd_sum = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SD'})
    
    df_tab = df_ma_filt.merge(so_sum, on='SKU', how='left').merge(si_sum, on='SKU', how='left').merge(sc_sum, on='SKU', how='left').merge(sd_sum, on='SKU', how='left').fillna(0)
    st.dataframe(df_tab[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'SO', 'SI', 'SC', 'SD']].sort_values('SO', ascending=False), use_container_width=True, hide_index=True)
