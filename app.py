import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v10.3", layout="wide")

# --- 1. CONFIGURACIÓN DE COLORES (DEFINICIÓN GLOBAL) ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131',
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700',
    'TENIS': '#FFD700', 'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513',
    'FOOTBALL': '#000000', 'FUTBOL': '#000000'
}

# --- 2. CARGA DE DATOS CON CACHÉ ---
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
    # --- 3. PROCESAMIENTO MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
        df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
        col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
        df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 4. PROCESAMIENTO TRANSACCIONES ---
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
    if not stk_raw.empty:
        stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

    # --- 5. SIDEBAR Y FILTROS ---
    st.sidebar.header("🔍 Filtros de Control")
    raw_months = sorted(list(set(so_raw['Mes'].dropna()) | set(si_raw['Mes'].dropna()) | (set(stk_raw['Mes'].dropna()) if not stk_raw.empty else set())), 
                        key=lambda x: pd.to_datetime(x, format='%Y-%m'), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Periodo (Mes)", ["Todos"] + raw_months)
    search_query = st.sidebar.text_input("🔎 Buscador (SKU / Descripción)").upper()

    skus_con_mov = set(so_raw['SKU']) | set(si_raw['SKU']) | (set(stk_raw['SKU']) if not stk_raw.empty else set())
    maestro_act = df_ma[df_ma['SKU'].isin(skus_con_mov)]
    f_dis = st.sidebar.multiselect("👟 Disciplinas Activas", sorted(maestro_act['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas Activas", sorted(maestro_act['FRANJA_PRECIO'].unique()))

    def apply_filters(df_target, is_maestro=False):
        t = df_target.copy()
        if t.empty: return t
        if not is_maestro:
            if f_periodo != "Todos": t = t[t['Mes'] == f_periodo]
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
    st.title(f"📊 Torre de Control Dass v10.3")

    if search_query and len(search_query) > 3:
        hist_so = so_raw[so_raw['SKU'].str.contains(search_query)].groupby('Mes')['Cant'].sum().reset_index()
        if not hist_so.empty:
            st.subheader(f"📈 Tendencia: {search_query}")
            m1, m2 = st.columns([1, 3])
            mes_pico = hist_so.loc[hist_so['Cant'].idxmax()]
            m1.metric("Mes Pico", mes_pico['Mes'], f"{mes_pico['Cant']:,.0f} uds")
            m2.plotly_chart(px.line(hist_so.sort_values('Mes'), x='Mes', y='Cant', markers=True, height=250), use_container_width=True)
            st.divider()

    # KPIs e Inventario
    max_f = stk_final['Fecha_dt'].max() if not stk_final.empty else None
    stk_snap = stk_final[stk_final['Fecha_dt'] == max_f] if max_f else pd.DataFrame()
    
    k_so = so_final['Cant'].sum()
    k_si = si_final['Cant'].sum()
    k_sc = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum() if not stk_snap.empty else 0
    k_sd = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum() if not stk_snap.empty else 0

    col_k1, col_k2, col_k3, col_k4 = st.columns(4)
    col_k1.metric("Sell Out", f"{k_so:,.0f}")
    col_k2.metric("Sell In", f"{k_si:,.0f}")
    col_k3.metric("Stock Cliente", f"{k_sc:,.0f}")
    col_k4.metric("Stock Dass", f"{k_sd:,.0f}")

    # --- 7. BLOQUES DE GRÁFICOS ---
    def render_row(title, so_data, si_data, stk_data, group_col, color_map=None):
        st.subheader(title)
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        
        # Tortas
        so_p = so_data.groupby(group_col)['Cant'].sum().reset_index()
        sd_p = stk_data[stk_data['Cliente_up'].str.contains('DASS')].groupby(group_col)['Cant'].sum().reset_index() if not stk_data.empty else pd.DataFrame()
        sc_p = stk_data[~stk_data['Cliente_up'].str.contains('DASS')].groupby(group_col)['Cant'].sum().reset_index() if not stk_data.empty else pd.DataFrame()

        if not sd_p.empty: c1.plotly_chart(px.pie(sd_p, values='Cant', names=group_col, title="Stk Dass", color=group_col, color_discrete_map=color_map), use_container_width=True)
        if not so_p.empty: c2.plotly_chart(px.pie(so_p, values='Cant', names=group_col, title="Sell Out", color=group_col, color_discrete_map=color_map), use_container_width=True)
        if not sc_p.empty: c3.plotly_chart(px.pie(sc_p, values='Cant', names=group_col, title="Stk Cliente", color=group_col, color_discrete_map=color_map), use_container_width=True)
        
        # Barras
        si_m = si_data.groupby(['Mes', group_col])['Cant'].sum().reset_index()
        if not si_m.empty:
            c4.plotly_chart(px.bar(si_m, x='Mes', y='Cant', color=group_col, title=f"Evolución {group_col}", color_discrete_map=color_map), use_container_width=True)

    # Inyectamos el Maestro a stk_snap para que tenga las categorías antes de graficar
    stk_snap_ma = stk_snap.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left') if not stk_snap.empty else pd.DataFrame()

    render_row("📌 Análisis por Disciplina", so_final, si_final, stk_snap_ma, 'Disciplina', COLOR_MAP_DIS)
    render_row("🏷️ Análisis por Franja Comercial", so_final, si_final, stk_snap_ma, 'FRANJA_PRECIO')
    
    # Fila 3: Ingresos
    st.subheader("🚚 Análisis de Arribos (Ingresos)")
    i1, i2, i3, i4 = st.columns([1, 1, 1, 2])
    ing_dis = si_final.groupby('Disciplina')['Cant'].sum().reset_index()
    ing_fra = si_final.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index()
    
    if not ing_dis.empty: i1.plotly_chart(px.pie(ing_dis, values='Cant', names='Disciplina', title="Ingresos x Dis", color='Disciplina', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not ing_fra.empty: i2.plotly_chart(px.pie(ing_fra, values='Cant', names='FRANJA_PRECIO', title="Ingresos x Franja"), use_container_width=True)
    
    sd_dis = stk_snap_ma[stk_snap_ma['Cliente_up'].str.contains('DASS')].groupby('Disciplina')['Cant'].sum().reset_index() if not stk_snap_ma.empty else pd.DataFrame()
    if not sd_dis.empty: i3.plotly_chart(px.pie(sd_dis, values='Cant', names='Disciplina', title="Stock Dass Part.", color='Disciplina', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    
    si_fra_m = si_final.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index()
    if not si_fra_m.empty: i4.plotly_chart(px.bar(si_fra_m, x='Mes', y='Cant', color='FRANJA_PRECIO', title="Arribos x Franja Mensual"), use_container_width=True)

    # --- 8. TABLA FINAL ---
    st.divider()
    so_sum = so_final.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SO'})
    si_sum = si_final.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SI'})
    sc_sum = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SC'}) if not stk_snap.empty else pd.DataFrame(columns=['SKU', 'SC'])
    sd_sum = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SD'}) if not stk_snap.empty else pd.DataFrame(columns=['SKU', 'SD'])
    
    df_tab = df_ma_filt.merge(so_sum, on='SKU', how='left').merge(si_sum, on='SKU', how='left').merge(sc_sum, on='SKU', how='left').merge(sd_sum, on='SKU', how='left').fillna(0)
    st.dataframe(df_tab[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'SO', 'SI', 'SC', 'SD']].sort_values('SO', ascending=False), use_container_width=True, hide_index=True)
