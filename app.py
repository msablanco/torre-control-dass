import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v10.6", layout="wide")

# --- 1. CONFIGURACIÓN GLOBAL ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131',
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700',
    'TENIS': '#FFD700', 'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513',
    'FOOTBALL': '#000000', 'FUTBOL': '#000000'
}

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
    # --- 2. PROCESAMIENTO MAESTRO (Base de todo) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    df_ma = df_ma.drop_duplicates(subset=['SKU'])
    
    col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
    col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
    df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. PROCESAMIENTO TRANSACCIONES (Optimizado) ---
    def clean_trans(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        df['Cant'] = pd.to_numeric(df.get('Unidades', df.get('Cantidad', 0)), errors='coerce').fillna(0)
        c_f = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[c_f], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        return df

    so_raw = clean_trans('Sell_out')
    si_raw = clean_trans('Sell_in')
    stk_raw = clean_trans('Stock')
    if not stk_raw.empty:
        stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

    # --- 4. SIDEBAR ---
    st.sidebar.header("🔍 Filtros")
    raw_months = sorted(list(set(so_raw['Mes'].dropna()) | set(si_raw['Mes'].dropna())), 
                        key=lambda x: pd.to_datetime(x, format='%Y-%m'), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Periodo", ["Todos"] + raw_months)
    search_sku = st.sidebar.text_input("🔎 Buscador (SKU/Desc)").upper()

    # Filtros dinámicos (Solo disciplinas con datos reales)
    skus_activos = set(so_raw['SKU']) | set(si_raw['SKU'])
    maestro_act = df_ma[df_ma['SKU'].isin(skus_activos)]
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(maestro_act['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(maestro_act['FRANJA_PRECIO'].unique()))

    # --- 5. LÓGICA DE FILTRADO ---
    def apply_filters(df_target, is_maestro=False):
        t = df_target.copy()
        if t.empty: return t
        if not is_maestro:
            if f_periodo != "Todos": t = t[t['Mes'] == f_periodo]
            t = t.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')
        if f_dis: t = t[t['Disciplina'].isin(f_dis)]
        if f_fra: t = t[t['FRANJA_PRECIO'].isin(f_fra)]
        if search_sku:
            t = t[t.apply(lambda r: r.astype(str).str.contains(search_sku).any(), axis=1)]
        return t

    so_f = apply_filters(so_raw)
    si_f = apply_filters(si_raw)
    stk_f = apply_filters(stk_raw)
    df_ma_filt = apply_filters(df_ma, is_maestro=True)

    # --- 6. DASHBOARD ---
    st.title(f"📊 Torre de Control Dass v10.6")

    # Pico Histórico (Solo si hay búsqueda)
    if search_sku and len(search_sku) > 3:
        hist_so = so_raw[so_raw['SKU'].str.contains(search_sku)].groupby('Mes')['Cant'].sum().reset_index()
        if not hist_so.empty:
            st.subheader(f"📈 Tendencia: {search_sku}")
            m1, m2 = st.columns([1, 3])
            mes_pico = hist_so.loc[hist_so['Cant'].idxmax()]
            m1.metric("Pico de Venta", mes_pico['Mes'], f"{mes_pico['Cant']:,.0f} u.")
            m2.plotly_chart(px.line(hist_so.sort_values('Mes'), x='Mes', y='Cant', markers=True, height=250), use_container_width=True)
            st.divider()

    # KPIs
    max_f = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_f] if max_f else pd.DataFrame()
    k_so, k_si = so_f['Cant'].sum(), si_f['Cant'].sum()
    k_sc = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum() if not stk_snap.empty else 0
    k_sd = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum() if not stk_snap.empty else 0

    c_k1, c_k2, c_k3, c_k4 = st.columns(4)
    c_k1.metric("Sell Out", f"{k_so:,.0f}")
    c_k2.metric("Sell In", f"{k_si:,.0f}")
    c_k3.metric("Stk Cliente", f"{k_sc:,.0f}")
    c_k4.metric("Stk Dass", f"{k_sd:,.0f}")

    # --- 7. LAS 3 FILAS DE GRÁFICOS (12 GRÁFICOS) ---
    def render_row(title, so_df, si_df, stk_df, group_col, color_map=None):
        st.subheader(title)
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        
        # Validar existencia de columna
        for d in [so_df, si_df, stk_df]:
            if not d.empty and group_col not in d.columns: return

        if not stk_df.empty:
            sd_p = stk_df[stk_df['Cliente_up'].str.contains('DASS')].groupby(group_col)['Cant'].sum().reset_index()
            sc_p = stk_df[~stk_df['Cliente_up'].str.contains('DASS')].groupby(group_col)['Cant'].sum().reset_index()
            if not sd_p.empty: c1.plotly_chart(px.pie(sd_p, values='Cant', names=group_col, title="Stk Dass", color=group_col, color_discrete_map=color_map), use_container_width=True)
            if not sc_p.empty: c3.plotly_chart(px.pie(sc_p, values='Cant', names=group_col, title="Stk Cliente", color=group_col, color_discrete_map=color_map), use_container_width=True)

        if not so_df.empty:
            so_p = so_df.groupby(group_col)['Cant'].sum().reset_index()
            c2.plotly_chart(px.pie(so_p, values='Cant', names=group_col, title="Sell Out", color=group_col, color_discrete_map=color_map), use_container_width=True)
        
        si_m = si_df.groupby(['Mes', group_col])['Cant'].sum().reset_index()
        if not si_m.empty:
            c4.plotly_chart(px.bar(si_m, x='Mes', y='Cant', color=group_col, title=f"Sell In Mensual", color_discrete_map=color_map), use_container_width=True)

    # Inyectar categorías al Stock
    stk_snap_ma = stk_snap.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left') if not stk_snap.empty else pd.DataFrame()

    render_row("📌 Análisis por Disciplina", so_f, si_f, stk_snap_ma, 'Disciplina', COLOR_MAP_DIS)
    render_row("🏷️ Análisis por Franja Comercial", so_f, si_f, stk_snap_ma, 'FRANJA_PRECIO')
    
    # Fila 3: Arribos
    st.subheader("🚚 Análisis de Arribos")
    i1, i2, i3, i4 = st.columns([1, 1, 1, 2])
    if not si_f.empty:
        i1.plotly_chart(px.pie(si_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Arribos x Dis", color='Disciplina', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        i2.plotly_chart(px.pie(si_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Arribos x Franja"), use_container_width=True)
        # Reutilizamos Stock Dass para la tercera torta
        if not stk_snap_ma.empty:
            sd_dis = stk_snap_ma[stk_snap_ma['Cliente_up'].str.contains('DASS')].groupby('Disciplina')['Cant'].sum().reset_index()
            i3.plotly_chart(px.pie(sd_dis, values='Cant', names='Disciplina', title="Stk Dass Part.", color='Disciplina', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        i4.plotly_chart(px.bar(si_f.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Arribos x Franja"), use_container_width=True)

    # --- 8. TABLA ---
    st.divider()
    res = so_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SO'})
    si_sum = si_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SI'})
    df_tab = df_ma_filt.merge(res, on='SKU', how='left').merge(si_sum, on='SKU', how='left').fillna(0)
    st.dataframe(df_tab[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'SO', 'SI']].sort_values('SO', ascending=False), use_container_width=True, hide_index=True)
