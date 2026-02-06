import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.express as px

st.set_page_config(page_title="Dass Performance v10.9", layout="wide")

# --- 1. CONFIGURACIÓN DE COLORES ---
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
    # --- 2. MAESTRO (Limpieza y Unicidad) ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    df_ma = df_ma.drop_duplicates(subset=['SKU'])
    
    col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
    col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
    df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. PROCESAMIENTO DE TRANSACCIONES ---
    def process_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return df
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        df['Cant'] = pd.to_numeric(df.get('Unidades', df.get('Cantidad', 0)), errors='coerce').fillna(0)
        c_f = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[c_f], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        # Inyectar categorías del Maestro de una vez para evitar KeyErrors luego
        return df.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')

    so_all = process_df('Sell_out')
    si_all = process_df('Sell_in')
    stk_all = process_df('Stock')
    if not stk_all.empty:
        stk_all['Cliente_up'] = stk_all.get('Cliente', '').fillna('').astype(str).str.upper()

    # --- 4. SIDEBAR ---
    st.sidebar.header("🔍 Control de Panel")
    search_query = st.sidebar.text_input("🔎 Buscador Universal (SKU / Nombre)").upper()
    
    raw_months = sorted(list(set(so_all['Mes'].dropna()) | set(si_all['Mes'].dropna())), 
                        key=lambda x: pd.to_datetime(x, format='%Y-%m'), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Mes de Análisis", ["Todos"] + raw_months)
    
    f_dis = st.sidebar.multiselect("👟 Disciplina", sorted(df_ma['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("🏷️ Franja", sorted(df_ma['FRANJA_PRECIO'].unique()))

    # --- 5. LÓGICA DE FILTRADO ---
    def apply_filters(df):
        if df.empty: return df
        temp = df
        if f_periodo != "Todos": temp = temp[temp['Mes'] == f_periodo]
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query:
            temp = temp[temp.apply(lambda r: r.astype(str).str.contains(search_query).any(), axis=1)]
        return temp

    so_f = apply_filters(so_all)
    si_f = apply_filters(si_all)
    stk_f = apply_filters(stk_all)

    # --- 6. ANÁLISIS HISTÓRICO (Línea de Tiempo) ---
    st.title(f"📊 Torre de Control Dass v10.9")
    
    if search_query and len(search_query) > 2:
        # Buscamos en so_all (histórico completo) pero solo el producto buscado
        hist_sku = so_all[so_all.apply(lambda r: r.astype(str).str.contains(search_query).any(), axis=1)]
        if not hist_sku.empty:
            st.subheader(f"📈 Evolución Histórica de Ventas")
            hist_plot = hist_sku.groupby('Mes')['Cant'].sum().reset_index().sort_values('Mes')
            pico = hist_plot.loc[hist_plot['Cant'].idxmax()]
            
            m1, m2 = st.columns([1, 3])
            m1.metric("Pico Histórico", pico['Mes'], f"{pico['Cant']:,.0f} u.")
            m2.plotly_chart(px.line(hist_plot, x='Mes', y='Cant', markers=True, height=250), use_container_width=True)
            st.divider()

    # KPIs
    max_f = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_f] if max_f else pd.DataFrame()
    
    k_so, k_si = so_f['Cant'].sum(), si_f['Cant'].sum()
    k_sc = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum() if not stk_snap.empty else 0
    k_sd = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')]['Cant'].sum() if not stk_snap.empty else 0

    col_k1, col_k2, col_k3, col_k4 = st.columns(4)
    col_k1.metric("Sell Out", f"{k_so:,.0f}")
    col_k2.metric("Sell In", f"{k_si:,.0f}")
    col_k3.metric("Stock Cliente", f"{k_sc:,.0f}")
    col_k4.metric("Stock Dass", f"{k_sd:,.0f}")

    # --- 7. LAS 3 FILAS DE GRÁFICOS (12 GRÁFICOS TOTAL) ---
    def render_row(title, so_data, si_data, stk_data, group_col, color_map=None):
        st.subheader(title)
        c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
        
        # Pie Charts
        def safe_pie(df, target_col, title_pie, target_tab):
            if not df.empty and group_col in df.columns:
                fig_data = df.groupby(group_col)['Cant'].sum().reset_index()
                if not fig_data.empty and fig_data['Cant'].sum() > 0:
                    fig = px.pie(fig_data, values='Cant', names=group_col, title=title_pie, 
                                 color=group_col, color_discrete_map=color_map)
                    target_tab.plotly_chart(fig, use_container_width=True)

        safe_pie(stk_data[stk_data['Cliente_up'].str.contains('DASS')] if not stk_data.empty else pd.DataFrame(), group_col, "Stock Dass", c1)
        safe_pie(so_data, group_col, "Sell Out", c2)
        safe_pie(stk_data[~stk_data['Cliente_up'].str.contains('DASS')] if not stk_data.empty else pd.DataFrame(), group_col, "Stock Cliente", c3)
        
        # Bar Chart
        if not si_data.empty and group_col in si_data.columns:
            si_m = si_data.groupby(['Mes', group_col])['Cant'].sum().reset_index()
            fig_bar = px.bar(si_m, x='Mes', y='Cant', color=group_col, title="Sell In Mensual", color_discrete_map=color_map)
            c4.plotly_chart(fig_bar, use_container_width=True)

    render_row("📌 Análisis por Disciplina", so_f, si_f, stk_snap, 'Disciplina', COLOR_MAP_DIS)
    render_row("🏷️ Análisis por Franja Comercial", so_f, si_f, stk_snap, 'FRANJA_PRECIO')
    
    # Fila 3: Arribos
    st.subheader("🚚 Análisis de Arribos e Ingresos")
    i1, i2, i3, i4 = st.columns([1, 1, 1, 2])
    if not si_f.empty:
        i1.plotly_chart(px.pie(si_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Ingresos x Dis", color='Disciplina', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        i2.plotly_chart(px.pie(si_f.groupby('FRANJA_PRECIO')['Cant'].sum().reset_index(), values='Cant', names='FRANJA_PRECIO', title="Ingresos x Franja"), use_container_width=True)
        if not stk_snap.empty:
            sd_dis = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')].groupby('Disciplina')['Cant'].sum().reset_index()
            i3.plotly_chart(px.pie(sd_dis, values='Cant', names='Disciplina', title="Stock Dass Part.", color='Disciplina', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        i4.plotly_chart(px.bar(si_f.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Arribos Mensuales x Franja"), use_container_width=True)

    # --- 8. TABLA DE DETALLE ---
    st.divider()
    st.subheader("📋 Detalle de Inventario y Ventas")
    # Consolidamos la tabla final
    t_so = so_f.groupby(['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO'])['Cant'].sum().reset_index().rename(columns={'Cant': 'SO'})
    t_si = si_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'SI'})
    t_st = stk_snap.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock'})
    
    df_final = t_so.merge(t_si, on='SKU', how='outer').merge(t_st, on='SKU', how='outer').fillna(0)
    st.dataframe(df_final.sort_values('SO', ascending=False), use_container_width=True, hide_index=True)
