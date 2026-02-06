import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

st.set_page_config(page_title="Dass Performance v11.12", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL ---
COLOR_MAP_DIS = {'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000'}

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
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col, default in {'Disciplina': 'OTRO', 'FRANJA_PRECIO': 'SIN CAT', 'Descripcion': 'SIN DESCRIPCION'}.items():
            if col not in df_ma.columns: df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        df_ma['Busqueda'] = df_ma['SKU'] + " " + df_ma['Descripcion']

    # --- 3. LIMPIEZA DE TRANSACCIONES ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c.upper() for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'Cant')
        df['Cant'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), 'Fecha')
        df['Fecha_dt'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['Mes'] = df['Fecha_dt'].dt.strftime('%Y-%m')
        df['Cliente_up'] = df.get('Cliente', '').fillna('').astype(str).str.upper()
        return df[['SKU', 'Cant', 'Mes', 'Fecha_dt', 'Cliente_up']]

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 4. SIDEBAR (FILTROS) ---
    st.sidebar.header("🔍 Filtros de Cadena")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + sorted(list(set(so_raw['Mes'].dropna())), reverse=True))
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df_ma['Disciplina'].unique()))
    
    # Filtros de Clientes
    f_cli_so = st.sidebar.multiselect("👤 Clientes Sell Out", sorted(so_raw['Cliente_up'].unique()))
    f_cli_si = st.sidebar.multiselect("📦 Clientes Sell In", sorted(si_raw['Cliente_up'].unique()))

    # --- 5. LÓGICA DE FILTRADO CRUZADO (LA CLAVE) ---
    def apply_logic(df, mode=None, filter_month=True):
        temp = df.copy()
        # 1. Filtro de SKU/Disciplina (Afecta a todo siempre)
        m_filtered = df_ma.copy()
        if f_dis: m_filtered = m_filtered[m_filtered['Disciplina'].isin(f_dis)]
        if search_query: m_filtered = m_filtered[m_filtered['Busqueda'].str.contains(search_query, na=False)]
        temp = temp[temp['SKU'].isin(m_filtered['SKU'])]
        
        # 2. Filtro de Mes
        if filter_month and f_periodo != "Todos":
            temp = temp[temp['Mes'] == f_periodo]
        
        # 3. Filtro de Cliente (Cruza información entre tablas)
        # Si filtramos por un cliente de Facturación, queremos ver su Sell Out y su Stock también
        selected_clients = set(f_cli_so) | set(f_cli_si)
        
        if selected_clients:
            # Si el DF tiene columna de cliente y NO es stock de Dass, filtramos por cliente
            if 'Cliente_up' in temp.columns:
                # El stock de DASS nunca se filtra por cliente externo
                is_dass_stock = temp['Cliente_up'].str.contains('DASS', na=False).all() if not temp.empty else False
                if not is_dass_stock:
                    temp = temp[temp['Cliente_up'].isin(selected_clients)]
        
        return temp.merge(df_ma[['SKU', 'Disciplina', 'Descripcion']], on='SKU', how='left')

    so_f = apply_logic(so_raw, filter_month=True)
    si_f = apply_logic(si_raw, filter_month=True)
    stk_f = apply_logic(stk_raw, filter_month=True)

    # --- 6. DASHBOARD ---
    st.title("📊 Torre de Control Dass v11.12")
    
    max_date = stk_f['Fecha_dt'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['Fecha_dt'] == max_date] if max_date else pd.DataFrame()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out (Venta)", f"{so_f['Cant'].sum():,.0f}")
    k2.metric("Sell In (Factura)", f"{si_f['Cant'].sum():,.0f}")
    # El Stock Dass solo responde a SKU/Disciplina/Mes, no a Clientes externos
    stk_dass_total = stk_raw[stk_raw['Cliente_up'].str.contains('DASS', na=False)]
    # Aplicamos filtro de SKU/Disciplina manualmente para el Stock Dass
    m_filt_skus = df_ma[df_ma['Disciplina'].isin(f_dis)]['SKU'] if f_dis else df_ma['SKU']
    if search_query: m_filt_skus = df_ma[df_ma['Busqueda'].str.contains(search_query, na=False)]['SKU']
    stk_dass_val = stk_dass_total[stk_dass_total['SKU'].isin(m_filt_skus)]['Cant'].sum()
    
    k3.metric("Stock Dass", f"{stk_dass_val:,.0f}")
    k4.metric("Stock Cliente", f"{stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)]['Cant'].sum():,.0f}")

    # --- 7. GRÁFICOS ---
    st.subheader("📌 Análisis por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    if not stk_snap.empty:
        c1.plotly_chart(px.pie(stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
        c3.plotly_chart(px.pie(stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not so_f.empty:
        c2.plotly_chart(px.pie(so_f.groupby('Disciplina')['Cant'].sum().reset_index(), values='Cant', names='Disciplina', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not si_f.empty:
        c4.plotly_chart(px.bar(si_f.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In (Facturación)", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # --- 8. LÍNEA DE TIEMPO 4D ---
    st.divider()
    st.subheader("📈 Evolución Histórica Sincronizada")
    so_h = apply_logic(so_raw, filter_month=False).groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out'})
    si_h = apply_logic(si_raw, filter_month=False).groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In'})
    stk_h_raw = apply_logic(stk_raw, filter_month=False)
    sd_h = stk_h_raw[stk_h_raw['Cliente_up'].str.contains('DASS', na=False)].groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
    sc_h = stk_h_raw[~stk_h_raw['Cliente_up'].str.contains('DASS', na=False)].groupby('Mes')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Cliente'})
    df_h = so_h.merge(si_h, on='Mes', how='outer').merge(sd_h, on='Mes', how='outer').merge(sc_h, on='Mes', how='outer').fillna(0).sort_values('Mes')
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Stock Dass'], name='Stock Dass', line=dict(color='#00A693', width=2)))
    fig.add_trace(go.Scatter(x=df_h['Mes'], y=df_h['Stock Cliente'], name='Stock Cliente', line=dict(color='#FFD700', width=2)))
    st.plotly_chart(fig, use_container_width=True)

    # --- 9. TABLA DE DETALLE (TODAS LAS COLUMNAS) ---
    st.divider()
    st.subheader("📋 Detalle Analítico por SKU")
    
    # Cálculos para la tabla respetando los filtros cruzados
    t_so = so_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out Total'})
    t_si = si_f.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In Total'})
    t_stk_d = stk_snap[stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
    t_stk_c = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS', na=False)].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Cliente'})
    
    # Max Mensual 3M dinámico
    so_hist_f = apply_logic(so_raw, filter_month=False)
    if not so_hist_f.empty:
        meses_3 = sorted(so_hist_f['Mes'].unique())[-3:]
        t_max = so_hist_f[so_hist_f['Mes'].isin(meses_3)].groupby(['SKU', 'Mes'])['Cant'].sum().reset_index()
        t_max = t_max.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
    else: t_max = pd.DataFrame(columns=['SKU', 'Max_Mensual_3M'])

    df_final = df_ma[['SKU', 'Descripcion', 'Disciplina']].merge(t_so, on='SKU', how='left').merge(t_max, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').merge(t_stk_d, on='SKU', how='left').merge(t_si, on='SKU', how='left').fillna(0)
    df_final['MOS'] = (df_final['Stock Cliente'] / df_final['Max_Mensual_3M']).replace([float('inf'), -float('inf')], 0).fillna(0)
    
    # Mostrar solo lo que tiene movimiento bajo los filtros actuales
    df_final = df_final[(df_final['Sell Out Total'] > 0) | (df_final['Stock Cliente'] > 0) | (df_final['Stock Dass'] > 0) | (df_final['Sell In Total'] > 0)]

    st.dataframe(df_final.sort_values('Sell Out Total', ascending=False), use_container_width=True, hide_index=True)
