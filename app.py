import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Dass Performance v11.38", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL (COLORES) ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3', 'OTRO': '#E5E5E5'
}

COLOR_MAP_FRA = {
    'PINNACLE': '#4B0082', 'BEST': '#1E90FF', 'BETTER': '#32CD32', 
    'GOOD': '#FF8C00', 'CORE': '#696969', 'SIN CATEGORIA': '#D3D3D3'
}

# --- 2. CARGA DE DATOS DESDE GOOGLE DRIVE ---
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
            while not done:
                _, done = downloader.next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            # Normalizar nombres de columnas
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error en conexión Drive: {e}")
        return {}

data = load_data()

# --- 3. PROCESAMIENTO SI HAY DATOS ---
if data:
    # --- MAESTRO DE PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])
        for col, default in {'DISCIPLINA': 'SIN CATEGORIA', 'FRANJA_PRECIO': 'SIN CATEGORIA', 'DESCRIPCION': 'SIN DESCRIPCION'}.items():
            if col not in df_ma.columns: df_ma[col] = default
            df_ma[col] = df_ma[col].fillna(default).astype(str).str.upper()
        df_ma['BUSQUEDA'] = df_ma['SKU'] + " " + df_ma['DESCRIPCION']

    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame(columns=['SKU', 'CANT', 'MES', 'FECHA_DT', 'CLIENTE_UP'])
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT'])), 'CANT')
        df['CANT'] = pd.to_numeric(df.get(col_cant, 0), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), 'FECHA')
        df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
        df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').fillna('S/D').astype(str).str.upper()
        return df

    so_raw = clean_df('Sell_out')
    si_raw = clean_df('Sell_in')
    stk_raw = clean_df('Stock')

    # --- 4. FILTROS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    meses_op = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True) if not so_raw.empty else []
    f_periodo = st.sidebar.selectbox("📅 Mes Principal", ["Todos"] + meses_op)
    
    opts_dis = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_dis = st.sidebar.multiselect("👟 Disciplinas", opts_dis)
    
    opts_fra = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_fra = st.sidebar.multiselect("💰 Franjas de Precio", opts_fra)
    
    f_cli_so = st.sidebar.multiselect("👤 Cliente SO", sorted(so_raw['CLIENTE_UP'].unique()) if not so_raw.empty else [])
    f_cli_si = st.sidebar.multiselect("📦 Cliente SI", sorted(si_raw['CLIENTE_UP'].unique()) if not si_raw.empty else [])
    selected_clients = set(f_cli_so) | set(f_cli_si)

    def apply_logic(df, filter_month=True):
        if df.empty: return df
        temp = df.copy()
        temp = temp.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        for c in ['DISCIPLINA', 'FRANJA_PRECIO']: temp[c] = temp[c].fillna('SIN CATEGORIA')
        if f_dis: temp = temp[temp['DISCIPLINA'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query: 
            temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False) | temp['SKU'].str.contains(search_query, na=False)]
        if filter_month and f_periodo != "Todos":
            temp = temp[temp['MES'] == f_periodo]
        if selected_clients:
            temp = temp[temp['CLIENTE_UP'].isin(selected_clients)]
        return temp

    so_f, si_f, stk_f = apply_logic(so_raw), apply_logic(si_raw), apply_logic(stk_raw)

    # --- 5. DASHBOARD PRINCIPAL ---
    st.title("📊 Torre de Control Dass v11.38")
    max_date = stk_f['FECHA_DT'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['FECHA_DT'] == max_date] if max_date else pd.DataFrame()
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out Seleccionado", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In Seleccionado", f"{si_f['CANT'].sum():,.0f}")
    val_d = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k3.metric("Stock Dass", f"{val_d:,.0f}")
    val_c = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_snap.empty else 0
    k4.metric("Stock Cliente", f"{val_c:,.0f}")

    # --- 6. GRÁFICOS DE MIX ---
    st.divider()
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    if val_d > 0:
        c1.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not so_f.empty:
        c2.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if val_c > 0:
        c3.plotly_chart(px.pie(stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not si_f.empty:
        df_bar_dis = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
        fig_bar_dis = px.bar(df_bar_dis, x='MES', y='CANT', color='DISCIPLINA', title="Evolución Sell In por Disciplina", color_discrete_map=COLOR_MAP_DIS, text_auto='.2s')
        c4.plotly_chart(fig_bar_dis, use_container_width=True)

    # --- 7. TABLA DE DETALLE ---
    st.divider()
    st.subheader("📋 Detalle General por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_stk_d = stk_snap[stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_stk_c = stk_snap[~stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').merge(t_stk_d, on='SKU', how='left').merge(t_si, on='SKU', how='left').fillna(0)
    st.dataframe(df_final[df_final[['Sell Out', 'Stock Cliente', 'Stock Dass', 'Sell In']].sum(axis=1) > 0].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

    # --- 8. SECCIÓN INTELIGENCIA DE RANKINGS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings y Tendencias")
    col_sel1, col_sel2 = st.columns(2)
    with col_sel1:
        mes_act = st.selectbox("Mes Reciente (A)", meses_op, index=0, key="m_a")
    with col_sel2:
        mes_ant = st.selectbox("Mes Anterior (B)", meses_op, index=min(1, len(meses_op)-1), key="m_b")

    # Cálculos de Ranking
    r_a = so_raw[so_raw['MES'] == mes_act].groupby('SKU')['CANT'].sum().reset_index()
    r_b = so_raw[so_raw['MES'] == mes_ant].groupby('SKU')['CANT'].sum().reset_index()
    r_a['Pos_A'] = r_a['CANT'].rank(ascending=False, method='min')
    r_b['Pos_B'] = r_b['CANT'].rank(ascending=False, method='min')

    df_rk = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(r_a[['SKU', 'Pos_A', 'CANT']], on='SKU', how='inner')
    df_rk = df_rk.merge(r_b[['SKU', 'Pos_B']], on='SKU', how='left').fillna({'Pos_B': 999})
    df_rk['Salto'] = df_rk['Pos_B'] - df_rk['Pos_A']

    st.subheader(f"🔥 Líderes de Venta en {mes_act}")
    top_rk = df_rk.sort_values('Pos_A').head(10).copy()
    top_rk['Evolución'] = top_rk['Salto'].apply(lambda x: "🆕 Nuevo" if x > 500 else (f"⬆️ +{int(x)}" if x > 0 else (f"⬇️ {int(x)}" if x < 0 else "➡️ =")))
    st.dataframe(top_rk[['Pos_A', 'SKU', 'DESCRIPCION', 'CANT', 'Evolución']].rename(columns={'Pos_A': 'Pos', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)

    # --- 9. SEMÁFORO DE QUIEBRE (MOS) ---
    st.divider()
    st.subheader("🚨 Alerta de Quiebre: Velocidad vs Cobertura (MOS)")
    
    # Cruce con stock para el semáforo
    df_mos = df_rk.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
    df_mos['Stock_Total'] = df_mos['Stock Dass'] + df_mos['Stock Cliente']
    # Meses de Stock = Stock Total / Venta del mes actual
    df_mos['MOS'] = (df_mos['Stock_Total'] / df_mos['CANT']).replace([float('inf'), -float('inf')], 0).fillna(0)

    def semaforo(row):
        if row['Salto'] >= 5 and row['MOS'] < 1.0 and row['CANT'] > 0: return '🔴 CRÍTICO: < 1 Mes'
        if row['Salto'] > 0 and row['MOS'] < 2.0 and row['CANT'] > 0: return '🟡 ADVERTENCIA: < 2 Meses'
        return '🟢 OK: Stock Suficiente'

    df_mos['Estado'] = df_mos.apply(semaforo, axis=1)
    df_alertas = df_mos[df_mos['Estado'] != '🟢 OK: Stock Suficiente'].sort_values(['Salto', 'MOS'], ascending=[False, True])

    if not df_alertas.empty:
        st.warning(f"Se detectaron {len(df_alertas)} productos con alto crecimiento y baja cobertura.")
        st.dataframe(df_alertas[['Estado', 'SKU', 'DESCRIPCION', 'Salto', 'CANT', 'MOS']].rename(columns={'Salto': 'Puestos Subidos', 'CANT': 'Venta Mes', 'MOS': 'Meses Cobertura'}), use_container_width=True, hide_index=True)
        csv = df_alertas.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Descargar Plan de Reposición", csv, f"reposicion_{mes_act}.csv", "text/csv")
    else:
        st.success("✅ Cobertura mensual saludable para los productos estrella.")

    # Gráfico de Riesgo
    fig_mos = px.scatter(df_mos[df_mos['CANT'] > 0], x='Salto', y='MOS', size='CANT', color='Estado', hover_name='DESCRIPCION', 
                         title="Mapa de Calor: Crecimiento vs Cobertura",
                         color_discrete_map={'🔴 CRÍTICO: < 1 Mes': '#ff4b4b', '🟡 ADVERTENCIA: < 2 Meses': '#ffa500', '🟢 OK: Stock Suficiente': '#28a745'})
    fig_mos.add_hline(y=1.0, line_dash="dot", line_color="red", annotation_text="Peligro: < 1 Mes")
    st.plotly_chart(fig_mos, use_container_width=True)

else:
    st.error("No se pudo cargar la base de datos desde Google Drive.")
