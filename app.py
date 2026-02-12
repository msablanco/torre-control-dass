import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Performance & Inteligencia => Fila Calzado", layout="wide")

# --- 1. CONFIGURACIÓN VISUAL (MAPAS DE COLORES CONSISTENTES) ---
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

# --- 2. CARGA DE DATOS ---
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
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}")
        return {}

data = load_data()

if data:
    # --- 3. PROCESAMIENTO INICIAL ---
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

    so_raw, si_raw, stk_raw = clean_df('Sell_out'), clean_df('Sell_in'), clean_df('Stock')

    # --- 4. LÓGICA DE STOCK SNAPSHOT (CORRECCIÓN CLAVE) ---
    if not stk_raw.empty:
        max_date_stk = stk_raw['FECHA_DT'].max()
        # Tomamos la última foto del stock independientemente del filtro de mes
        stk_snap = stk_raw[stk_raw['FECHA_DT'] == max_date_stk].copy()
        # Inyectamos el Maestro de Productos al Stock para evitar KeyErrors en los Mix
        stk_snap = stk_snap.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        for c in ['DISCIPLINA', 'FRANJA_PRECIO']: stk_snap[c] = stk_snap[c].fillna('SIN CATEGORIA')
    else:
        stk_snap = pd.DataFrame()

    # --- 5. FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 SKU / Descripción").upper()
    meses_op = sorted([str(x) for x in so_raw['MES'].dropna().unique()], reverse=True) if not so_raw.empty else []
    f_periodo = st.sidebar.selectbox("📅 Mes", ["Todos"] + meses_op)
    
    opts_dis = sorted([str(x) for x in df_ma['DISCIPLINA'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_dis = st.sidebar.multiselect("👟 Disciplinas", opts_dis)
    opts_fra = sorted([str(x) for x in df_ma['FRANJA_PRECIO'].unique()]) if not df_ma.empty else ["SIN CATEGORIA"]
    f_fra = st.sidebar.multiselect("💰 Franjas", opts_fra)
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

    so_f, si_f = apply_logic(so_raw), apply_logic(si_raw)

# --- 6. PROCESAMIENTO DINÁMICO DE STOCK (WHolesale) ---
    # Creamos la versión filtrada del stock que responda a la Sidebar
    if not stk_snap.empty:
        # Filtramos el stock según los SKUs resultantes de los filtros de Disciplina/Franja/Búsqueda
        # Usamos so_f como referencia de SKUs válidos tras filtros
        skus_validos = df_ma.copy()
        if f_dis: skus_validos = skus_validos[skus_validos['DISCIPLINA'].isin(f_dis)]
        if f_fra: skus_validos = skus_validos[skus_validos['FRANJA_PRECIO'].isin(f_fra)]
        if search_query:
             skus_validos = skus_validos[skus_validos['BUSQUEDA'].str.contains(search_query, na=False)]
        
        stk_f = stk_snap[stk_snap['SKU'].isin(skus_validos['SKU'])].copy()
    else:
        stk_f = pd.DataFrame()

    # Calculamos totales para validación de gráficos
    val_d = stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_f.empty else 0
    val_c = stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum() if not stk_f.empty else 0

    # --- 6b. VISUALIZACIÓN DE STOCK (Solo si hay datos) ---
    if (val_d + val_c) > 0:
        st.divider()
        st.subheader("📦 Stock en Clientes (Wholesale)")
        col_st1, col_st2 = st.columns(2)

        with col_st1:
            stk_dis_g = stk_f.groupby('DISCIPLINA')['CANT'].sum().reset_index()
            fig_stk_dis = px.bar(stk_dis_g, x='DISCIPLINA', y='CANT', title="Stock por Disciplina",
                                 color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
            st.plotly_chart(fig_stk_dis, use_container_width=True)

        with col_st2:
            stk_fra_g = stk_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index()
            fig_stk_fra = px.bar(stk_fra_g, x='FRANJA_PRECIO', y='CANT', title="Stock por Franja",
                                 color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA)
            st.plotly_chart(fig_stk_fra, use_container_width=True)

    # --- 7. ANÁLISIS DE MIX (PIES) ---
    st.divider()
    st.subheader("📌 Análisis de Mix por Disciplina")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    
    if val_d > 0:
        c1.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                               values='CANT', names='DISCIPLINA', title="Stock Dass", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if not so_f.empty:
        c2.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                               values='CANT', names='DISCIPLINA', title="Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    if val_c > 0:
        c3.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index(), 
                               values='CANT', names='DISCIPLINA', title="Stock Cliente", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    
    if not si_f.empty:
        df_bar_dis = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
        fig_bar_dis = px.bar(df_bar_dis, x='MES', y='CANT', color='DISCIPLINA', title="Sell In (Mix)", color_discrete_map=COLOR_MAP_DIS, text_auto='.2s')
        c4.plotly_chart(fig_bar_dis, use_container_width=True)

    # --- 8. ANÁLISIS POR FRANJA ---
    st.subheader("💰 Análisis por Franja de Precio")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    
    if val_d > 0:
        f1.plotly_chart(px.pie(stk_f[stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                               values='CANT', names='FRANJA_PRECIO', title="Stock Dass (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    if not so_f.empty:
        f2.plotly_chart(px.pie(so_f.groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                               values='CANT', names='FRANJA_PRECIO', title="Sell Out (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    if val_c > 0:
        f3.plotly_chart(px.pie(stk_f[~stk_f['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('FRANJA_PRECIO')['CANT'].sum().reset_index(), 
                               values='CANT', names='FRANJA_PRECIO', title="Stock Cliente (Franja)", color='FRANJA_PRECIO', color_discrete_map=COLOR_MAP_FRA), use_container_width=True)
    
    if not si_f.empty:
        df_bar_fra = si_f.groupby(['MES', 'FRANJA_PRECIO'])['CANT'].sum().reset_index()
        fig_bar_fra = px.bar(df_bar_fra, x='MES', y='CANT', color='FRANJA_PRECIO', title="Sell In (Mix)", color_discrete_map=COLOR_MAP_FRA, text_auto='.2s')
        f4.plotly_chart(fig_bar_fra, use_container_width=True)

    # --- 9. EVOLUCIÓN HISTÓRICA ---
    st.divider()
    st.subheader("📈 Evolución Histórica Comparativa")
    h_so = apply_logic(so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
    h_si = apply_logic(si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell In')
    
    df_h = h_so.merge(h_si, on='MES', how='outer').fillna(0).sort_values('MES')
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
    fig_h.add_trace(go.Scatter(x=df_h['MES'], y=df_h['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3, dash='dot')))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 10. DETALLE POR SKU ---
    st.divider()
    st.subheader("📋 Detalle por SKU")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_stk_f = stk_f.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Actual')
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_stk_f, on='SKU', how='left').merge(t_si, on='SKU', how='left').fillna(0)
    df_final = df_final[(df_final['Sell Out'] > 0) | (df_final['Stock_Actual'] > 0) | (df_final['Sell In'] > 0)]
    st.dataframe(df_final.sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

    # --- 11. RANKINGS ---
    if len(meses_op) >= 2:
        st.divider()
        st.header("🏆 Inteligencia de Rankings")
        mes_actual = meses_op[0]
        mes_anterior = meses_op[1]

        rank_a = so_raw[so_raw['MES'] == mes_actual].groupby('SKU')['CANT'].sum().reset_index()
        rank_b = so_raw[so_raw['MES'] == mes_anterior].groupby('SKU')['CANT'].sum().reset_index()
        rank_a['Puesto_A'] = rank_a['CANT'].rank(ascending=False, method='min')
        rank_b['Puesto_B'] = rank_b['CANT'].rank(ascending=False, method='min')

        df_rank = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rank_a[['SKU', 'Puesto_A', 'CANT']], on='SKU', how='inner')
        df_rank = df_rank.merge(rank_b[['SKU', 'Puesto_B']], on='SKU', how='left').fillna({'Puesto_B': 999})
        df_rank['Salto'] = df_rank['Puesto_B'] - df_rank['Puesto_A']

        st.subheader(f"🔥 Top 10 Ventas {mes_actual}")
        st.dataframe(df_rank.sort_values('Puesto_A').head(10), use_container_width=True, hide_index=True)

    # --- 12. EXPLORADOR TÁCTICO POR DISCIPLINA ---
    st.divider()
    st.subheader("👟 Explorador Táctico por Disciplina")
    disciplinas_disponibles = sorted(df_rank['DISCIPLINA'].unique())
    disciplina_select = st.selectbox("Seleccioná una Disciplina para profundizar:", disciplinas_disponibles)
    df_rank_dis = df_rank[df_rank['DISCIPLINA'] == disciplina_select].copy()
    df_rank_dis['Pos_Categoría'] = df_rank_dis['CANT'].rank(ascending=False, method='min')

    col_l1, col_l2 = st.columns([2, 1])
    with col_l1:
        st.markdown(f"**Top 10 de {disciplina_select}**")
        df_dis_show = df_rank_dis.sort_values('Pos_Categoría').head(10).copy()
        df_dis_show['Evolución'] = df_dis_show['Salto'].apply(lambda x: "🔥 Nuevo" if x > 500 else (f"🔼 +{int(x)}" if x > 0 else (f"🔽 {int(x)}" if x < 0 else "⏺️ =")))
        st.dataframe(df_dis_show[['Pos_Categoría', 'SKU', 'DESCRIPCION', 'CANT', 'Evolución']], use_container_width=True, hide_index=True)
    with col_l2:
        st.metric(f"Total {disciplina_select}", f"{df_rank_dis['CANT'].sum():,.0f}")
        fig_mini = px.bar(df_dis_show.head(5), x='CANT', y='SKU', orientation='h', color_discrete_sequence=[COLOR_MAP_DIS.get(disciplina_select, '#0055A4')], text_auto='.2s')
        fig_mini.update_layout(height=250, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
        st.plotly_chart(fig_mini, use_container_width=True)

    # --- 13. ALERTA DE QUIEBRE Y MOS ---
    st.divider()
    st.subheader("🚨 Alerta de Quiebre: Velocidad vs Cobertura Mensual (MOS)")
    df_alerta = df_rank.merge(t_stk_d, on='SKU', how='left').merge(t_stk_c, on='SKU', how='left').fillna(0)
    df_alerta['Stock_Total'] = df_alerta['Stock Dass'] + df_alerta['Stock Cliente']
    df_alerta['MOS_Proyectado'] = (df_alerta['Stock_Total'] / df_alerta['CANT']).replace([float('inf'), -float('inf')], 0).fillna(0)

    def definir_semaforo_mensual(row):
        if row['Salto'] >= 5 and row['MOS_Proyectado'] < 1.0 and row['CANT'] > 0: return '🔴 CRÍTICO: < 1 Mes'
        elif row['Salto'] > 0 and row['MOS_Proyectado'] < 2.0 and row['CANT'] > 0: return '🟡 ADVERTENCIA: < 2 Meses'
        else: return '🟢 OK: Stock Suficiente'

    df_alerta['Estado'] = df_alerta.apply(definir_semaforo_mensual, axis=1)
    df_riesgo = df_alerta[df_alerta['Estado'] != '🟢 OK: Stock Suficiente'].sort_values(['Salto', 'MOS_Proyectado'], ascending=[False, True])

    if not df_riesgo.empty:
        st.warning(f"Se detectaron {len(df_riesgo)} productos en riesgo de quiebre.")
        st.dataframe(df_riesgo[['Estado', 'SKU', 'DESCRIPCION', 'DISCIPLINA', 'Salto', 'CANT', 'MOS_Proyectado']].rename(columns={'Salto': 'Puestos Subidos', 'CANT': 'Venta Mes', 'MOS_Proyectado': 'Meses Stock'}), use_container_width=True, hide_index=True)
        csv = df_riesgo.to_csv(index=False).encode('utf-8')
        st.download_button(label="📥 Descargar Lista de Reposición (CSV)", data=csv, file_name=f'reposicion_{mes_actual}.csv', mime='text/csv')

    fig_mos = px.scatter(df_alerta[df_alerta['CANT'] > 0], x='Salto', y='MOS_Proyectado', size='CANT', color='Estado', hover_name='DESCRIPCION', title="Mapa de Velocidad vs Cobertura (MOS)", color_discrete_map={'🔴 CRÍTICO: < 1 Mes': '#ff4b4b', '🟡 ADVERTENCIA: < 2 Meses': '#ffa500', '🟢 OK: Stock Suficiente': '#28a745'})
    fig_mos.add_hline(y=1.0, line_dash="dot", line_color="red", annotation_text="Peligro: < 1 Mes")
    fig_mos.add_hline(y=2.0, line_dash="dot", line_color="orange", annotation_text="Alerta: < 2 Meses")
    st.plotly_chart(fig_mos, use_container_width=True)

else:
    st.error("No se detectaron archivos o hay un error en la conexión con Google Drive.")

















