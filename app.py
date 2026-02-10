import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px
from google import genai  # Importante: la nueva librería

# --- CONFIGURACIÓN IA (Ponlo justo después de los imports) ---
if "GEMINI_API_KEY" in st.secrets:
    client = genai.Client(api_key=st.secrets["GEMINI_API_KEY"])
else:
    st.error("⚠️ Falta la GEMINI_API_KEY en los Secrets")

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
        items = results.get('files', [])
        
        if not items:
            st.error("No se encontraron archivos CSV en la carpeta de Google Drive.")
            return {}
            
        dfs = {}
        for item in items:
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            fh.seek(0)
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            
            # Limpieza de nombres de columnas
            df.columns = df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
            
            file_name = item['name'].replace('.csv', '')
            dfs[file_name] = df
        return dfs
    except Exception as e:
        st.error(f"Error al conectar con Google Drive: {e}")
        return {}

data = load_data_from_drive()

if data:
    # --- 3. PROCESAMIENTO DEL MAESTRO ---
    df_maestro = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_maestro.empty:
        df_maestro['SKU'] = df_maestro['SKU'].astype(str).str.strip().str.upper()
        df_maestro = df_maestro.drop_duplicates(subset=['SKU'])
        
        if 'DISCIPLINA' not in df_maestro.columns: df_maestro['DISCIPLINA'] = 'SIN CATEGORIA'
        if 'FRANJA_PRECIO' not in df_maestro.columns: df_maestro['FRANJA_PRECIO'] = 'SIN CATEGORIA'
        if 'DESCRIPCION' not in df_maestro.columns: df_maestro['DESCRIPCION'] = 'SIN DESCRIPCION'
        
        df_maestro['DISCIPLINA'] = df_maestro['DISCIPLINA'].fillna('SIN CATEGORIA').astype(str).str.upper()
        df_maestro['FRANJA_PRECIO'] = df_maestro['FRANJA_PRECIO'].fillna('SIN CATEGORIA').astype(str).str.upper()
        df_maestro['DESCRIPCION'] = df_maestro['DESCRIPCION'].fillna('SIN DESCRIPCION').astype(str).str.upper()
        df_maestro['BUSQUEDA'] = df_maestro['SKU'] + " " + df_maestro['DESCRIPCION']

    # --- 4. LIMPIEZA DE TRANSACCIONALES ---
    def limpiar_transaccional(df_name):
        df = data.get(df_name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        col_cant = next((c for c in df.columns if any(x in c for x in ['UNIDADES', 'CANTIDAD', 'CANT', 'INGRESOS'])), None)
        if col_cant:
            df['CANT'] = pd.to_numeric(df[col_cant], errors='coerce').fillna(0)
        else:
            df['CANT'] = 0
            
        col_fecha = next((c for c in df.columns if any(x in c for x in ['FECHA', 'VENTA', 'ARRIVO', 'MOVIMIENTO'])), None)
        if col_fecha:
            df['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        
        if 'CLIENTE' in df.columns:
            df['CLIENTE_UP'] = df['CLIENTE'].fillna('S/D').astype(str).str.upper()
        else:
            df['CLIENTE_UP'] = 'S/D'
            
        return df

    df_so_raw = limpiar_transaccional('Sell_out')
    df_si_raw = limpiar_transaccional('Sell_in')
    df_stk_raw = limpiar_transaccional('Stock')
    df_ing_raw = limpiar_transaccional('Ingresos') # NUEVA CARGA

    # Snapshot de Stock Actual (Dass vs Clientes)
    if not df_stk_raw.empty:
        max_fecha_stk = df_stk_raw['FECHA_DT'].max()
        df_stk_snap = df_stk_raw[df_stk_raw['FECHA_DT'] == max_fecha_stk].copy()
        df_stk_snap = df_stk_snap.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        df_stk_snap['DISCIPLINA'] = df_stk_snap['DISCIPLINA'].fillna('SIN CATEGORIA')
        df_stk_snap['FRANJA_PRECIO'] = df_stk_snap['FRANJA_PRECIO'].fillna('SIN CATEGORIA')
    else:
        df_stk_snap = pd.DataFrame()

    # --- 5. INTERFAZ DE FILTROS ---
    st.sidebar.header("🔍 Filtros Globales")
    search_query = st.sidebar.text_input("🎯 Buscar SKU o Modelo", "").upper()
    
    meses_disponibles = sorted([str(x) for x in df_so_raw['MES'].dropna().unique()], reverse=True) if not df_so_raw.empty else []
    mes_actual_default = meses_disponibles[0] if meses_disponibles else None
    mes_filtro = st.sidebar.selectbox("📅 Mes de Análisis (KPIs y Mix)", ["Todos"] + meses_disponibles, index=0)

    disciplinas_opts = sorted(list(df_maestro['DISCIPLINA'].unique())) if not df_maestro.empty else []
    f_disciplina = st.sidebar.multiselect("👟 Disciplina", disciplinas_opts)
    
    franjas_opts = sorted(list(df_maestro['FRANJA_PRECIO'].unique())) if not df_maestro.empty else []
    f_franja = st.sidebar.multiselect("💰 Franja de Precio", franjas_opts)

    clientes_so = sorted(df_so_raw['CLIENTE_UP'].unique()) if not df_so_raw.empty else []
    clientes_si = sorted(df_si_raw['CLIENTE_UP'].unique()) if not df_si_raw.empty else []
    f_clientes = st.sidebar.multiselect("👤 Filtrar por Cliente", sorted(list(set(clientes_so) | set(clientes_si))))
# --- TÍTULO PRINCIPAL (Pégalo justo después de cerrar el bloque del sidebar) ---
    st.title("📊 Torre de Control: Sell Out & Abastecimiento")
    # --- ASISTENTE IA EN LA BARRA LATERAL ---
    st.sidebar.divider()
    st.sidebar.subheader("🤖 Consultas Dass IA")

    # Inicializamos la memoria para que no se borre al cambiar filtros
    if 'resultado_lateral' not in st.session_state:
        st.session_state.resultado_lateral = ""

    # Todo este bloque va dentro del sidebar
    with st.sidebar.expander("💬 Consultar a la IA", expanded=False):
        with st.form("form_ia_sidebar"):
            u_q = st.text_input("Pregunta sobre los datos:")
            btn_preguntar = st.form_submit_button("Analizar")
            
            if btn_preguntar and u_q:
                # Contexto rápido
                total_so = df_so_f['CANT'].sum() if not df_so_f.empty else 0
                ctx = f"Ventas Sell Out: {total_so:,.0f}."
                
                try:
                    # Llamada a Gemini
                    response = client.models.generate_content(
                        model="gemini-2.0-flash-lite",
                        contents=f"Analista Dass. Datos: {ctx}. Pregunta: {u_q}"
                    )
                    st.session_state.resultado_lateral = response.text
                except Exception as e:
                    st.error("Error de cuota. Reintenta en 1 min.")

        # Mostramos la respuesta dentro del mismo expander
        if st.session_state.resultado_lateral:
            st.info(st.session_state.resultado_lateral)
            if st.button("Limpiar"):
                st.session_state.resultado_lateral = ""
                st.rerun()
        # --- ASISTENTE IA (RESET DE INDENTACIÓN) ---
st.sidebar.divider()

if 'df_so_f' in locals() or 'df_so_f' in globals():
    with st.sidebar.expander("💬 Consultar a la IA", expanded=False):
        with st.form("form_ia_sidebar"):
            u_q = st.text_input("Pregunta sobre los datos:")
            btn_preguntar = st.form_submit_button("Analizar")
            
            if btn_preguntar and u_q:
                total_so = df_so_f['CANT'].sum() if not df_so_f.empty else 0
                ctx = f"Ventas Sell Out: {total_so:,.0f}."
                
                try:
                    response = client.models.generate_content(
                        model="gemini-2.0-flash-lite",
                        contents=f"Analista Dass. Datos: {ctx}. Pregunta: {u_q}"
                    )
                    st.session_state.resultado_lateral = response.text
                except Exception as e:
                    st.error("Error de cuota. Reintenta en 1 min.")

        if st.session_state.get('resultado_lateral'):
            st.info(st.session_state.resultado_lateral)
    # --- 6. APLICACIÓN DE LÓGICA DE FILTROS ---
    def filtrar_dataframe(df, filtrar_mes=True):
        if df.empty: return df
        temp = df.merge(df_maestro[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION', 'BUSQUEDA']], on='SKU', how='left')
        temp['DISCIPLINA'] = temp['DISCIPLINA'].fillna('SIN CATEGORIA')
        temp['FRANJA_PRECIO'] = temp['FRANJA_PRECIO'].fillna('SIN CATEGORIA')
        
        if f_disciplina: temp = temp[temp['DISCIPLINA'].isin(f_disciplina)]
        if f_franja: temp = temp[temp['FRANJA_PRECIO'].isin(f_franja)]
        if search_query: temp = temp[temp['BUSQUEDA'].str.contains(search_query, na=False)]
        if f_clientes: temp = temp[temp['CLIENTE_UP'].isin(f_clientes)]
        if filtrar_mes and mes_filtro != "Todos": temp = temp[temp['MES'] == mes_filtro]
        return temp

    df_so_f = filtrar_dataframe(df_so_raw)
    df_si_f = filtrar_dataframe(df_si_raw)
    df_ing_f = filtrar_dataframe(df_ing_raw) # NUEVO FILTRO

# --- 7. IA BAJO DEMANDA (Cero consumo automático) ---
    st.divider()
    
    # Usamos session_state para que el resultado no se borre al filtrar otros datos
    if 'resultado_estratégico' not in st.session_state:
        st.session_state.resultado_estratégico = ""

    

    # --- KPIs PRINCIPALES ---
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Sell Out (Pares)", f"{df_so_f['CANT'].sum():,.0f}")
    kpi2.metric("Sell In (Pares)", f"{df_si_f['CANT'].sum():,.0f}")
    kpi3.metric("Ingresos 2025", f"{df_ing_f['CANT'].sum():,.0f}")
    
    # Cálculo de stock basado en el snapshot
    if not df_stk_snap.empty:
        stock_dass = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)]['CANT'].sum()
    else:
        stock_dass = 0
    kpi4.metric("Stock Depósito Dass", f"{stock_dass:,.0f}")
    # --- 8. MIX Y EVOLUCIÓN HISTÓRICA ---
    st.divider()
    col_mix1, col_mix2, col_mix3 = st.columns([1, 1, 2])

    with col_mix1:
        if not df_so_f.empty:
            mix_so = df_so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index()
            fig_mix_so = px.pie(mix_so, values='CANT', names='DISCIPLINA', title="Mix Sell Out", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
            st.plotly_chart(fig_mix_so, use_container_width=True)

    with col_mix2:
        if not df_stk_snap.empty:
            mix_stk = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('DISCIPLINA')['CANT'].sum().reset_index()
            fig_mix_stk = px.pie(mix_stk, values='CANT', names='DISCIPLINA', title="Mix Stock Depósito", color='DISCIPLINA', color_discrete_map=COLOR_MAP_DIS)
            st.plotly_chart(fig_mix_stk, use_container_width=True)

    with col_mix3:
        # Evolución Sell Out vs Sell In vs Ingresos
        evol_so = filtrar_dataframe(df_so_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell Out')
        evol_si = filtrar_dataframe(df_si_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Sell In')
        evol_ing = filtrar_dataframe(df_ing_raw, False).groupby('MES')['CANT'].sum().reset_index(name='Ingresos') # NUEVO
        
        evol_total = evol_so.merge(evol_si, on='MES', how='outer').merge(evol_ing, on='MES', how='outer').fillna(0).sort_values('MES')
        
        fig_evol = go.Figure()
        fig_evol.add_trace(go.Scatter(x=evol_total['MES'], y=evol_total['Ingresos'], name='Ingresos', line=dict(color='#A9A9A9', width=2, dash='dot'))) # NUEVA LÍNEA
        fig_evol.add_trace(go.Scatter(x=evol_total['MES'], y=evol_total['Sell Out'], name='Sell Out', line=dict(color='#0055A4', width=4)))
        fig_evol.add_trace(go.Scatter(x=evol_total['MES'], y=evol_total['Sell In'], name='Sell In', line=dict(color='#FF3131', width=3)))
        fig_evol.update_layout(title="Flujo Logístico: Ingresos vs Sell In vs Sell Out", hovermode='x unified')
        st.plotly_chart(fig_evol, use_container_width=True)

    # --- 9. RANKING DE PRODUCTOS Y TENDENCIAS ---
    st.divider()
    st.header("🏆 Inteligencia de Rankings y Tendencias")
    
    col_sel1, col_sel2 = st.columns(2)
    with col_sel1:
        mes_actual = st.selectbox("Mes de Comparación (A)", meses_disponibles, index=0, key='ma')
    with col_sel2:
        mes_anterior = st.selectbox("Mes Base (B)", meses_disponibles, index=min(1, len(meses_disponibles)-1), key='mb')

    # Lógica de Ranking
    def obtener_ranking(mes):
        df_mes = df_so_raw[df_so_raw['MES'] == mes].groupby('SKU')['CANT'].sum().reset_index()
        df_mes['Posicion'] = df_mes['CANT'].rank(ascending=False, method='min')
        return df_mes

    rk_a = obtener_ranking(mes_actual)
    rk_b = obtener_ranking(mes_anterior)

    df_tendencia = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(rk_a[['SKU', 'Posicion', 'CANT']], on='SKU', how='inner')
    df_tendencia = df_tendencia.merge(rk_b[['SKU', 'Posicion']], on='SKU', how='left', suffixes=('_A', '_B'))
    df_tendencia['Posicion_B'] = df_tendencia['Posicion_B'].fillna(999) # Si no existía, puesto 999
    df_tendencia['Salto'] = df_tendencia['Posicion_B'] - df_tendencia['Posicion_A']

    # Visualización Ranking Top 10
    st.subheader(f"Top 10 Productos con Mayor Venta en {mes_actual}")
    top_10 = df_tendencia.sort_values('Posicion_A').head(10).copy()
    
    def format_salto(val):
        if val > 500: return "🆕 Nuevo"
        if val > 0: return f"⬆️ +{int(val)}"
        if val < 0: return f"⬇️ {int(val)}"
        return "➡️ ="

    top_10['Tendencia'] = top_10['Salto'].apply(format_salto)
    st.dataframe(top_10[['Posicion_A', 'SKU', 'DESCRIPCION', 'CANT', 'Tendencia']].rename(columns={'Posicion_A': 'Puesto', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)

    # --- 10. EXPLORADOR POR DISCIPLINA ---
    st.divider()
    st.subheader("👟 Explorador Táctico por Disciplina")
    disciplina_foc = st.selectbox("Seleccione Disciplina para análisis profundo:", disciplinas_opts)
    
    df_dis_foc = df_tendencia[df_tendencia['DISCIPLINA'] == disciplina_foc].copy()
    df_dis_foc['Posicion_Cat'] = df_dis_foc['CANT'].rank(ascending=False, method='min')
    
    col_dis1, col_dis2 = st.columns([2, 1])
    with col_dis1:
        st.write(f"**Top 10 en {disciplina_foc}:**")
        df_dis_foc_show = df_dis_foc.sort_values('Posicion_Cat').head(10)
        st.dataframe(df_dis_foc_show[['Posicion_Cat', 'SKU', 'DESCRIPCION', 'CANT']].rename(columns={'Posicion_Cat': 'Puesto Cat', 'CANT': 'Pares'}), use_container_width=True, hide_index=True)
    with col_dis2:
        st.metric(f"Venta Total {disciplina_foc}", f"{df_dis_foc['CANT'].sum():,.0f} prs")
        fig_bar_dis = px.bar(df_dis_foc_show.head(5), x='CANT', y='SKU', orientation='h', title="Top 5 Volumen", color_discrete_sequence=[COLOR_MAP_DIS.get(disciplina_foc, '#000')])
        st.plotly_chart(fig_bar_dis, use_container_width=True)

    # --- 11. ALERTAS DE QUIEBRE Y COBERTURA (MOS) ---
    st.divider()
    st.header("🚨 Alerta de Quiebre y Cobertura (MOS)")
    
    # Unificar datos para MOS
    stk_dass_group = df_stk_snap[df_stk_snap['CLIENTE_UP'].str.contains('DASS', na=False)].groupby('SKU')['CANT'].sum().reset_index(name='Stock_Dass')
    df_alerta = df_tendencia.merge(stk_dass_group, on='SKU', how='left').fillna(0)
    
    # MOS Proyectado: Stock / Venta del mes actual
    df_alerta['MOS_Proyectado'] = (df_alerta['Stock_Dass'] / df_alerta['CANT']).replace([float('inf'), -float('inf')], 0).fillna(0)

    def definir_semaforo_mensual(row):
        if row['Salto'] >= 5 and row['MOS_Proyectado'] < 1.0 and row['CANT'] > 0:
            return '🔴 CRÍTICO: < 1 Mes'
        elif row['Salto'] > 0 and row['MOS_Proyectado'] < 2.0 and row['CANT'] > 0:
            return '🟡 ADVERTENCIA: < 2 Meses'
        else:
            return '🟢 OK: Stock Suficiente'

    df_alerta['Estado'] = df_alerta.apply(definir_semaforo_mensual, axis=1)
    df_riesgo = df_alerta[df_alerta['Estado'] != '🟢 OK: Stock Suficiente'].sort_values(['Salto', 'MOS_Proyectado'], ascending=[False, True])

    if not df_riesgo.empty:
        st.warning(f"Se detectaron {len(df_riesgo)} productos en riesgo de quiebre.")
        st.dataframe(df_riesgo[['Estado', 'SKU', 'DESCRIPCION', 'DISCIPLINA', 'Salto', 'CANT', 'MOS_Proyectado']].rename(columns={'Salto': 'Puestos Subidos', 'CANT': 'Venta Mes', 'MOS_Proyectado': 'Meses Stock'}), use_container_width=True, hide_index=True)
        csv = df_riesgo.to_csv(index=False).encode('utf-8')
        st.download_button(label="📥 Descargar Lista de Reposición (CSV)", data=csv, file_name=f'reposicion_{mes_actual}.csv', mime='text/csv')
    else:
        st.success("✅ Cobertura mensual saludable para los productos en crecimiento.")

    fig_mos = px.scatter(
        df_alerta[df_alerta['CANT'] > 0], 
        x='Salto', y='MOS_Proyectado', 
        size='CANT', color='Estado', 
        hover_name='DESCRIPCION',
        title="Mapa de Velocidad vs Cobertura (MOS)",
        color_discrete_map={'🔴 CRÍTICO: < 1 Mes': '#ff4b4b', '🟡 ADVERTENCIA: < 2 Meses': '#ffa500', '🟢 OK: Stock Suficiente': '#28a745'}
    )
    st.plotly_chart(fig_mos, use_container_width=True)

    # --- 12. TABLA MAESTRA DETALLADA ---
    st.divider()
    st.subheader("📋 Detalle Maestro de Productos (Consolidado)")
    
    # Agrupamos SI e Ingresos para la tabla final
    res_si = df_si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_In')
    res_ing = df_ing_f.groupby('SKU')['CANT'].sum().reset_index(name='Ingresos')
    res_so = df_so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell_Out')
    res_stk = df_stk_snap.groupby('SKU')['CANT'].sum().reset_index(name='Stock_Total')

    df_final = df_maestro[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(res_so, on='SKU', how='left')
    df_final = df_final.merge(res_si, on='SKU', how='left')
    df_final = df_final.merge(res_ing, on='SKU', how='left')
    df_final = df_final.merge(res_stk, on='SKU', how='left').fillna(0)

    st.dataframe(df_final.sort_values('Sell_Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("No se pudieron cargar los datos. Verifique la carpeta de Drive.")















































