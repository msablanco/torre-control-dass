import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.graph_objects as go
import plotly.express as px

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Fila - Performance & Inventory", layout="wide")

# --- 1. CARGA DE DATOS ---
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
            while done is False:
                status, done = downloader.next_chunk()
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
    # --- 2. FUNCIÓN DE LIMPIEZA ROBUSTA ---
    def clean_df(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: 
            return pd.DataFrame(columns=['SKU', 'CANT', 'FECHA_DT', 'MES', 'CLIENTE_UP'])
        
        df['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        
        # Cantidad
        col_c = next((c for c in df.columns if any(x in c for x in ['UNID', 'CANT', 'QTY'])), None)
        df['CANT'] = pd.to_numeric(df[col_c], errors='coerce').fillna(0) if col_c else 0
        
        # Fecha (Forzamos detección de Fecha de Arrivo o similares)
        col_f = next((c for c in df.columns if any(x in c for x in ['FECHA', 'ARRIVO', 'ETA', 'VENTA'])), None)
        if col_f:
            df['FECHA_DT'] = pd.to_datetime(df[col_f], dayfirst=True, errors='coerce')
            df['MES'] = df['FECHA_DT'].dt.strftime('%Y-%m')
        else:
            df['MES'] = "S/D" # Evita el KeyError
            
        df['CLIENTE_UP'] = df.get('CLIENTE', 'S/D').astype(str).str.upper()
        return df

    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()

    so_raw = clean_df('Sell_out')
    stk_raw = clean_df('Stock')
    ing_raw = clean_df('ingresos')

    # --- 3. SIDEBAR ---
    st.sidebar.header("Filtros")
    meses_op = sorted(list(set(so_raw['MES'].dropna().unique()) | set(stk_raw['MES'].dropna().unique())), reverse=True)
    f_mes = st.sidebar.selectbox("Mes de Análisis", meses_op if meses_op else ["S/D"])
    f_dis = st.sidebar.multiselect("Disciplina", sorted(df_ma['DISCIPLINA'].unique().astype(str)) if 'DISCIPLINA' in df_ma.columns else [])
    search = st.sidebar.text_input("Buscar SKU").upper()

    # --- 4. PROCESAMIENTO ---
    ing_mes_act = ing_raw[ing_raw['MES'] == f_mes].groupby('SKU')['CANT'].sum().reset_index(name='ING_REALIZADO')
    ing_futuro_full = ing_raw[ing_raw['MES'] > f_mes].copy()
    ing_futuro_agg = ing_futuro_full.groupby('SKU')['CANT'].sum().reset_index(name='ING_FUTURO')

    so_f = so_raw[so_raw['MES'] == f_mes]
    stk_f = stk_raw[stk_raw['MES'] == f_mes]
    stk_f['TIPO'] = stk_f['CLIENTE_UP'].apply(lambda x: 'DASS' if 'DASS' in str(x) else 'CLIENTE')

    if f_dis:
        skus_dis = df_ma[df_ma['DISCIPLINA'].isin(f_dis)]['SKU']
        so_f = so_f[so_f['SKU'].isin(skus_dis)]
        stk_f = stk_f[stk_f['SKU'].isin(skus_dis)]

    # --- 5. KPIs ---
    st.title(f"📊 Dashboard Performance - {f_mes}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Venta Mes", f"{so_f['CANT'].sum():,.0f}")
    c2.metric("Stock Total", f"{stk_f['CANT'].sum():,.0f}")
    c3.metric("Ingresos Mes", f"{ing_mes_act['ING_REALIZADO'].sum():,.0f}")
    c4.metric("Ingresos Futuros", f"{ing_futuro_agg['ING_FUTURO'].sum():,.0f}")

    # --- 6. EVOLUCIÓN (HISTÓRICO) ---
    st.subheader("📈 3. Evolución Histórica")
    h_so = so_raw.groupby('MES')['CANT'].sum().reset_index()
    h_ing = ing_raw.groupby('MES')['CANT'].sum().reset_index() if not ing_raw.empty else pd.DataFrame(columns=['MES', 'CANT'])
    
    fig_h = go.Figure()
    fig_h.add_trace(go.Scatter(x=h_so['MES'], y=h_so['CANT'], name="Venta", line=dict(color="#0055A4", width=3)))
    if not h_ing.empty:
        fig_h.add_trace(go.Bar(x=h_ing['MES'], y=h_ing['CANT'], name="Ingresos", marker_color="#FF3131", opacity=0.4))
    st.plotly_chart(fig_h, use_container_width=True)

    # --- 7. TABLA MAESTRA ---
    st.divider()
    st.subheader("📋 6-7. Detalle y Cobertura")
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='VENTA')
    t_stk = stk_f.groupby(['SKU', 'TIPO'])['CANT'].sum().unstack(fill_value=0).reset_index()
    for col in ['CLIENTE', 'DASS']: 
        if col not in t_stk.columns: t_stk[col] = 0
    
    df_m = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(t_so, on='SKU', how='left')
    df_m = df_m.merge(t_stk[['SKU', 'CLIENTE', 'DASS']], on='SKU', how='left')
    df_m = df_m.merge(ing_futuro_agg, on='SKU', how='left').fillna(0)
    df_m['COBERTURA'] = (df_m['CLIENTE'] / df_m['VENTA']).replace([float('inf')], 99).fillna(0)

    st.dataframe(df_m.sort_values('VENTA', ascending=False), use_container_width=True, hide_index=True)
    # --- 8. GRÁFICOS: TORTAS ---
    st.divider()
    t1, t2, t3 = st.columns(3)
    # Torta 1: Venta por Disciplina
    so_dis = so_f.merge(df_ma[['SKU', 'DISCIPLINA']], on='SKU').groupby('DISCIPLINA')['CANT'].sum().reset_index()
    with t1: st.plotly_chart(px.pie(so_dis, values='CANT', names='DISCIPLINA', title="Venta por Disciplina"), use_container_width=True)
    
    # Torta 2: Venta por Franja
    so_fra = so_f.merge(df_ma[['SKU', 'FRANJA_PRECIO']], on='SKU').groupby('FRANJA_PRECIO')['CANT'].sum().reset_index()
    with t2: st.plotly_chart(px.pie(so_fra, values='CANT', names='FRANJA_PRECIO', title="Venta por Franja"), use_container_width=True)
    
    # Torta 3: Stock Dass vs Clientes
    stk_f['TIPO'] = stk_f['CLIENTE_UP'].apply(lambda x: 'DASS' if 'DASS' in str(x) else 'CLIENTE')
    stk_tipo = stk_f.groupby('TIPO')['CANT'].sum().reset_index()
    with t3: st.plotly_chart(px.pie(stk_tipo, values='CANT', names='TIPO', title="Distribución Stock"), use_container_width=True)

    # --- 9. TABLA DE DETALLE (PUNTO 6 Y 7) ---
    st.divider()
    st.subheader("Detalle de SKUs, Cobertura e Ingresos")
    
    # Agrupamos datos para la gran tabla
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='VENTA')
    t_stk = stk_f.groupby(['SKU', 'TIPO'])['CANT'].sum().unstack(fill_value=0).reset_index()
    t_stk.columns = ['SKU', 'STK_CLIENTE', 'STK_DASS'] if 'DASS' in t_stk.columns else ['SKU', 'STK_CLIENTE']
    
    df_final = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA']].merge(t_so, on='SKU', how='left')
    df_final = df_final.merge(t_stk, on='SKU', how='left')
    df_final = df_final.merge(ing_mes_act, on='SKU', how='left')
    df_final = df_final.merge(ing_futuros, on='SKU', how='left')
    df_final = df_final.fillna(0)
    
    # Cálculo Cobertura
    df_final['COBERTURA'] = (df_final['STK_CLIENTE'] / df_final['VENTA']).replace([float('inf')], 99).fillna(0)

    def style_cob(v):
        color = '#FFB3B3' if v < 1.5 and v > 0 else '#B3FFB3' if v >= 1.5 and v <= 3 else '#FFFFB3' if v > 3 else ''
        return f'background-color: {color}'

    st.dataframe(df_final.sort_values('VENTA', ascending=False).style.applymap(style_cob, subset=['COBERTURA']), use_container_width=True, hide_index=True)

    # --- 10. RANKING Y COMPARATIVA (PUNTO 9) ---
    st.divider()
    st.subheader("Ranking de Ventas y Saltos")
    m_ant = meses[min(1, len(meses)-1)]
    
    def get_rk(m):
        return so_raw[so_raw['MES'] == m].groupby('SKU')['CANT'].sum().rank(ascending=False)

    rk_table = df_ma[['SKU', 'DESCRIPCION']].copy()
    rk_table['Pos_Actual'] = rk_table['SKU'].map(get_rk(f_mes)).fillna(999)
    rk_table['Pos_Anterior'] = rk_table['SKU'].map(get_rk(m_ant)).fillna(999)
    rk_table['Salto'] = rk_table['Pos_Anterior'] - rk_table['Pos_Actual']
    
    st.write(f"Comparativo Posiciones: {f_mes} vs {m_ant}")
    st.dataframe(rk_table.sort_values('Pos_Actual').head(15), use_container_width=True, hide_index=True)

  # --- 11. QUIEBRES ---
    st.divider()
    st.subheader("⚠️ 11. SKUs en Quiebre")
    quiebres = df_m[(df_m['VENTA'] > 0) & (df_m['CLIENTE'] == 0)]
    st.dataframe(quiebres[['SKU', 'DESCRIPCION', 'VENTA', 'ING_FUTURO']], use_container_width=True, hide_index=True)

    # --- 12. CRONOGRAMA INGRESOS ---
    st.divider()
    st.subheader("🗓️ 12. Cronograma de Ingresos Futuros")
    if not ing_futuro_full.empty:
        st.dataframe(ing_futuro_full.groupby('MES')['CANT'].sum().reset_index(), use_container_width=True, hide_index=True)
    else:
        st.write("No hay ingresos programados posteriores a este mes.")

    # --- 13. RESUMEN EJECUTIVO ---
    st.divider()
    st.subheader("🏢 13. Resumen Ejecutivo")
    total_v = df_m['VENTA'].sum()
    st.info(f"Resumen del periodo: Venta total de {total_v:,.0f} unidades con un stock en clientes de {df_m['CLIENTE'].sum():,.0f}.")

