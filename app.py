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

# --- 1. CONFIGURACIÓN VISUAL ---
COLOR_MAP_DIS = {
    'SPORTSWEAR': '#0055A4', 'RUNNING': '#87CEEB', 'TRAINING': '#FF3131', 
    'HERITAGE': '#00A693', 'KIDS': '#FFB6C1', 'TENNIS': '#FFD700', 
    'SANDALS': '#90EE90', 'OUTDOOR': '#8B4513', 'FOOTBALL': '#000000',
    'SIN CATEGORIA': '#D3D3D3'
}
COLOR_MAP_FRA = {
    'PINNACLE': '#4B0082', 'BEST': '#1E90FF', 'BETTER': '#32CD32', 
    'GOOD': '#FF8C00', 'CORE': '#696969', 'SIN CATEGORIA': '#D3D3D3'
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
            # Leemos el CSV tratando de detectar el separador automáticamente
            df = pd.read_csv(fh, encoding='latin-1', sep=None, engine='python', dtype=str)
            dfs[item['name'].replace('.csv', '')] = df
        return dfs
    except Exception as e:
        st.error(f"Error Drive: {e}"); return {}

data = load_data()

if data:
    # --- 2. PROCESAMIENTO MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma.columns = df_ma.columns.str.strip().str.upper()
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        df_ma = df_ma.drop_duplicates(subset=['SKU'])

    # --- 3. LIMPIEZA DE STOCK (LÓGICA SUMAR.SI F:F; wholesale; B:B) ---
    def clean_stock(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        
        res = pd.DataFrame()
        # Mapeo por posición de columna para ser infalibles
        res['SKU'] = df.iloc[:, 0].astype(str).str.strip().str.upper() # Col A
        res['CANT'] = pd.to_numeric(df.iloc[:, 1], errors='coerce').fillna(0) # Col B
        res['CLIENTE_F'] = df.iloc[:, 5].astype(str).str.strip().str.upper() # Col F
        
        # Intentamos buscar la fecha en la Col D (índice 3)
        res['FECHA_DT'] = pd.to_datetime(df.iloc[:, 3], dayfirst=True, errors='coerce')
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    def clean_sales(name):
        df = data.get(name, pd.DataFrame()).copy()
        if df.empty: return pd.DataFrame()
        df.columns = df.columns.str.strip().str.upper()
        res = pd.DataFrame()
        res['SKU'] = df['SKU'].astype(str).str.strip().str.upper()
        res['CANT'] = pd.to_numeric(df.get('CANTIDAD', df.get('CANT', 0)), errors='coerce').fillna(0)
        col_fecha = next((c for c in df.columns if 'FECHA' in c), None)
        res['FECHA_DT'] = pd.to_datetime(df[col_fecha], dayfirst=True, errors='coerce') if col_fecha else pd.NaT
        res['MES'] = res['FECHA_DT'].dt.strftime('%Y-%m')
        return res

    so_f = clean_sales('Sell_out')
    si_f = clean_sales('Sell_in')
    stk_f = clean_stock('Stock')

    # --- 4. FILTROS Y CRUCE ---
    st.sidebar.header("🔍 Filtros")
    search_query = st.sidebar.text_input("🎯 SKU").upper()
    
    def enrich(df):
        if df.empty: return df
        temp = df.merge(df_ma[['SKU', 'DISCIPLINA', 'FRANJA_PRECIO', 'DESCRIPCION']], on='SKU', how='left')
        temp['DISCIPLINA'] = temp['DISCIPLINA'].fillna('SIN CATEGORIA')
        if search_query:
            temp = temp[temp['SKU'].str.contains(search_query) | temp['DESCRIPCION'].str.contains(search_query, na=False)]
        return temp

    so_f, si_f, stk_f = enrich(so_f), enrich(si_f), enrich(stk_f)

    # --- 5. CÁLCULO DE MÉTRICAS ---
    st.title("📊 Torre de Control Dass v11.38")
    
    max_date = stk_f['FECHA_DT'].max() if not stk_f.empty else None
    stk_snap = stk_f[stk_f['FECHA_DT'] == max_date] if max_date else pd.DataFrame()

    # Aplicación de la fórmula SUMAR.SI(F:F; "wholesale"; B:B)
    val_stk_cli = stk_snap[stk_snap['CLIENTE_F'] == 'WHOLESALE']['CANT'].sum()
    val_stk_dass = stk_snap[stk_snap['CLIENTE_F'].str.contains('DASS', na=False)]['CANT'].sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{so_f['CANT'].sum():,.0f}")
    k2.metric("Sell In", f"{si_f['CANT'].sum():,.0f}")
    k3.metric("Stock Dass", f"{val_stk_dass:,.0f}")
    k4.metric("Stock Cliente", f"{val_stk_cli:,.0f}")

    # --- 6. GRÁFICOS DE DISCIPLINAS ---
    st.divider()
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    
    with c1:
        if val_stk_dass > 0:
            st.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_F'].str.contains('DASS')].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Dass", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c2:
        if not so_f.empty:
            st.plotly_chart(px.pie(so_f.groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Sell Out", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c3:
        if val_stk_cli > 0:
            st.plotly_chart(px.pie(stk_snap[stk_snap['CLIENTE_F'] == 'WHOLESALE'].groupby('DISCIPLINA')['CANT'].sum().reset_index(), values='CANT', names='DISCIPLINA', title="Stock Cliente", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)
    with c4:
        if not si_f.empty:
            df_bar = si_f.groupby(['MES', 'DISCIPLINA'])['CANT'].sum().reset_index()
            # Cálculo de porcentajes para etiquetas
            total_mes = df_bar.groupby('MES')['CANT'].transform('sum')
            df_bar['%'] = (df_bar['CANT'] / total_mes) * 100
            fig = px.bar(df_bar, x='MES', y='CANT', color='DISCIPLINA', title="Sell In (Participación %)", 
                         text=df_bar['%'].apply(lambda x: f'{x:.1f}%'), color_discrete_map=COLOR_MAP_DIS)
            fig.update_traces(textposition='inside')
            fig.update_layout(barmode='stack', yaxis_title="Unidades")
            st.plotly_chart(fig, use_container_width=True)

    # --- 7. TABLA DE DETALLE FINAL ---
    st.divider()
    st.subheader("📋 Detalle de Inventario y Ventas por SKU")
    
    t_so = so_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell Out')
    t_si = si_f.groupby('SKU')['CANT'].sum().reset_index(name='Sell In')
    t_sd = stk_snap[stk_snap['CLIENTE_F'].str.contains('DASS')].groupby('SKU')['CANT'].sum().reset_index(name='Stock Dass')
    t_sc = stk_snap[stk_snap['CLIENTE_F'] == 'WHOLESALE'].groupby('SKU')['CANT'].sum().reset_index(name='Stock Cliente')
    
    df_res = df_ma[['SKU', 'DESCRIPCION', 'DISCIPLINA', 'FRANJA_PRECIO']].merge(t_so, on='SKU', how='left').merge(t_si, on='SKU', how='left').merge(t_sd, on='SKU', how='left').merge(t_sc, on='SKU', how='left').fillna(0)
    
    # Solo mostrar filas con actividad
    df_res = df_res[(df_res['Sell Out'] > 0) | (df_res['Sell In'] > 0) | (df_res['Stock Dass'] > 0) | (df_res['Stock Cliente'] > 0)]
    st.dataframe(df_res.sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("No se pudo cargar la data de Google Drive.")
