import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v9.3", layout="wide")

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
    # --- 2. PROCESAMIENTO MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
    col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
    df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. SELL OUT (SUMA PURA) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
    so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
    so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
    so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
    
    so_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
    so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
    max_3m = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})

    # --- 4. STOCK (ÚLTIMA FECHA) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
    stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
    stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
    stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
    
    max_f = stk_raw['Fecha_dt'].max()
    stk_actual = stk_raw[stk_raw['Fecha_dt'] == max_f]
    
    stk_cli = stk_actual[~stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    stk_dass = stk_actual[stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})

    # --- 5. INGRESOS (SELL IN) ---
    si_raw = data.get('Sell_in', pd.DataFrame()).copy()
    si_raw['SKU'] = si_raw['SKU'].astype(str).str.strip().str.upper()
    si_raw['Cant'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
    si_raw['Fecha_Arrivo'] = pd.to_datetime(si_raw['Fecha_Arrivo'], dayfirst=True, errors='coerce')
    si_raw['Mes_Arrivo'] = si_raw['Fecha_Arrivo'].dt.strftime('%Y-%m')
    si_total = si_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In Total'})

    # --- 6. UNIÓN Y BUSCADOR UNIVERSAL ---
    df = df_ma.merge(so_sku, on='SKU', how='left').merge(max_3m, on='SKU', how='left')
    df = df.merge(stk_cli, on='SKU', how='left').merge(stk_dass, on='SKU', how='left')
    df = df.merge(si_total, on='SKU', how='left').fillna(0)

    st.sidebar.header("🔍 Buscador y Filtros")
    # El buscador universal ahora está arriba
    search_query = st.sidebar.text_input("🔎 Buscador Universal (SKU, Desc, Franja, etc.)").upper()
    
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df['Disciplina'].unique().tolist()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(df['FRANJA_PRECIO'].unique().tolist()))

    # LÓGICA DE FILTRADO MULTI-COLUMNA
    if search_query:
        mask = (
            df['SKU'].str.contains(search_query, na=False) |
            df['Descripcion'].str.contains(search_query, na=False) |
            df['Disciplina'].str.contains(search_query, na=False) |
            df['FRANJA_PRECIO'].str.contains(search_query, na=False)
        )
        df = df[mask]

    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]

    # --- 7. DASHBOARD ---
    st.title("📊 Torre de Control Dass v9.3")
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out Total", f"{df['Sell out Total'].sum():,.0f}")
    k2.metric("Stock Clientes (Actual)", f"{df['Stock Clientes'].sum():,.0f}")
    k3.metric("Stock Dass (Actual)", f"{df['Stock Dass'].sum():,.0f}")
    k4.metric("Sell In Total", f"{df['Sell In Total'].sum():,.0f}")

    # --- GRAFICOS (FILA 1 Y 2) ---
    def pie_chart(dataframe, val, name, title, target, colors=None):
        sub = dataframe[dataframe[val] > 0]
        if not sub.empty:
            fig = px.pie(sub, values=val, names=name, title=title, color=name, color_discrete_map=colors)
            target.plotly_chart(fig, use_container_width=True)

    st.subheader("📌 Análisis por Disciplina e Ingresos")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    pie_chart(df, 'Stock Dass', 'Disciplina', "Stock Dass", c1, COLOR_MAP_DIS)
    pie_chart(df, 'Sell out Total', 'Disciplina', "Sell Out", c2, COLOR_MAP_DIS)
    pie_chart(df, 'Stock Clientes', 'Disciplina', "Stock Cliente", c3, COLOR_MAP_DIS)
    
    si_dis = si_raw.merge(df_ma[['SKU', 'Disciplina']], on='SKU', how='left')
    si_dis_grp = si_dis.groupby(['Mes_Arrivo', 'Disciplina'])['Cant'].sum().reset_index()
    fig_bar_dis = px.bar(si_dis_grp, x='Mes_Arrivo', y='Cant', color='Disciplina', title="Ingresos x Mes", color_discrete_map=COLOR_MAP_DIS)
    c4.plotly_chart(fig_bar_dis, use_container_width=True)

    st.subheader("🏷️ Análisis por Franja e Ingresos")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    pie_chart(df, 'Stock Dass', 'FRANJA_PRECIO', "Stock Dass", f1)
    pie_chart(df, 'Sell out Total', 'FRANJA_PRECIO', "Sell Out", f2)
    pie_chart(df, 'Stock Clientes', 'FRANJA_PRECIO', "Stock Cliente", f3)
    
    si_fra = si_raw.merge(df_ma[['SKU', 'FRANJA_PRECIO']], on='SKU', how='left')
    si_fra_grp = si_fra.groupby(['Mes_Arrivo', 'FRANJA_PRECIO'])['Cant'].sum().reset_index()
    fig_bar_fra = px.bar(si_fra_grp, x='Mes_Arrivo', y='Cant', color='FRANJA_PRECIO', title="Ingresos x Franja")
    f4.plotly_chart(fig_bar_fra, use_container_width=True)

    # --- 8. TABLA DE DETALLE ---
    st.divider()
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    
    st.dataframe(
        df[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass', 'Sell In Total']]
        .sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 'Stock Clientes': '{:,.0f}', 
            'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}', 'Sell In Total': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
