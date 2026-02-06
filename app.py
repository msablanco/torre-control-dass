import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v9.5", layout="wide")

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
    # --- 2. MAESTRO DE PRODUCTOS ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
        df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
        col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
        df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. SELL OUT (VENTA MINORISTA) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    df_so_sku = pd.DataFrame(columns=['SKU', 'Sell out Total', 'Max_Mensual_3M'])
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        df_so_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        max_3m = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        df_so_sku = df_so_sku.merge(max_3m, on='SKU', how='left')

    # --- 4. STOCK (ÚLTIMA FOTO) ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    df_stk_cli = pd.DataFrame(columns=['SKU', 'Stock Clientes'])
    df_stk_dass = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
        max_f = stk_raw['Fecha_dt'].max()
        stk_actual = stk_raw[stk_raw['Fecha_dt'] == max_f]
        df_stk_cli = stk_actual[~stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
        df_stk_dass = stk_actual[stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})

    # --- 5. SELL IN (VENTA MAYORISTA - CORREGIDO) ---
    si_raw = data.get('Sell_in', pd.DataFrame()).copy()
    df_si_sku = pd.DataFrame(columns=['SKU', 'Sell In Total'])
    if not si_raw.empty:
        si_raw['SKU'] = si_raw['SKU'].astype(str).str.strip().str.upper()
        si_raw['Cant'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        # La fecha en Sell In es la fecha de facturación/venta
        col_fecha_si = next((c for c in si_raw.columns if any(x in c.upper() for x in ['FECHA', 'VENTA', 'FACTURA'])), None)
        if col_fecha_si:
            si_raw['Fecha_Fact_dt'] = pd.to_datetime(si_raw[col_fecha_si], dayfirst=True, errors='coerce')
            si_raw['Mes_Venta'] = si_raw['Fecha_Fact_dt'].dt.strftime('%Y-%m')
        else:
            si_raw['Mes_Venta'] = "SIN FECHA"
        df_si_sku = si_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In Total'})

    # --- 6. UNIÓN Y BUSCADOR UNIVERSAL ---
    df = df_ma.merge(df_so_sku, on='SKU', how='left').merge(df_stk_cli, on='SKU', how='left').merge(df_stk_dass, on='SKU', how='left').merge(df_si_sku, on='SKU', how='left').fillna(0)

    st.sidebar.header("🔍 Buscador y Filtros")
    search_query = st.sidebar.text_input("🔎 Buscador (SKU, Desc, Franja, etc.)").upper()
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df['Disciplina'].unique().tolist()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(df['FRANJA_PRECIO'].unique().tolist()))

    if search_query:
        mask = df.apply(lambda row: row.astype(str).str.contains(search_query).any(), axis=1)
        df = df[mask]
    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]

    # --- 7. DASHBOARD ---
    st.title("📊 Torre de Control Dass v9.5")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Venta Minorista (Sell Out)", f"{df['Sell out Total'].sum():,.0f}")
    k2.metric("Venta Mayorista (Sell In)", f"{df['Sell In Total'].sum():,.0f}")
    k3.metric("Stock Clientes (Actual)", f"{df['Stock Clientes'].sum():,.0f}")
    k4.metric("Stock Dass (Actual)", f"{df['Stock Dass'].sum():,.0f}")

    def pie_chart(dataframe, val, name, title, target, colors=None):
        sub = dataframe[dataframe[val] > 0]
        if not sub.empty:
            fig = px.pie(sub, values=val, names=name, title=title, color=name, color_discrete_map=colors)
            target.plotly_chart(fig, use_container_width=True)

    # --- FILA 1: DISCIPLINA ---
    st.subheader("📌 Análisis por Disciplina y Evolución Sell In")
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    pie_chart(df, 'Stock Dass', 'Disciplina', "Stock Dass", c1, COLOR_MAP_DIS)
    pie_chart(df, 'Sell out Total', 'Disciplina', "Sell Out", c2, COLOR_MAP_DIS)
    pie_chart(df, 'Stock Clientes', 'Disciplina', "Stock Cliente", c3, COLOR_MAP_DIS)
    
    if not si_raw.empty:
        si_dis = si_raw.merge(df_ma[['SKU', 'Disciplina']], on='SKU', how='left')
        si_dis_grp = si_dis.groupby(['Mes_Venta', 'Disciplina'])['Cant'].sum().reset_index()
        c4.plotly_chart(px.bar(si_dis_grp, x='Mes_Venta', y='Cant', color='Disciplina', title="Venta Mayorista (Sell In) por Mes", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # --- FILA 2: FRANJA ---
    st.subheader("🏷️ Análisis por Franja y Evolución Sell In")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    pie_chart(df, 'Stock Dass', 'FRANJA_PRECIO', "Stock Dass", f1)
    pie_chart(df, 'Sell out Total', 'FRANJA_PRECIO', "Sell Out", f2)
    pie_chart(df, 'Stock Clientes', 'FRANJA_PRECIO', "Stock Cliente", f3)
    
    if not si_raw.empty:
        si_fra = si_raw.merge(df_ma[['SKU', 'FRANJA_PRECIO']], on='SKU', how='left')
        si_fra_grp = si_fra.groupby(['Mes_Venta', 'FRANJA_PRECIO'])['Cant'].sum().reset_index()
        f4.plotly_chart(px.bar(si_fra_grp, x='Mes_Venta', y='Cant', color='FRANJA_PRECIO', title="Venta Mayorista (Sell In) por Franja"), use_container_width=True)

    # --- 8. TABLA ---
    st.divider()
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    st.dataframe(
        df[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass', 'Sell In Total']]
        .sort_values('Sell out Total', ascending=False).style.format({'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}', 'Sell In Total': '{:,.0f}'})
        .map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
