import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v9.7", layout="wide")

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
    # --- 2. MAESTRO ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
    col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
    df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. SELL OUT ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
    so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
    so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
    so_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
    so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
    max_3m = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index().groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})

    # --- 4. STOCK ---
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
    stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
    stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
    stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
    max_f = stk_raw['Fecha_dt'].max()
    stk_actual = stk_raw[stk_raw['Fecha_dt'] == max_f]
    stk_cli = stk_actual[~stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})
    stk_dass = stk_actual[stk_actual['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})

    # --- 5. SELL IN (VENTA MAYORISTA) ---
    si_raw = data.get('Sell_in', pd.DataFrame()).copy()
    si_raw['SKU'] = si_raw['SKU'].astype(str).str.strip().str.upper()
    si_raw['Cant'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
    col_f_si = next((c for c in si_raw.columns if any(x in c.upper() for x in ['FECHA', 'VENTA'])), 'Fecha')
    si_raw['Mes_Venta'] = pd.to_datetime(si_raw[col_f_si], dayfirst=True, errors='coerce').dt.strftime('%Y-%m')
    si_total = si_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In Total'})

    # --- 6. INGRESOS (ARRIBOS) ---
    ing_raw = data.get('Sell_in', pd.DataFrame()).copy() # Usando la misma fuente o 'Ingresos' si existe
    ing_raw['SKU'] = ing_raw['SKU'].astype(str).str.strip().str.upper()
    ing_raw['Cant'] = pd.to_numeric(ing_raw['Unidades'], errors='coerce').fillna(0)
    col_f_ing = next((c for c in ing_raw.columns if any(x in c.upper() for x in ['FECHA_ARRIVO', 'ARRIVO', 'LLEGADA'])), col_f_si)
    ing_raw['Mes_Ingreso'] = pd.to_datetime(ing_raw[col_f_ing], dayfirst=True, errors='coerce').dt.strftime('%Y-%m')
    ing_total = ing_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Ingresos Total'})

    # --- 7. MERGE Y FILTROS ---
    df = df_ma.merge(so_sku, on='SKU', how='left').merge(max_3m, on='SKU', how='left').merge(stk_cli, on='SKU', how='left')
    df = df.merge(stk_dass, on='SKU', how='left').merge(si_total, on='SKU', how='left').merge(ing_total, on='SKU', how='left').fillna(0)

    st.sidebar.header("🔍 Buscador y Filtros")
    search_query = st.sidebar.text_input("🔎 Buscador Universal").upper()
    f_dis = st.sidebar.multiselect("👟 Disciplinas", sorted(df['Disciplina'].unique().tolist()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas", sorted(df['FRANJA_PRECIO'].unique().tolist()))

    if search_query:
        mask = df.apply(lambda row: row.astype(str).str.contains(search_query).any(), axis=1)
        df = df[mask]
    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]

    # --- 8. DASHBOARD ---
    st.title("📊 Torre de Control Dass v9.7")
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out", f"{df['Sell out Total'].sum():,.0f}")
    k2.metric("Sell In", f"{df['Sell In Total'].sum():,.0f}")
    k3.metric("Ingresos", f"{df['Ingresos Total'].sum():,.0f}")
    k4.metric("Stk Cliente", f"{df['Stock Clientes'].sum():,.0f}")
    k5.metric("Stk Dass", f"{df['Stock Dass'].sum():,.0f}")

    def pie_chart(dataframe, val, name, title, target, colors=None):
        sub = dataframe[dataframe[val] > 0]
        if not sub.empty:
            fig = px.pie(sub, values=val, names=name, title=title, color=name, color_discrete_map=colors)
            target.plotly_chart(fig, use_container_width=True)

    # FILA 1: DISCIPLINA
    st.subheader("📌 Análisis por Disciplina")
    d1, d2, d3, d4 = st.columns([1, 1, 1, 2])
    pie_chart(df, 'Stock Dass', 'Disciplina', "Stock Dass", d1, COLOR_MAP_DIS)
    pie_chart(df, 'Sell out Total', 'Disciplina', "Sell Out", d2, COLOR_MAP_DIS)
    pie_chart(df, 'Stock Clientes', 'Disciplina', "Stock Cliente", d3, COLOR_MAP_DIS)
    
    si_dis_grp = si_raw.merge(df_ma[['SKU', 'Disciplina']], on='SKU').groupby(['Mes_Venta', 'Disciplina'])['Cant'].sum().reset_index()
    d4.plotly_chart(px.bar(si_dis_grp, x='Mes_Venta', y='Cant', color='Disciplina', title="Sell In Mensual", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # FILA 2: FRANJA
    st.subheader("🏷️ Análisis por Franja Comercial")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    pie_chart(df, 'Stock Dass', 'FRANJA_PRECIO', "Stock Dass", f1)
    pie_chart(df, 'Sell out Total', 'FRANJA_PRECIO', "Sell Out", f2)
    pie_chart(df, 'Stock Clientes', 'FRANJA_PRECIO', "Stock Cliente", f3)
    
    si_fra_grp = si_raw.merge(df_ma[['SKU', 'FRANJA_PRECIO']], on='SKU').groupby(['Mes_Venta', 'FRANJA_PRECIO'])['Cant'].sum().reset_index()
    f4.plotly_chart(px.bar(si_fra_grp, x='Mes_Venta', y='Cant', color='FRANJA_PRECIO', title="Sell In Mensual"), use_container_width=True)

    # FILA 3: INGRESOS (NUEVA)
    st.subheader("🚚 Análisis de Ingresos (Arribos)")
    i1, i2, i3, i4 = st.columns([1, 1, 1, 2])
    ing_ma = ing_raw.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left')
    
    pie_chart(df, 'Ingresos Total', 'Disciplina', "Ingresos x Dis", i1, COLOR_MAP_DIS)
    pie_chart(df, 'Ingresos Total', 'FRANJA_PRECIO', "Ingresos x Franja", i2)
    # i3 queda libre o para Stock Dass Part.
    pie_chart(df, 'Stock Dass', 'Disciplina', "Stock Dass Part.", i3, COLOR_MAP_DIS)
    
    ing_fra_mes = ing_ma.groupby(['Mes_Ingreso', 'FRANJA_PRECIO'])['Cant'].sum().reset_index()
    i4.plotly_chart(px.bar(ing_fra_mes, x='Mes_Ingreso', y='Cant', color='FRANJA_PRECIO', title="Arribos Mensuales por Franja"), use_container_width=True)

    # --- 9. TABLA ---
    st.divider()
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    st.dataframe(df[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass', 'Sell In Total', 'Ingresos Total']].sort_values('Sell out Total', ascending=False), use_container_width=True, hide_index=True)
