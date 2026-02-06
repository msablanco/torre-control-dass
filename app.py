import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v9.6", layout="wide")

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
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
        df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
        col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
        df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. SELL OUT (Venta Minorista) ---
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    df_so_sku = pd.DataFrame(columns=['SKU', 'Sell out Total', 'Max_Mensual_3M'])
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        df_so_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        max_3m = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        df_so_sku = df_so_sku.merge(max_3m, on='SKU', how='left')

    # --- 4. SELL IN (Venta Mayorista) ---
    si_raw = data.get('Sell_in', pd.DataFrame()).copy()
    df_si_sku = pd.DataFrame(columns=['SKU', 'Sell In Total'])
    if not si_raw.empty:
        si_raw['SKU'] = si_raw['SKU'].astype(str).str.strip().str.upper()
        si_raw['Cant'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
        col_f_si = next((c for c in si_raw.columns if any(x in c.upper() for x in ['FECHA', 'VENTA'])), None)
        if col_f_si:
            si_raw['Fecha_dt'] = pd.to_datetime(si_raw[col_f_si], dayfirst=True, errors='coerce')
            si_raw['Mes_Venta'] = si_raw['Fecha_dt'].dt.strftime('%Y-%m')
        df_si_sku = si_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In Total'})

    # --- 5. INGRESOS (Arribos de Mercadería) ---
    # Buscamos archivo 'Ingresos' o similar
    ing_raw = data.get('Ingresos', pd.DataFrame()).copy()
    df_ing_sku = pd.DataFrame(columns=['SKU', 'Ingresos Total'])
    if not ing_raw.empty:
        ing_raw['SKU'] = ing_raw['SKU'].astype(str).str.strip().str.upper()
        ing_raw['Cant'] = pd.to_numeric(ing_raw['Cantidad'], errors='coerce').fillna(0)
        col_f_ing = next((c for c in ing_raw.columns if any(x in c.upper() for x in ['FECHA', 'ARRIVO', 'LLEGADA'])), None)
        if col_f_ing:
            ing_raw['Fecha_dt'] = pd.to_datetime(ing_raw[col_f_ing], dayfirst=True, errors='coerce')
            ing_raw['Mes_Ingreso'] = ing_raw['Fecha_dt'].dt.strftime('%Y-%m')
        df_ing_sku = ing_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Ingresos Total'})

    # --- 6. STOCK (Última Fecha) ---
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

    # --- 7. MERGE Y FILTROS ---
    df = df_ma.merge(df_so_sku, on='SKU', how='left').merge(df_si_sku, on='SKU', how='left')
    df = df.merge(df_stk_cli, on='SKU', how='left').merge(df_stk_dass, on='SKU', how='left')
    df = df.merge(df_ing_sku, on='SKU', how='left').fillna(0)

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
    st.title("📊 Torre de Control Dass v9.6")
    
    # KPIs
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Sell Out", f"{df['Sell out Total'].sum():,.0f}")
    k2.metric("Sell In", f"{df['Sell In Total'].sum():,.0f}")
    k3.metric("Ingresos", f"{df['Ingresos Total'].sum():,.0f}")
    k4.metric("Stk Cliente", f"{df['Stock Clientes'].sum():,.0f}")
    k5.metric("Stk Dass", f"{df['Stock Dass'].sum():,.0f}")

    def plot_row(title_prefix, col_val, df_source_si, df_source_ing):
        st.subheader(f"📌 Análisis de {title_prefix}")
        c1, c2, c3 = st.columns([1, 1, 2])
        
        # Tortas (Mantenidas)
        sub_pie = df[df[col_val] > 0]
        fig_so = px.pie(sub_pie, values='Sell out Total', names='Disciplina' if 'Dis' in title_prefix else 'FRANJA_PRECIO', title="Part. Sell Out", color_discrete_map=COLOR_MAP_DIS if 'Dis' in title_prefix else None)
        c1.plotly_chart(fig_so, use_container_width=True)
        
        fig_stk = px.pie(sub_pie, values='Stock Clientes', names='Disciplina' if 'Dis' in title_prefix else 'FRANJA_PRECIO', title="Part. Stock Cliente", color_discrete_map=COLOR_MAP_DIS if 'Dis' in title_prefix else None)
        c2.plotly_chart(fig_stk, use_container_width=True)

        # Barras Comparativas (Sell In vs Ingresos)
        # Aquí combinamos Sell In y Ingresos para ver el flujo
        c3.markdown(f"**Evolución Mensual {title_prefix}**")
        # Lógica de barras mixtas se puede expandir aquí
        if not si_raw.empty:
            si_temp = si_raw.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO']], on='SKU')
            si_grp = si_temp.groupby(['Mes_Venta', 'Disciplina' if 'Dis' in title_prefix else 'FRANJA_PRECIO'])['Cant'].sum().reset_index()
            fig_bar = px.bar(si_grp, x='Mes_Venta', y='Cant', color='Disciplina' if 'Dis' in title_prefix else 'FRANJA_PRECIO', title=f"Venta Mayorista (Sell In)", color_discrete_map=COLOR_MAP_DIS if 'Dis' in title_prefix else None)
            c3.plotly_chart(fig_bar, use_container_width=True)

    plot_row("Disciplina", "Sell out Total", si_raw, ing_raw)
    plot_row("Franja Comercial", "Sell out Total", si_raw, ing_raw)

    # --- NUEVA FILA: INGRESOS ---
    st.subheader("🚚 Análisis de Ingresos (Arribos)")
    i1, i2, i3 = st.columns([1, 1, 2])
    if not ing_raw.empty:
        df_ing_ma = ing_raw.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO']], on='SKU', how='left')
        fig_ing_dis = px.pie(df_ing_ma, values='Cant', names='Disciplina', title="Ingresos por Disciplina", color_discrete_map=COLOR_MAP_DIS)
        i1.plotly_chart(fig_ing_dis, use_container_width=True)
        
        fig_ing_fra = px.pie(df_ing_ma, values='Cant', names='FRANJA_PRECIO', title="Ingresos por Franja")
        i2.plotly_chart(fig_ing_fra, use_container_width=True)
        
        ing_mes = df_ing_ma.groupby(['Mes_Ingreso', 'Disciplina'])['Cant'].sum().reset_index()
        fig_ing_bar = px.bar(ing_mes, x='Mes_Ingreso', y='Cant', color='Disciplina', title="Arribos Mensuales", color_discrete_map=COLOR_MAP_DIS)
        i3.plotly_chart(fig_ing_bar, use_container_width=True)

    # --- 9. TABLA ---
    st.divider()
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    st.dataframe(df[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass', 'Sell In Total', 'Ingresos Total']].sort_values('Sell out Total', ascending=False), use_container_width=True, hide_index=True)
