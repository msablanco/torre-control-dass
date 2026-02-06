import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import numpy as np
import plotly.express as px

st.set_page_config(page_title="Dass Performance v10.0", layout="wide")

# --- 1. COLORES ---
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
    # --- 2. CARGA Y LIMPIEZA INICIAL ---
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    df_ma = df_ma.drop_duplicates(subset=['SKU'])
    
    col_f = next((c for c in df_ma.columns if any(x in c.upper() for x in ['FRANJA', 'SEGMENTO', 'PRECIO'])), 'FRANJA_PRECIO')
    df_ma['FRANJA_PRECIO'] = df_ma[col_f].fillna('SIN CAT').astype(str).str.upper()
    col_d = next((c for c in df_ma.columns if 'DISCIPLINA' in c.upper()), 'Disciplina')
    df_ma['Disciplina'] = df_ma[col_d].fillna('OTRO').astype(str).str.upper()

    # --- 3. PROCESAMIENTO DE TRANSACCIONES ---
    # Sell Out
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
    so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
    so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
    so_raw['Mes'] = so_raw['Fecha_dt'].dt.strftime('%Y-%m')
    
    # Sell In e Ingresos
    si_raw = data.get('Sell_in', pd.DataFrame()).copy()
    si_raw['SKU'] = si_raw['SKU'].astype(str).str.strip().str.upper()
    si_raw['Cant'] = pd.to_numeric(si_raw['Unidades'], errors='coerce').fillna(0)
    col_f_si = next((c for c in si_raw.columns if any(x in c.upper() for x in ['FECHA', 'VENTA'])), 'Fecha')
    si_raw['Fecha_dt'] = pd.to_datetime(si_raw[col_f_si], dayfirst=True, errors='coerce')
    si_raw['Mes'] = si_raw['Fecha_dt'].dt.strftime('%Y-%m')
    
    # Stock
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
    stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
    stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
    stk_raw['Mes'] = stk_raw['Fecha_dt'].dt.strftime('%Y-%m')
    stk_raw['Cliente_up'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

    # --- 4. FILTROS DINÁMICOS EN SIDEBAR ---
    st.sidebar.header("🔍 Filtros de Control")
    
    # A. Filtro de Periodo (Meses disponibles en todos los datos)
    all_months = sorted(list(set(so_raw['Mes'].dropna()) | set(si_raw['Mes'].dropna()) | set(stk_raw['Mes'].dropna())), reverse=True)
    f_periodo = st.sidebar.selectbox("📅 Periodo (Mes)", ["Todos"] + all_months)

    # Aplicar filtro de periodo antes de calcular opciones de Disciplina/Franja
    def filter_by_month(df_target):
        if f_periodo != "Todos":
            return df_target[df_target['Mes'] == f_periodo]
        return df_target

    so_f = filter_by_month(so_raw)
    si_f = filter_by_month(si_raw)
    stk_f = filter_by_month(stk_raw)

    # B. Filtros de Disciplina/Franja (Solo los que tienen datos reales)
    skus_con_movimiento = set(so_f['SKU']) | set(si_f['SKU']) | set(stk_f['SKU'])
    maestro_con_data = df_ma[df_ma['SKU'].isin(skus_con_movimiento)]

    f_dis = st.sidebar.multiselect("👟 Disciplinas Activas", sorted(maestro_con_data['Disciplina'].unique()))
    f_fra = st.sidebar.multiselect("🏷️ Franjas Activas", sorted(maestro_con_data['FRANJA_PRECIO'].unique()))
    search_query = st.sidebar.text_input("🔎 Buscador Universal").upper()

    # --- 5. LÓGICA DE FILTRADO FINAL ---
    def final_filter(df_to_proc, is_maestro=False):
        temp = df_to_proc.copy()
        if not is_maestro: # Si es tabla de mov, unir con maestro para filtrar por categoria
            temp = temp.merge(df_ma[['SKU', 'Disciplina', 'FRANJA_PRECIO', 'Descripcion']], on='SKU', how='left')
        if f_dis: temp = temp[temp['Disciplina'].isin(f_dis)]
        if f_fra: temp = temp[temp['FRANJA_PRECIO'].isin(f_fra)]
        if search_query:
            mask = temp.apply(lambda row: row.astype(str).str.contains(search_query).any(), axis=1)
            temp = temp[mask]
        return temp

    so_final = final_filter(so_f)
    si_final = final_filter(si_f)
    stk_final = final_filter(stk_f)
    df_filt = final_filter(df_ma, is_maestro=True)

    # --- 6. CÁLCULO DE MÉTRICAS AGRUPADAS ---
    so_grp = so_final.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell Out'})
    si_grp = si_final.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell In'})
    
    # Stock: Si es "Todos", toma la última fecha global. Si es un mes, la última de ese mes.
    max_f_stk = stk_final['Fecha_dt'].max()
    stk_snap = stk_final[stk_final['Fecha_dt'] == max_f_stk]
    stk_cli_grp = stk_snap[~stk_snap['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stk Cli'})
    stk_dass_grp = stk_snap[stk_snap['Cliente_up'].str.contains('DASS')].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stk Dass'})

    # Unión final para Tabla y KPIs
    df_dash = df_filt.merge(so_grp, on='SKU', how='left').merge(si_grp, on='SKU', how='left')
    df_dash = df_dash.merge(stk_cli_grp, on='SKU', how='left').merge(stk_dass_grp, on='SKU', how='left').fillna(0)

    # --- 7. DASHBOARD ---
    st.title(f"📊 Torre de Control Dass v10.0 {'- ' + f_periodo if f_periodo != 'Todos' else ''}")
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sell Out", f"{df_dash['Sell Out'].sum():,.0f}")
    k2.metric("Sell In", f"{df_dash['Sell In'].sum():,.0f}")
    k3.metric("Stock Cliente", f"{df_dash['Stk Cli'].sum():,.0f}")
    k4.metric("Stock Dass", f"{df_dash['Stk Dass'].sum():,.0f}")

    def pie_chart(dataframe, val, name, title, target, colors=None):
        sub = dataframe[dataframe[val] > 0]
        if not sub.empty:
            fig = px.pie(sub, values=val, names=name, title=title, color=name, color_discrete_map=colors)
            target.plotly_chart(fig, use_container_width=True)

    # FILA 1: DISCIPLINA
    st.subheader("📌 Análisis por Disciplina")
    d1, d2, d3, d4 = st.columns([1, 1, 1, 2])
    pie_chart(df_dash, 'Stk Dass', 'Disciplina', "Stk Dass", d1, COLOR_MAP_DIS)
    pie_chart(df_dash, 'Sell Out', 'Disciplina', "Sell Out", d2, COLOR_MAP_DIS)
    pie_chart(df_dash, 'Stk Cli', 'Disciplina', "Stock Cliente", d3, COLOR_MAP_DIS)
    d4.plotly_chart(px.bar(si_final.groupby(['Mes', 'Disciplina'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='Disciplina', title="Sell In Mensual", color_discrete_map=COLOR_MAP_DIS), use_container_width=True)

    # FILA 2: FRANJA
    st.subheader("🏷️ Análisis por Franja")
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    pie_chart(df_dash, 'Stk Dass', 'FRANJA_PRECIO', "Stk Dass", f1)
    pie_chart(df_dash, 'Sell Out', 'FRANJA_PRECIO', "Sell Out", f2)
    pie_chart(df_dash, 'Stk Cli', 'FRANJA_PRECIO', "Stock Cliente", f3)
    f4.plotly_chart(px.bar(si_final.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Sell In Mensual x Franja"), use_container_width=True)

    # FILA 3: INGRESOS (Usando fecha arrivo si existe)
    st.subheader("🚚 Análisis de Arribos")
    i1, i2, i3, i4 = st.columns([1, 1, 1, 2])
    pie_chart(si_final, 'Cant', 'Disciplina', "Arribos x Dis", i1, COLOR_MAP_DIS)
    pie_chart(si_final, 'Cant', 'FRANJA_PRECIO', "Arribos x Franja", i2)
    pie_chart(df_dash, 'Stk Dass', 'Disciplina', "Stock Dass Part.", i3, COLOR_MAP_DIS)
    i4.plotly_chart(px.bar(si_final.groupby(['Mes', 'FRANJA_PRECIO'])['Cant'].sum().reset_index(), x='Mes', y='Cant', color='FRANJA_PRECIO', title="Arribos Mensuales x Franja"), use_container_width=True)

    # --- 8. TABLA ---
    st.divider()
    st.dataframe(df_dash[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell Out', 'Sell In', 'Stk Cli', 'Stk Dass']].sort_values('Sell Out', ascending=False), use_container_width=True, hide_index=True)
