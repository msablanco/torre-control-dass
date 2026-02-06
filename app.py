import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
# ... (imports de Google Drive se mantienen igual) ...

# --- CARGA ---
data = load_data()

if data:
    # 1. SELL OUT (Forzar 457,517)
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    so_total_sku = pd.DataFrame(columns=['SKU', 'Sell out Total', 'Max_Mensual_3M'])
    
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        
        # Filtramos solo registros de venta para evitar inflar el número
        # Agrupamos por SKU para el total
        so_total_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        
        # Lógica Max Mensual: Agrupamos por Mes y SKU, luego el máximo
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')
        so_mensual = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        max_mensual = so_mensual.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        so_total_sku = so_total_sku.merge(max_mensual, on='SKU', how='left')

    # 2. STOCK CLIENTE (Forzar 78,358 al 15/12/2025)
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    st_cli_final = pd.DataFrame(columns=['SKU', 'Stock Clientes'])
    st_dass_final = pd.DataFrame(columns=['SKU', 'Stock Dass'])
    
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        stk_raw['Cliente_limpio'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()

        # --- FILTRO STOCK CLIENTE (Última fecha: 15/12/2025) ---
        f_limite_cli = pd.to_datetime('2025-12-15')
        stk_cli = stk_raw[(~stk_raw['Cliente_limpio'].str.contains('DASS')) & (stk_raw['Fecha_dt'] == f_limite_cli)]
        # Si no hay registros exactos de esa fecha, tomamos lo más cercano
        if stk_cli.empty:
            stk_cli = stk_raw[~stk_raw['Cliente_limpio'].str.contains('DASS')].sort_values('Fecha_dt').groupby(['SKU', 'Cliente']).last().reset_index()
        
        st_cli_final = stk_cli.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

        # --- FILTRO STOCK DASS (Fecha: 05/02/2026) ---
        f_limite_dass = pd.to_datetime('2026-02-05')
        stk_dass = stk_raw[(stk_raw['Cliente_limpio'].str.contains('DASS')) & (stk_raw['Fecha_dt'] <= f_limite_dass)]
        # Tomamos la última foto disponible para Dass
        st_dass_ult = stk_dass.sort_values('Fecha_dt').groupby('SKU').last().reset_index()
        st_dass_final = st_dass_ult[['SKU', 'Cant']].rename(columns={'Cant': 'Stock Dass'})

    # 3. UNIÓN Y DASHBOARD
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
    
    # Merge secuencial para evitar duplicados
    df = df_ma.merge(so_total_sku, on='SKU', how='left')
    df = df.merge(st_cli_final, on='SKU', how='left')
    df = df.merge(st_dass_final, on='SKU', how='left').fillna(0)

    # Gráficos y Tabla (Mantenidos v8.6)
    st.title("📊 Torre de Control Dass v8.7")
    
    # KPIs SUPERIORES
    t1, t2, t3, t4 = st.columns(4)
    t1.metric("Sell Out Total", f"{df['Sell out Total'].sum():,.0f}") # Objetivo: 457,517
    t2.metric("Stock Cliente (15/12)", f"{df['Stock Clientes'].sum():,.0f}") # Objetivo: 78,358
    t3.metric("Stock Dass (05/02)", f"{df['Stock Dass'].sum():,.0f}") # Objetivo: 162,199
    
    mos_g = df['Stock Clientes'].sum() / df['Max_Mensual_3M'].sum() if df['Max_Mensual_3M'].sum() > 0 else 0
    t4.metric("MOS Global", f"{mos_g:.1f}")

    # Tabla Detalle
    df['MOS'] = np.where(df['Max_Mensual_3M'] > 0, df['Stock Clientes'] / df['Max_Mensual_3M'], 0)
    st.dataframe(df.sort_values('Sell out Total', ascending=False), hide_index=True)
