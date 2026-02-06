import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
# ... (imports de Google Drive mantenidos igual) ...

# --- CARGA Y PROCESAMIENTO ---
data = load_data()

if data:
    # 1. Maestro de Productos (Mantenido v8.2)
    df_ma = data.get('Maestro_Productos', pd.DataFrame()).copy()
    if not df_ma.empty:
        df_ma['SKU'] = df_ma['SKU'].astype(str).str.strip().str.upper()
        # Lógica de Franja y Disciplina v8.2...

    # 2. PROCESAMIENTO SELL OUT (La clave del error de los 738k)
    so_raw = data.get('Sell_out', pd.DataFrame()).copy()
    if not so_raw.empty:
        so_raw['SKU'] = so_raw['SKU'].astype(str).str.strip().str.upper()
        so_raw['Cant'] = pd.to_numeric(so_raw['Unidades'], errors='coerce').fillna(0)
        so_raw['Fecha_dt'] = pd.to_datetime(so_raw['Fecha'], dayfirst=True, errors='coerce')
        so_raw['MesAnio'] = so_raw['Fecha_dt'].dt.to_period('M')

        # --- AQUÍ CORREGIMOS EL MÁXIMO ---
        # Primero: Sumamos TODO el Sell Out por SKU y Mes (incluyendo todos los clientes)
        so_mensual_agrupado = so_raw.groupby(['SKU', 'MesAnio'])['Cant'].sum().reset_index()
        
        # Segundo: De esos totales mensuales por SKU, buscamos el Máximo
        max_mensual_final = so_mensual_agrupado.groupby('SKU')['Cant'].max().reset_index().rename(columns={'Cant': 'Max_Mensual_3M'})
        
        # Tercero: Sell Out Total por SKU (para la tabla)
        so_total_sku = so_raw.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Sell out Total'})
        
        so_stats = so_total_sku.merge(max_mensual_final, on='SKU', how='left')

    # 3. STOCK (Última foto por Cliente)
    stk_raw = data.get('Stock', pd.DataFrame()).copy()
    if not stk_raw.empty:
        stk_raw['SKU'] = stk_raw['SKU'].astype(str).str.strip().str.upper()
        stk_raw['Cant'] = pd.to_numeric(stk_raw['Cantidad'], errors='coerce').fillna(0)
        stk_raw['Fecha_dt'] = pd.to_datetime(stk_raw['Fecha'], dayfirst=True, errors='coerce')
        
        # Filtro Stock Dass vs Clientes
        stk_raw['Cliente_stk'] = stk_raw['Cliente'].fillna('').astype(str).str.upper()
        mask_dass = stk_raw['Cliente_stk'].str.contains('DASS', na=False)
        
        # Stock Dass: Suma total actual
        st_dass_grp = stk_raw[mask_dass].groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Dass'})
        
        # Stock Clientes: Última foto por SKU y Cliente
        stk_cli_ult = stk_raw[~mask_dass].sort_values('Fecha_dt').groupby(['SKU', 'Cliente'])['Cant'].last().reset_index()
        st_cli_sum = stk_cli_ult.groupby('SKU')['Cant'].sum().reset_index().rename(columns={'Cant': 'Stock Clientes'})

    # --- 4. MERGE FINAL Y TABLA ---
    df = df_ma.merge(st_dass_grp, on='SKU', how='left').merge(so_stats, on='SKU', how='left').merge(st_cli_sum, on='SKU', how='left').fillna(0)

    # (Filtros Sidebar v8.2 aplicados aquí...)

    st.title("📊 Torre de Control Dass v8.3")
    
    # --- KPIs TOTALES (CALIBRADOS) ---
    df_ver = df[(df['Sell out Total'] > 0) | (df['Stock Clientes'] > 0) | (df['Stock Dass'] > 0)].copy()
    
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Sell Out Total", f"{df_ver['Sell out Total'].sum():,.0f}")
    t2.metric("Max Mensual (Pico)", f"{df_ver['Max_Mensual_3M'].sum():,.0f}")
    t3.metric("Stock Cliente", f"{df_ver['Stock Clientes'].sum():,.0f}")
    
    # Cálculo MOS Global: Stock Total / Máximo Mensual Total
    mos_g = df_ver['Stock Clientes'].sum() / df_ver['Max_Mensual_3M'].sum() if df_ver['Max_Mensual_3M'].sum() > 0 else 0
    t4.metric("MOS Global", f"{mos_g:.1f}")
    t5.metric("Stock Dass Disp.", f"{df_ver['Stock Dass'].sum():,.0f}")

    # --- TABLA SIN ÍNDICE ---
    df_ver['MOS'] = np.where(df_ver['Max_Mensual_3M'] > 0, df_ver['Stock Clientes'] / df_ver['Max_Mensual_3M'], 0)
    
    st.dataframe(
        df_ver[['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']]
        .sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True, hide_index=True
    )
