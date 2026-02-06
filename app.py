import streamlit as st
import pandas as pd
# ... (mantenemos los mismos imports de la v8.0) ...

# --- [TODO EL PROCESAMIENTO DE DATOS Y FILTROS SE MANTIENE IGUAL A v8.0] ---

if data:
    # ... (Procesamiento de df_ma, so_stats, st_cli_grp y filtros v8.0) ...

    # --- 5. MERGE Y CÁLCULOS (v8.0) ---
    so_sum = so_stats.groupby('SKU')[['Sell out Total', 'Max_Mensual_3M']].sum().reset_index()
    st_cli_sum = st_cli_grp.groupby('SKU')['Stock Clientes'].sum().reset_index()
    df = df_ma.merge(st_dass_grp, on='SKU', how='left').merge(so_sum, on='SKU', how='left').merge(st_cli_sum, on='SKU', how='left').fillna(0)
    
    # Aplicar filtros adicionales (v8.0)
    if f_dis: df = df[df['Disciplina'].isin(f_dis)]
    if f_fra: df = df[df['FRANJA_PRECIO'].isin(f_fra)]
    if sku_search: df = df[df['SKU'].str.contains(sku_search.upper())]

    # ... (Visualización de los 6 gráficos de torta v8.0) ...

    # --- 6. TABLA EJECUTIVA CON SUBTOTALES SUPERIORES ---
    st.divider()
    st.subheader("🏆 Resumen Ejecutivo: MOS e Inventarios")

    # Definimos los datos a mostrar (filtrando para no mostrar filas vacías)
    df_ver = df[(df['Sell out Total'] > 0) | (df['Stock Clientes'] > 0) | (df['Stock Dass'] > 0)].copy()
    df_ver['MOS'] = np.where(df_ver['Max_Mensual_3M'] > 0, df_ver['Stock Clientes'] / df_ver['Max_Mensual_3M'], 0)

    # --- CÁLCULO DE TOTALES PARA LOS KPI CARDS ---
    total_so = df_ver['Sell out Total'].sum()
    total_max = df_ver['Max_Mensual_3M'].sum()
    total_stk_cli = df_ver['Stock Clientes'].sum()
    total_stk_dass = df_ver['Stock Dass'].sum()
    # MOS Promedio Ponderado (Total Stock / Total Max Mensual)
    mos_global = total_stk_cli / total_max if total_max > 0 else 0

    # --- FILA DE SUB-TOTALES (Arriba de la tabla) ---
    # Creamos 5 columnas para alinear con las métricas de la tabla
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Sell Out Total", f"{total_so:,.0f}")
    m2.metric("Max. Mensual (Sum)", f"{total_max:,.0f}")
    m3.metric("Stock Cliente", f"{total_stk_cli:,.0f}")
    m4.metric("MOS Global", f"{mos_global:.1f}")
    m5.metric("Stock Dass Disp.", f"{total_stk_dass:,.0f}")

    # --- CONFIGURACIÓN DE LA TABLA (Sin índice) ---
    cols_tabla = ['SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 'Sell out Total', 'Max_Mensual_3M', 'Stock Clientes', 'MOS', 'Stock Dass']
    
    st.dataframe(
        df_ver[cols_tabla].sort_values('Sell out Total', ascending=False).style.format({
            'Sell out Total': '{:,.0f}', 'Max_Mensual_3M': '{:,.0f}', 
            'Stock Clientes': '{:,.0f}', 'MOS': '{:.1f}', 'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 3 else ('background-color: #ccffcc' if 0 < v <= 1 else ''), subset=['MOS']),
        use_container_width=True,
        hide_index=True  # <--- ESTO QUITA EL NÚMERO DE FILA
    )
    
    st.caption("💡 Los subtotales superiores se ajustan automáticamente según los filtros aplicados a la izquierda.")

else:
    st.error("Error al cargar datos.")
