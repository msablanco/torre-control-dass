import streamlit as st
import pandas as pd
# ... (mantenemos los imports de google y plotly de las versiones anteriores) ...

# --- [EL PROCESAMIENTO DE DATOS SE MANTIENE IGUAL HASTA EL MERGE FINAL] ---

if data:
    # ... (Lógica de carga y filtros v7.0) ...

    # --- 5. CÁLCULO DE MÉTRICAS PARA LA TABLA ---
    # Calculamos el total de Sell Out para saber el % de participación de cada SKU
    total_so = df['Sell out Clientes'].sum()
    
    df['% Share'] = np.where(total_so > 0, (df['Sell out Clientes'] / total_so) * 100, 0)
    df['WOS'] = np.where(df['Sell out Clientes'] > 0, df['Stock Clientes'] / (df['Sell out Clientes'] / 4), 0) 
    # Nota: Divido Sell Out por 4 para estimar venta semanal si los datos son mensuales
    
    # --- 6. DASHBOARD VISUAL ---
    st.title("📊 Torre de Control Dass v7.1")
    
    # [Aquí van las dos filas de gráficos de torta que ya configuramos]
    # ... (safe_pie_colored de Disciplina y Franja) ...

    # --- 7. SUPER TABLA INTEGRADA ---
    st.divider()
    st.subheader("🏆 Análisis Maestro de SKUs (Ventas + Stock + Disponibilidad)")
    
    # Seleccionamos y reordenamos las columnas para que sigan el hilo de la charla
    cols_finales = [
        'SKU', 'Descripcion', 'Disciplina', 'FRANJA_PRECIO', 
        'Sell in', 'Sell out Clientes', '% Share',
        'Stock Clientes', 'WOS', 'Stock Dass'
    ]
    
    df_ranking = df[cols_finales].sort_values('Sell out Clientes', ascending=False)

    # Formatamos la tabla para que sea legible
    st.dataframe(
        df_ranking.style.format({
            'Sell in': '{:,.0f}',
            'Sell out Clientes': '{:,.0f}',
            '% Share': '{:.1f}%',
            'Stock Clientes': '{:,.0f}',
            'WOS': '{:.1f}',
            'Stock Dass': '{:,.0f}'
        }).map(lambda v: 'background-color: #ffcccc' if v > 4 else ('background-color: #ccffcc' if 0 < v <= 2 else ''), subset=['WOS']),
        use_container_width=True,
        height=600
    )

    st.caption("💡 Tip: El WOS (Weeks on Hand) te dice cuántas semanas dura el stock del cliente. Rojo (>4) es sobrestock, Verde (1-2) es rotación ideal.")
