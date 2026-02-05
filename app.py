import streamlit as st
import pandas as pd
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Torre de Control Dass", layout="wide", page_icon="👟")

# --- CONEXIÓN A GOOGLE DRIVE ---
@st.cache_data(ttl=3600)  # Cache de 1 hora para no saturar la API
def load_data_from_drive():
    try:
        info = st.secrets["gcp_service_account"]
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        folder_id = st.secrets["google_drive_folder_id"]
        
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv'",
            fields="files(id, name)"
        ).execute()
        items = results.get('files', [])

        dfs = {}
        for item in items:
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            name = item['name'].replace('.csv', '')
            dfs[name] = pd.read_csv(fh, encoding='latin-1')
        return dfs
    except Exception as e:
        st.error(f"Error conectando a Drive: {e}")
        return None

# --- PROCESAMIENTO DE DATOS ---
data = load_data_from_drive()

if data:
    # 1. Preparar Sell Out y VPS (Venta Promedio Semanal)
    # Agrupamos por SKU, Cliente y Ubicación para tener la granularidad total
    df_out = data['Sell_out']
    vps = df_out.groupby(['SKU', 'Cliente', 'Ubicacion'])['Unidades'].sum() / 4
    vps = vps.reset_index().rename(columns={'Unidades': 'VPS'})

    # 2. Cruce Maestro (Unificar Stock + Maestro + Ventas + Ingresos)
    # Empezamos por Stock para asegurar que vemos lo que hay en depósito
    df = data['Stock'].merge(data['Maestro_Productos'], on='SKU', how='left')
    df = df.merge(vps, on=['SKU', 'Cliente', 'Ubicacion'], how='left').fillna(0)
    
    # Agregar Ingresos Futuros (A nivel SKU, ya que son compras generales)
    if 'Ingresos' in data:
        ingresos_sum = data['Ingresos'].groupby('SKU')['Cantidad'].sum().reset_index()
        df = df.merge(ingresos_sum, on='SKU', how='left').rename(columns={'Cantidad_x': 'Stock_Actual', 'Cantidad_y': 'Ingresos_Futuros'}).fillna(0)

    # 3. Lógica de Canales
    def categorizar_canal(ubi):
        ubi = str(ubi).upper()
        if 'MAYORISTA' in ubi: return 'Mayorista'
        if 'ECOM' in ubi or 'WEB' in ubi: return 'E-com'
        return 'Retail'
    
    df['Canal'] = df['Ubicacion'].apply(categorizar_canal)
    
    # 4. Cálculo de Cobertura (WOS)
    # Si la venta es 0, asignamos 99 semanas para evitar errores infinitos
    df['WOS'] = df.apply(lambda x: x['Stock_Actual'] / x['VPS'] if x['VPS'] > 0 else 99, axis=1)

    # --- INTERFAZ: SIDEBAR FILTROS ---
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/Information_icon.svg/1200px-Information_icon.svg.png", width=50) # Icono decorativo
    st.sidebar.title("Filtros de Control")
    
    if st.sidebar.button("🔄 Actualizar Datos de Drive"):
        st.cache_data.clear()
        st.rerun()

    f_canal = st.sidebar.multiselect("Canal", df['Canal'].unique(), default=df['Canal'].unique())
    f_cliente = st.sidebar.multiselect("Cliente", df['Cliente'].unique(), default=df['Cliente'].unique())
    f_disciplina = st.sidebar.multiselect("Disciplina", df['Disciplina'].unique(), default=df['Disciplina'].unique())
    f_sku = st.sidebar.text_input("Buscador (SKU, Descripción, Cliente)")

    # Aplicar filtros al DataFrame
    query = (df['Canal'].isin(f_canal)) & (df['Cliente'].isin(f_cliente)) & (df['Disciplina'].isin(f_disciplina))
    df_f = df[query]
    
    if f_sku:
        df_f = df_f[df_f.apply(lambda r: f_sku.lower() in str(r).lower(), axis=1)]

    # --- KPIs PRINCIPALES ---
    st.title("👟 Torre de Control Dass: Inventarios")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stock Físico", f"{df_f['Stock_Actual'].sum():,.0f}")
    c2.metric("Venta Semanal (Sell Out)", f"{df_f['VPS'].sum():,.0f}")
    c3.metric("WOS Promedio", f"{df_f['WOS'].replace(99, 0).mean():.1f} sem")
    c4.metric("Ingresos Pendientes", f"{df_f['Ingresos_Futuros'].sum():,.0f}")

    # --- TABLA DE DATOS CON FOTOS ---
    st.subheader("📋 Detalle de Inventario y Cobertura")
    
    # Semáforo de stock para la tabla
    def color_wos(val):
        color = 'red' if val < 3 else ('orange' if val < 6 else 'green')
        if val == 99: color = 'gray'
        return f'color: {color}'

    st.dataframe(
        df_f,
        column_config={
            "URL_Foto": st.column_config.ImageColumn("Producto"),
            "Stock_Actual": st.column_config.NumberColumn("Stock"),
            "WOS": st.column_config.NumberColumn("WOS (Sem)", format="%.1f"),
            "VPS": st.column_config.NumberColumn("Venta Sem."),
            "Ingresos_Futuros": st.column_config.NumberColumn("Ingresos")
        },
        hide_index=True,
        use_container_width=True
    )

    # --- CHAT CON IA (GEMINI) ---
    st.divider()
    st.subheader("🤖 Consultas Inteligentes (IA)")
    user_input = st.chat_input("Ej: ¿Qué productos de Adidas en Dexter están por quedarse sin stock?")

    if user_input:
        with st.chat_message("user"):
            st.write(user_input)
        
        # Configurar Gemini
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Preparamos un resumen compacto para la IA
        contexto_ia = df_f[['SKU', 'Descripcion', 'Cliente', 'Ubicacion', 'Stock_Actual', 'VPS', 'WOS']].head(40).to_string()
        
        prompt = f"""
        Eres un experto en planeamiento de inventarios de Dass. 
        Analiza los siguientes datos de stock y ventas para responder la pregunta del usuario.
        
        REGLAS DE NEGOCIO:
        - WOS < 3: Quiebre inminente.
        - WOS > 15: Exceso de stock (Stock inmovilizado).
        - 'MAYORISTA' es nuestro depósito central.
        - Si falta en una tienda pero hay en Mayorista, sugiere TRASLADO.
        - Si no hay en ninguno, sugiere COMPRA.

        DATOS:
        {contexto_ia}
        
        PREGUNTA: {user_input}
        """
        
        with st.chat_message("assistant"):
            with st.spinner("Analizando inventario..."):
                response = model.generate_content(prompt)
                st.write(response.text)

else:

    st.error("No se pudieron cargar los datos. Verifica los permisos de la Service Account en Google Drive.")
