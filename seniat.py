import streamlit as st
import pandas as pd
import psycopg2
import numpy as np
from datetime import datetime
from io import BytesIO
import re
from pyDolarVenezuela.pages import BCV
from pyDolarVenezuela import Monitor
import pytz

# --- CONFIGURACIÓN DE BASE DE DATOS ---
def conectar_contingencia():
    try:
        conexion = psycopg2.connect(
            host=st.secrets["supabase"]["host"],
            port=st.secrets["supabase"]["port"],
            database=st.secrets["supabase"]["database"],
            user=st.secrets["supabase"]["user"],
            password=st.secrets["supabase"]["password"]
        )
        return conexion
    except Exception as e:
        st.error(f"Fallo la conexión de contingencia: {e}")
        return None

# Probar la conexión
conn = conectar_contingencia()
if conn:
    st.success("¡Conectado exitosamente a Supabase (Contingencia)!")
    # Aquí puedes ejecutar tus consultas (cursor.execute...)
    conn.close()
@st.cache_data(ttl=3600)
def obtener_tasa_bcv_euro():
    """Obtiene la tasa oficial del Euro (EUR) en el BCV usando pyDolarVenezuela."""
    try:
        monitor = Monitor(BCV, 'USD') 
        euro_data = monitor.get_value_monitors("eur")
        tasa_eur = euro_data.price
        
        zone = pytz.timezone('America/Caracas')
        last_update_dt = euro_data.last_update
        last_update_ve = last_update_dt.astimezone(zone)
        fecha_str = last_update_ve.strftime('%d/%m/%Y, %I:%M %p')
        
        return float(tasa_eur), fecha_str
    except Exception as e:
        st.error(f"Fallo al conectar con BCV a través de pyDolarVenezuela: {e}")
        return None, "Error de conexión"

# --- FUNCIONES DE BASE DE DATOS Y LÓGICA ---


@st.cache_data(ttl=600)
def obtener_calendario_db():
    try:
        # Usamos la conexión de Supabase que ya creaste arriba
        conn = conectar_contingencia() 
        if conn is None:
            return pd.DataFrame() # Retorna vacío si falla la conexión

        query = """
            SELECT c.digito_rif, r.codigo_renta, c.anio, c.mes, 
                   COALESCE(c.quincena, 'Mensual') AS quincena, 
                   c.fecha_limite, tc.tipo AS tipo_contribuyente,
                   COALESCE(c.tipo_cierre, 'Ordinario') AS tipo_cierre
            FROM calendario_seniat c
            JOIN dim_renta r ON c.id_renta = r.id_renta
            JOIN dim_tipo_contribuyente tc ON c.id_tipo_contribuyente = tc.id_tipo_contribuyente;
        """
        df_cal = pd.read_sql_query(query, conn)
        conn.close()
        
        df_cal['fecha_limite'] = pd.to_datetime(df_cal['fecha_limite'])
        df_cal['anio'] = df_cal['anio'].astype(int)
        df_cal['mes'] = df_cal['mes'].astype(int)
        df_cal['digito_rif'] = df_cal['digito_rif'].astype(int)
        df_cal['quincena'] = df_cal['quincena'].fillna('Mensual').replace({'': 'Mensual', 'None': 'Mensual'})
        df_cal['tipo_contribuyente'] = df_cal['tipo_contribuyente'].astype(str).str.strip().str.lower()
        df_cal['tipo_cierre'] = df_cal['tipo_cierre'].astype(str).str.strip().str.lower()
        
        return df_cal
    except Exception as e:
        st.error(f"Error al obtener el calendario de contingencia: {e}")
        return pd.DataFrame()

def procesar_auditoria_completa(df_ops, df_cal, digito_usuario, fecha_contingencia, tipo_cierre_usuario):
    fecha_contingencia_dt = pd.to_datetime(fecha_contingencia)
    anio_fin_calendario = fecha_contingencia_dt.year
    
    # ==========================================
    # 1. PREPARACIÓN Y TRADUCCIÓN DEL EXCEL
    # ==========================================
    df_ops.columns = df_ops.columns.str.strip()
    
    col_doc = [c for c in df_ops.columns if 'documento' in c.lower() or 'planilla' in c.lower() or 'certificado' in c.lower()]
    if not col_doc: 
        col_doc = [c for c in df_ops.columns if 'nro' in c.lower() or 'num' in c.lower()]
        
    duplicados_eliminados = 0
    
    # 🔥 CORRECCIÓN 1: Deduplicación Inteligente (Evita borrar impuestos diferentes pagados el mismo día)
    if col_doc:
        nombre_col_doc = col_doc[0]
        total_previo = len(df_ops)
        
        df_ops['doc_temp'] = df_ops[nombre_col_doc].astype(str).str.strip().replace({'nan': pd.NA, '': pd.NA, 'None': pd.NA, '0': pd.NA})
        df_con_doc = df_ops.dropna(subset=['doc_temp'])
        df_sin_doc = df_ops[df_ops['doc_temp'].isna()]
        
        # Deduplica considerando el documento y la renta, para no pisar pagos válidos
        df_con_doc = df_con_doc.drop_duplicates(subset=['doc_temp', 'Renta'], keep='last')
        
        df_ops = pd.concat([df_con_doc, df_sin_doc]).drop(columns=['doc_temp']).copy()
        duplicados_eliminados = total_previo - len(df_ops)
    
    col_fecha = [c for c in df_ops.columns if 'fecha' in c.lower() and 'operaci' in c.lower()]
    if not col_fecha:
        st.error("❌ No encontré la columna de fecha de operación en el Excel.")
        st.stop()
    nombre_col_fecha = col_fecha[0]
    
    nombre_concepto = 'Concepto Contable' if 'Concepto Contable' in df_ops.columns else 'Descripción'
    if nombre_concepto not in df_ops.columns: df_ops[nombre_concepto] = '---'
        
    df_ops['Periodo_Original'] = df_ops['Periodo'].astype(str)

    # 🔥 NUEVO: Detectar si la fila contiene la palabra "sustitutiva" en cualquier columna
    df_ops['Es_Sustitutiva'] = df_ops.astype(str).apply(lambda col: col.str.lower().str.contains('sustitutiva')).any(axis=1)
    
    # 🔥 CORRECCIÓN 2: Forzar formato de fecha Latino (Día/Mes/Año)
    df_ops['Fecha_Operacion_Real'] = pd.to_datetime(df_ops[nombre_col_fecha], errors='coerce', dayfirst=True)
    
    # 🔥 CORRECCIÓN 3: Parseo de Periodo ultra-resistente
    def parse_periodo(val):
        if pd.isna(val): return 0, 0
        if isinstance(val, (datetime, pd.Timestamp)): return val.month, val.year
        val_str = str(val).lower().strip()
        
        meses_map = {'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12}
        mes, anio = 0, 0
        
        # Extraer Año
        match_anio = re.search(r'(20\d{2})', val_str)
        if match_anio:
            anio = int(match_anio.group(1))
        else:
            match_anio_2 = re.search(r'(?<!\d)(\d{2})(?!\d)', val_str) # Busca 2 digitos solos (ej. -23)
            if match_anio_2:
                anio = 2000 + int(match_anio_2.group(1))
                
        # Extraer Mes
        for m_str, m_num in meses_map.items():
            if m_str in val_str:
                mes = m_num
                break
                
        if mes == 0:
            match_mes = re.search(r'^(0?[1-9]|1[0-2])[-/]', val_str)
            if match_mes:
                mes = int(match_mes.group(1))
                
        return mes, anio

    periodos_parsed = df_ops['Periodo'].apply(parse_periodo)
    df_ops['Mes_Num'] = periodos_parsed.apply(lambda x: x[0])
    df_ops['Anio_Num'] = periodos_parsed.apply(lambda x: x[1])
    df_ops['Digito'] = digito_usuario
    
    df_validos = df_ops[df_ops['Anio_Num'] > 2000].copy()
    if not df_validos.empty:
        df_validos['YYYYMM'] = df_validos['Anio_Num'] * 100 + df_validos['Mes_Num']
        min_ym = df_validos['YYYYMM'].min()
        max_ym = df_validos['YYYYMM'].max()
        mes_ini, anio_ini = int(str(min_ym)[-2:]), int(str(min_ym)[:4])
        mes_fin, anio_fin_excel = int(str(max_ym)[-2:]), int(str(max_ym)[:4])
        periodo_str = f"{mes_ini:02d}/{anio_ini} hasta {mes_fin:02d}/{anio_fin_excel}"
        anio_inicio = anio_ini
    else:
        periodo_str = "Sin periodos válidos detectados"
        anio_inicio = anio_fin_calendario - 3 
        min_ym = anio_inicio * 100 + 1
        
    def normalizar_ops(row):
        renta = str(row.get('Renta', '')).upper().strip()
        concepto = str(row.get(nombre_concepto, '')).upper().strip()
        if 'ANTICIPO' in renta or 'ANTICIPO' in concepto: return 'ANTICIPO-ISLR'
        if 'IVA' in renta:
            if '30' in renta: return 'IVA/30'
            if '35' in renta: return 'IVA/35'
            return 'IVA/30'
        if 'RET' in renta or 'RET' in concepto: return 'ISLR-RETENCION'
        elif 'ISLR' in renta or 'ISLR' in concepto: return 'ISLR-ANUAL'
        return renta
        
    df_ops['Renta_Norm'] = df_ops.apply(normalizar_ops, axis=1)
    
    # 🔥 CORRECCIÓN 4: Flexibilización del Filtro de IVA (Evita borrar pagos válidos sin concepto)
    def es_registro_valido_iva(row):
        if row['Renta_Norm'] != 'IVA/30': return True
        concepto_limpio = re.sub(r'[^a-zA-Z]', '', str(row.get(nombre_concepto, '')).lower())
        # Si el concepto está vacío o dice pago, cuota, iva, totalapagar... es válido.
        if any(x in concepto_limpio for x in ['total', 'iva', 'pago', 'declaracion', 'cuota', '']):
            return True
        return False

    df_ops = df_ops[df_ops.apply(es_registro_valido_iva, axis=1)].copy()
    
    df_ops['Anio_Cruce'] = df_ops['Anio_Num']
    df_ops['Mes_Cruce'] = df_ops.apply(lambda r: 99 if r['Renta_Norm'] == 'ISLR-ANUAL' else r['Mes_Num'], axis=1)
    
    df_ops = df_ops[(df_ops['Fecha_Operacion_Real'].isna()) | (df_ops['Fecha_Operacion_Real'] <= fecha_contingencia_dt)].copy()
    
    anticipos_por_mes = {}
    if not df_ops.empty:
        df_ops['YYYYMM'] = df_ops['Anio_Cruce'] * 100 + df_ops['Mes_Cruce']
        
        df_esp_indicadores = df_ops[df_ops['Renta_Norm'] == 'IVA/35']
        
        if not df_esp_indicadores.empty:
            trans_yyyymm = df_esp_indicadores['YYYYMM'].min()
            df_ops['Tipo_Contribuyente'] = np.where(df_ops['YYYYMM'] >= trans_yyyymm, 'especial', 'ordinario')
            
            min_general_yyyymm = df_ops['YYYYMM'].min()
            if trans_yyyymm > min_general_yyyymm:
                mes_t = str(trans_yyyymm)[-2:]
                anio_t = str(trans_yyyymm)[:4]
                tipo_final_empresa = f"Transición a Especial ({mes_t}/{anio_t})"
            else:
                tipo_final_empresa = "Especial"
        else:
            df_ops['Tipo_Contribuyente'] = 'ordinario'
            trans_yyyymm = 999999
            tipo_final_empresa = "Ordinario"
            
        df_ops = df_ops.sort_values(by=['Digito', 'Renta_Norm', 'Anio_Cruce', 'Mes_Cruce', 'Fecha_Operacion_Real'])
        df_ops['Orden_Declaracion'] = df_ops.groupby(['Digito', 'Renta_Norm', 'Anio_Cruce', 'Mes_Cruce']).cumcount() + 1
        
        anticipos_ops = df_ops[df_ops['Renta_Norm'] == 'ANTICIPO-ISLR']
        anticipos_por_mes = anticipos_ops.groupby(['Anio_Cruce', 'Mes_Cruce']).size().to_dict()
        
        col_quin = [c for c in df_ops.columns if 'quincena' in c.lower()]
        df_ops['Quincena_Original'] = df_ops[col_quin[0]].apply(lambda val: 'Primera' if '1' in str(val) or 'PRIMER' in str(val).upper() else ('Segunda' if '2' in str(val) or 'SEGUND' in str(val).upper() else '')) if col_quin else ''
        
        def deducir_quincena(row):
            if row['Renta_Norm'] == 'ISLR-ANUAL': return 'Anual'
            if row['Renta_Norm'] == 'ISLR-RETENCION': return 'Mensual'
            if row['Tipo_Contribuyente'] != 'especial': return 'Mensual'
            if row['Renta_Norm'] == 'ANTICIPO-ISLR':
                if pd.notna(row['Fecha_Operacion_Real']):
                    dia_pago = row['Fecha_Operacion_Real'].day
                    if 1 <= dia_pago <= 15: return 'Segunda'
                    else: return 'Primera'
                if row['Quincena_Original'] in ['Primera', 'Segunda']: return row['Quincena_Original']
                return 'Primera' if row['Orden_Declaracion'] == 1 else 'Segunda'
            elif row['Renta_Norm'] in ['IVA/30', 'IVA/35']:
                if row['Quincena_Original'] in ['Primera', 'Segunda']: return row['Quincena_Original']
                return 'Primera' if row['Orden_Declaracion'] == 1 else 'Segunda'
            return 'Mensual'
            
        df_ops['Quincena_Deducida'] = df_ops.apply(deducir_quincena, axis=1)
    else:
        trans_yyyymm = 999999
        tipo_final_empresa = "Evaluación Sin Pagos Registrados"

    # ==========================================
    # 2. PREPARACIÓN DEL CALENDARIO
    # ==========================================
    anio_fin = anio_fin_calendario
    anios_presentes = list(range(anio_inicio, anio_fin + 1))
    tipo_c_user = tipo_cierre_usuario.strip().lower()
    
    def normalizar_cal(row):
        renta = str(row.get('codigo_renta', '')).upper().strip()
        if 'ANTICIPO' in renta: return 'ANTICIPO-ISLR'
        if 'IVA' in renta:
            if '30' in renta: return 'IVA/30'
            if '35' in renta: return 'IVA/35'
            return 'IVA/30'
        if 'RET' in renta: return 'ISLR-RETENCION'
        if '961' in renta or 'ISLR' in renta:
            diff_meses = (row['fecha_limite'].month - row['mes']) % 12
            if diff_meses >= 2: return 'ISLR-ANUAL'
            else: return 'ISLR-RETENCION'
        return renta
        
    df_cal['Renta_Norm'] = df_cal.apply(normalizar_cal, axis=1)
    
    def get_cal_anio_cruce(row):
        if row['Renta_Norm'] == 'ISLR-ANUAL': 
            if row['mes'] >= 10 and row['fecha_limite'].month <= 6: return row['fecha_limite'].year - 1
            return row['fecha_limite'].year
        if row['mes'] >= 10 and row['fecha_limite'].month <= 5: return row['fecha_limite'].year - 1
        return row['fecha_limite'].year
            
    df_cal['Anio_Cruce'] = df_cal.apply(get_cal_anio_cruce, axis=1)
    df_cal['Mes_Cruce'] = df_cal.apply(lambda r: 99 if r['Renta_Norm'] == 'ISLR-ANUAL' else r['mes'], axis=1)
    
    df_cal['YYYYMM_cal'] = df_cal['Anio_Cruce'] * 100 + df_cal['mes']
    df_cal['Tipo_Contribuyente_Esperado'] = np.where(df_cal['YYYYMM_cal'] >= trans_yyyymm, 'especial', 'ordinario')
    
    df_cal_empresa = df_cal[df_cal['digito_rif'] == digito_usuario].copy()
    
    mask_islr_anual = df_cal_empresa['Renta_Norm'] == 'ISLR-ANUAL'
    mask_cierre_exacto = (df_cal_empresa['tipo_cierre'] == tipo_c_user)
    
    if tipo_c_user == 'ordinario':
        df_cal_empresa = df_cal_empresa[~mask_islr_anual | (mask_cierre_exacto & (df_cal_empresa['mes'] == 12))]
    else:
        df_cal_empresa = df_cal_empresa[~mask_islr_anual | mask_cierre_exacto]
        
    def validar_universo(row):
        t = row['Tipo_Contribuyente_Esperado']
        r = row['Renta_Norm']
        if t == 'especial': return r in ['IVA/30', 'IVA/35', 'ANTICIPO-ISLR', 'ISLR-ANUAL', 'ISLR-RETENCION']
        else: return r in ['IVA/30', 'ISLR-ANUAL', 'ISLR-RETENCION']

    def es_obligacion_valida_desde_inicio(row):
        if row['Renta_Norm'] == 'ISLR-ANUAL': return row['Anio_Cruce'] >= anio_inicio
        else: return row['YYYYMM_cal'] >= min_ym
        
    df_cal_empresa = df_cal_empresa[
        (df_cal_empresa['Anio_Cruce'] <= anio_fin) &
        (df_cal_empresa.apply(es_obligacion_valida_desde_inicio, axis=1)) &
        (df_cal_empresa['tipo_contribuyente'] == df_cal_empresa['Tipo_Contribuyente_Esperado']) &
        (df_cal_empresa.apply(validar_universo, axis=1))
    ].copy()

    df_cal_empresa = df_cal_empresa[df_cal_empresa['fecha_limite'] <= fecha_contingencia_dt].copy()

    df_cal_empresa.loc[df_cal_empresa['Renta_Norm'] == 'ISLR-ANUAL', 'quincena'] = 'Anual'
    df_cal_empresa.loc[df_cal_empresa['Renta_Norm'] == 'ISLR-RETENCION', 'quincena'] = 'Mensual'
    mask_ord_iva = (df_cal_empresa['Tipo_Contribuyente_Esperado'] == 'ordinario') & (df_cal_empresa['Renta_Norm'] == 'IVA/30')
    df_cal_empresa.loc[mask_ord_iva, 'quincena'] = 'Mensual'

    mask_fantasmas_esp = (df_cal_empresa['Tipo_Contribuyente_Esperado'] == 'especial') & \
                         (df_cal_empresa['Renta_Norm'].isin(['IVA/30', 'IVA/35', 'ANTICIPO-ISLR'])) & \
                         (df_cal_empresa['quincena'] == 'Mensual')
    df_cal_empresa = df_cal_empresa[~mask_fantasmas_esp].copy()

    df_cal_empresa = df_cal_empresa.sort_values('fecha_limite').drop_duplicates(
        subset=['Anio_Cruce', 'Mes_Cruce', 'Renta_Norm', 'quincena', 'Tipo_Contribuyente_Esperado'], 
        keep='last'
    )

    # ==========================================
    # 3. EL CRUCE MAESTRO 
    # ==========================================
    if df_ops.empty: df_ops_cruce = pd.DataFrame(columns=['Digito', 'Renta_Norm', 'Anio_Cruce', 'Mes_Cruce', 'Quincena_Deducida'])
    else: df_ops_cruce = df_ops

    df_audit = pd.merge(
        df_cal_empresa, df_ops_cruce,
        left_on=['digito_rif', 'Renta_Norm', 'Anio_Cruce', 'Mes_Cruce', 'quincena'],
        right_on=['Digito', 'Renta_Norm', 'Anio_Cruce', 'Mes_Cruce', 'Quincena_Deducida'],
        how='outer', indicator=True
    )
    
    df_audit['Fase_Fiscal'] = df_audit['Tipo_Contribuyente_Esperado'].fillna(df_audit['Tipo_Contribuyente']).str.capitalize()
    df_audit['Frecuencia'] = df_audit['quincena'].fillna(df_audit['Quincena_Deducida'])
    
    # 4. RESOLUCIÓN DE ESTATUS
    def determinar_estatus(row):
        # 🔥 NUEVO: Exonerar automáticamente si la fila fue marcada como sustitutiva
        if pd.notna(row.get('Es_Sustitutiva')) and row.get('Es_Sustitutiva'):
            return '🟢 Exonerado (Sustitutiva)'

        estatus_base = ""
        if row['_merge'] == 'left_only': 
            if row['Renta_Norm'] == 'ANTICIPO-ISLR':
                veces_pagado = anticipos_por_mes.get((row['Anio_Cruce'], row['Mes_Cruce']), 0)
                if veces_pagado >= 1: return '🟢 Exonerado (Cubierto por Pago Único)'
            
            dias_hoy = (fecha_contingencia_dt - row['fecha_limite']).days
            if dias_hoy > 0: estatus_base = '❌ NO PAGADO (Vencido)'
            else: estatus_base = '⏳ Pendiente (Por Vencer)'
        elif row['_merge'] == 'right_only': 
            estatus_base = '⚠️ Pago Extra / Sin Regla'
        else:
            dias = (row['Fecha_Operacion_Real'] - row['fecha_limite']).days
            if dias > 0: estatus_base = '🔴 Pagado Tarde'
            elif dias < 0: estatus_base = '🟢 Pagado Adelantado'
            else: estatus_base = '🔵 Pagado Exacto'
            
            if row['Renta_Norm'] == 'ANTICIPO-ISLR':
                veces = anticipos_por_mes.get((row['Anio_Cruce'], row['Mes_Cruce']), 1)
                if veces == 1: estatus_base += f" (Único pago en {row['Frecuencia']})"

        return estatus_base

    df_audit['Estatus_Fiscal'] = df_audit.apply(determinar_estatus, axis=1)
    
    df_audit['Días de Desviación'] = np.where(
        df_audit['Estatus_Fiscal'].str.contains('Exonerado'),
        0, 
        np.where(
            df_audit['_merge'] == 'left_only',
            (fecha_contingencia_dt - df_audit['fecha_limite']).dt.days,
            (df_audit['Fecha_Operacion_Real'] - df_audit['fecha_limite']).dt.days
        )
    )

    col_monto = [c for c in df_audit.columns if 'monto' in c.lower() or 'total' in c.lower()]
    if col_monto: df_audit['Monto(Bs.)'] = pd.to_numeric(df_audit[col_monto[0]], errors='coerce').fillna(0.0)
    else: df_audit['Monto(Bs.)'] = 0.0

    # ==========================================
    # 4.5. CÁLCULO DE CONTINGENCIA (MULTAS EN BS Y EUROS)
    # ==========================================
    tasa_eur, fecha_bcv = obtener_tasa_bcv_euro()
    tasa_calculo = tasa_eur if tasa_eur else 0.0
    
    df_audit['Días_Mora_Real'] = np.where(df_audit['Días de Desviación'] > 0, df_audit['Días de Desviación'], 0)
    df_audit['Multa_Acumulada(Bs.)'] = df_audit['Monto(Bs.)'] * 0.05 * df_audit['Días_Mora_Real']
    
    df_audit['Multa_Art_103(EUR)'] = np.where(df_audit['Estatus_Fiscal'].str.contains('Pagado Tarde'), 100.0, 0.0)
    
    df_audit['Multa_Art_108(EUR)'] = np.where(
        (df_audit['Estatus_Fiscal'].str.contains('Pagado Tarde')) & (df_audit['Fase_Fiscal'].str.lower() == 'especial'),
        200.0,
        0.0
    )
    
    df_audit['Multas_Fijas(EUR)'] = df_audit['Multa_Art_103(EUR)'] + df_audit['Multa_Art_108(EUR)']
    df_audit['Multas_Fijas(Bs.)'] = df_audit['Multas_Fijas(EUR)'] * tasa_calculo
    
    df_audit['Total_Deuda(Bs.)'] = np.where(
        df_audit['Estatus_Fiscal'].str.contains('NO PAGADO|Pendiente'),
        df_audit['Monto(Bs.)'] + df_audit['Multa_Acumulada(Bs.)'] + df_audit['Multas_Fijas(Bs.)'],
        df_audit['Multa_Acumulada(Bs.)'] + df_audit['Multas_Fijas(Bs.)']
    )
    
    if tasa_calculo > 0:
        df_audit['Tasa_BCV_EUR'] = tasa_calculo
        df_audit['Deuda_Total(EUR)'] = (df_audit['Total_Deuda(Bs.)'] / tasa_calculo).round(2)
    else:
        df_audit['Tasa_BCV_EUR'] = 0.0
        df_audit['Deuda_Total(EUR)'] = "Sin conexión"

    # ==========================================
    # 5. Limpieza Visual Final y Creación de Columnas
    # ==========================================
    def display_impuesto(row):
        if row['Renta_Norm'] == 'ISLR-ANUAL': return 'ISLR (Declaración Anual)'
        if row['Renta_Norm'] == 'ISLR-RETENCION': return 'ISLR (Retención Mensual)'
        return row['Renta_Norm']
        
    df_audit['Impuesto'] = df_audit.apply(display_impuesto, axis=1)
    df_audit['Año'] = df_audit['Anio_Cruce'].astype(int)
    df_audit['Mes_Num_Final'] = df_audit['Mes_Cruce'].astype(int)
    
    if nombre_concepto in df_audit.columns: df_audit['Concepto Contable'] = df_audit[nombre_concepto].fillna('--- Sin Registro ---')
    else: df_audit['Concepto Contable'] = '--- Sin Registro ---'
        
    if 'Periodo_Original' in df_audit.columns: df_audit['Periodo Registrado en Excel'] = df_audit['Periodo_Original'].fillna('--- No Pagado ---')
    else: df_audit['Periodo Registrado en Excel'] = '--- No Pagado ---'
    
    meses_nombres = {1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre', 0: 'Desconocido', 99: 'Cierre Anual'}
    df_audit['Mes_Str'] = df_audit['Mes_Num_Final'].map(meses_nombres)
    
    df_audit['Periodo'] = df_audit['Mes_Str'] + ' ' + df_audit['Año'].astype(str)
    
    df_audit['Fecha Límite SENIAT'] = df_audit['fecha_limite'].dt.strftime('%d/%m/%Y').fillna('Sin Fecha Oficial')
    
    if 'Fecha_Operacion_Real' in df_audit.columns: df_audit['Fecha de Pago Real'] = df_audit['Fecha_Operacion_Real'].dt.strftime('%d/%m/%Y').fillna('--- No Pagado ---')
    else: df_audit['Fecha de Pago Real'] = '--- No Pagado ---'
        
    df_audit['Faltante'] = df_audit['Estatus_Fiscal'].str.contains('NO PAGADO|Pendiente')
    df_audit = df_audit.sort_values(by=['Faltante', 'Año', 'Mes_Num_Final', 'Impuesto', 'Frecuencia'])
    
    columnas_finales = [
        'Fase_Fiscal', 'Periodo', 'Impuesto', 'Frecuencia', 'Concepto Contable', 'Periodo Registrado en Excel', 
        'Fecha Límite SENIAT', 'Fecha de Pago Real', 'Monto(Bs.)', 'Estatus_Fiscal', 'Días de Desviación',
        'Multa_Acumulada(Bs.)', 'Multa_Art_103(EUR)', 'Multa_Art_108(EUR)', 'Multas_Fijas(Bs.)',
        'Total_Deuda(Bs.)', 'Tasa_BCV_EUR', 'Deuda_Total(EUR)'
    ]
    
    columnas_finales = [c for c in columnas_finales if c in df_audit.columns]
    
    df_reporte = df_audit[columnas_finales].rename(columns={'Días de Desviación': 'Días de Mora/Desviación'})
    
    info_expediente = {
        "digito": digito_usuario,
        "tipo": tipo_final_empresa,
        "rango_excel": periodo_str,
        "tipo_cierre": tipo_cierre_usuario,
        "duplicados_eliminados": duplicados_eliminados,
        "fecha_auditoria": fecha_contingencia_dt.strftime('%d/%m/%Y')
    }
    
    return df_reporte, df_cal_empresa, anios_presentes, info_expediente

def generar_excel_multitabla(df_detalles):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_detalles.to_excel(writer, index=False, sheet_name='Auditoría Completa')
        fases = df_detalles['Fase_Fiscal'].dropna().unique()
        for fase in fases:
            df_fase = df_detalles[df_detalles['Fase_Fiscal'] == fase]
            df_fase.to_excel(writer, index=False, sheet_name=f'Fase {fase}')
    processed_data = output.getvalue()
    return processed_data

# --- INTERFAZ DE STREAMLIT ---

st.set_page_config(page_title="Calculo Contingencia ", page_icon="⚖️", layout="wide")

st.sidebar.header("⚙️ Parámetros de Auditoría")

st.sidebar.subheader("🏢 Datos del Contribuyente")
digito_seleccionado = st.sidebar.number_input("Dígito Final del RIF", min_value=0, max_value=9, value=0, step=1)
tipo_cierre_seleccionado = st.sidebar.selectbox("Tipo de Cierre ISLR", options=["Ordinario", "Irregular"], index=0)

st.sidebar.subheader("📅 Evaluación de Contingencia")
fecha_calculo = st.sidebar.date_input("Fecha de Cálculo de Mora", datetime.now().date())

st.title("⚖️ Auditoría de Contingencias")
st.write("Cálculo de Contingencia por pagos tardados y omisiones (Incluye Multas COT Art. 103 y 108).")

df_calendario = obtener_calendario_db()
archivo_subido = st.file_uploader("Sube el archivo Excel de operaciones (.xlsx)", type=["xlsx"])

def renderizar_tablero_fase(titulo_fase, color_emoji, df_reporte_fase, df_cal_fase, anios):
    st.markdown(f"### {color_emoji} {titulo_fase}")
    if df_reporte_fase.empty and df_cal_fase.empty:
        st.info("No hay obligaciones ni pagos en esta fase para el periodo evaluado al corte actual.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Omitidos / Vencidos ❌", len(df_reporte_fase[df_reporte_fase['Estatus_Fiscal'].str.contains('NO PAGADO', na=False)]))
    col2.metric("Atrasados 🔴", len(df_reporte_fase[df_reporte_fase['Estatus_Fiscal'].str.contains('Pagado Tarde', na=False)]))
    col3.metric("A Tiempo / Exonerado 🟢", len(df_reporte_fase[df_reporte_fase['Estatus_Fiscal'].str.contains('Adelantado|Exacto|Exonerado', na=False)]))
    col4.metric("Extras / Sin Regla ⚠️", len(df_reporte_fase[df_reporte_fase['Estatus_Fiscal'].str.contains('Extra', na=False)]))

    st.markdown("##### ⏰ Resumen de Pagos Atrasados por Renta")
    df_morosos_fase = df_reporte_fase[df_reporte_fase['Estatus_Fiscal'].str.contains('Pagado Tarde', na=False)]
    if not df_morosos_fase.empty:
        resumen_morosos = df_morosos_fase.groupby('Impuesto').size().reset_index(name='Total Pagos Atrasados')
        st.dataframe(resumen_morosos, hide_index=True, use_container_width=True)
    else:
        st.success("¡Excelente! No hay pagos realizados fuera de fecha en esta fase.")

    st.markdown("<br>", unsafe_allow_html=True)

    impuestos_presentes = list(df_reporte_fase['Impuesto'].dropna().unique())
    pestanas = st.tabs(["📋 Auditoría Completa"] + [f"📂 {imp}" for imp in impuestos_presentes])
    
    def color_filas(val):
        if 'NO PAGADO' in str(val): return 'background-color: #ffcccc'
        elif 'Tarde' in str(val): return 'background-color: #ffdab9'
        elif 'Adelantado' in str(val) or 'Exacto' in str(val) or 'Exonerado' in str(val): return 'background-color: #cce8cf'
        elif 'Por Vencer' in str(val): return 'background-color: #fff3cd' 
        return ''
    
    with pestanas[0]:
        st.dataframe(df_reporte_fase.style.map(color_filas, subset=['Estatus_Fiscal']), use_container_width=True)
    for i, impuesto in enumerate(impuestos_presentes):
        with pestanas[i+1]:
            st.dataframe(df_reporte_fase[df_reporte_fase['Impuesto'] == impuesto].style.map(color_filas, subset=['Estatus_Fiscal']), use_container_width=True)

    st.markdown("#### 📅 Desglose de Cumplimiento por Año")
    for anio in sorted(anios):
        df_cal_anio = df_cal_fase[df_cal_fase['Anio_Cruce'] == anio]
        df_rep_anio = df_reporte_fase[df_reporte_fase['Periodo'].str.endswith(str(anio), na=False)]
        
        esperados_oficial = len(df_cal_anio)
        if esperados_oficial > 0:
            omitidos = df_rep_anio[df_rep_anio['Estatus_Fiscal'].str.contains('NO PAGADO|Pendiente', na=False)]
            omitidos_totales = len(omitidos)
            pagados = esperados_oficial - omitidos_totales
            
            icono_estado = "🟢" if omitidos_totales == 0 else "🔴"
            with st.expander(f"{icono_estado} Año {anio} — Exigidas al Corte: {esperados_oficial} | Cumplidas: {pagados} | Faltan: {omitidos_totales}"):
                if omitidos_totales > 0:
                    faltantes_resumen = omitidos.groupby(['Impuesto', 'Frecuencia']).size().reset_index(name='Declaraciones Faltantes / Vencidas')
                    st.write("**Detalle de Obligaciones Pendientes:**")
                    st.dataframe(faltantes_resumen, hide_index=True, use_container_width=True)
                else:
                    st.success(f"¡Excelente! Cumpliste todas las {esperados_oficial} obligaciones exigidas hasta la fecha de corte.")

if archivo_subido is not None and not df_calendario.empty:
    with st.spinner('Construyendo calendario tributario y cruzando información...'):
        df_empresa = pd.read_excel(archivo_subido, header=3)
        
        df_reporte, df_cal_esperado, anios, info = procesar_auditoria_completa(
            df_empresa, df_calendario, digito_seleccionado, fecha_calculo, tipo_cierre_seleccionado
        )
        
        if info['duplicados_eliminados'] > 0:
            st.warning(f"🧹 **Limpieza Automática:** Se detectaron y eliminaron {info['duplicados_eliminados']} registros duplicados en el Excel (compartían el mismo Número de Documento y Tipo de Impuesto).")

        st.markdown(f"## 📋 Expediente de Auditoría")
        st.info(f"📅 **Corte de Auditoría:** {info['fecha_auditoria']} | **Periodo Declarado (Excel):** {info['rango_excel']}")
        
        c1, c2, c3 = st.columns(3)
        c1.metric("🏢 Último Dígito del RIF ", f" {info['digito']}")
        c2.metric("📝 Estatus Fiscal", info['tipo'])
        c3.metric("⚙️ Tipo Cierre", info['tipo_cierre'])
        st.markdown("---")
        
        fases_presentes = set(df_reporte['Fase_Fiscal'].dropna().unique())
        
        if 'Ordinario' in fases_presentes and 'Especial' in fases_presentes:
            st.warning("⚠️ **TRANSICIÓN DETECTADA:** El contribuyente mutó de Ordinario a Especial. Selecciona la pestaña para ver el análisis de cada fase con vista completa.")
            
            tab_ord, tab_esp = st.tabs(["📘 Fase Ordinaria", "📙 Fase Especial"])
            
            with tab_ord:
                df_rep_ord = df_reporte[df_reporte['Fase_Fiscal'] == 'Ordinario']
                df_cal_ord = df_cal_esperado[df_cal_esperado['Tipo_Contribuyente_Esperado'] == 'ordinario']
                renderizar_tablero_fase("Análisis: Fase Ordinaria", "📘", df_rep_ord, df_cal_ord, anios)
                
            with tab_esp:
                df_rep_esp = df_reporte[df_reporte['Fase_Fiscal'] == 'Especial']
                df_cal_esp = df_cal_esperado[df_cal_esperado['Tipo_Contribuyente_Esperado'] == 'especial']
                renderizar_tablero_fase("Análisis: Fase Especial", "📙", df_rep_esp, df_cal_esp, anios)
                
            st.markdown("---")
            
        else:
            fase_unica = list(fases_presentes)[0] if fases_presentes else "Desconocida"
            renderizar_tablero_fase(f"Reporte Único: Contribuyente {fase_unica}", "📊", df_reporte, df_cal_esperado, anios)
            st.markdown("---")

        st.markdown("### 📊 RESUMEN GLOBAL DE MULTAS / MORA")
        
        tasa_oficial, fecha_oficial = obtener_tasa_bcv_euro()
        if tasa_oficial:
            st.success(f"💶 **Tasa BCV (EUR) Aplicada:** Bs. {tasa_oficial} | **Última actualización:** {fecha_oficial} (Vía pyDolarVenezuela)")
        
        col_t1, col_t2, col_t3 = st.columns(3)
        col_t1.metric("Total Omitidos / Vencidos ❌", len(df_reporte[df_reporte['Estatus_Fiscal'].str.contains('NO PAGADO', na=False)]))
        col_t2.metric("Total Pagos Atrasados 🔴", len(df_reporte[df_reporte['Estatus_Fiscal'].str.contains('Pagado Tarde', na=False)]))
        col_t3.metric("Total Pagos Extras ⚠️", len(df_reporte[df_reporte['Estatus_Fiscal'].str.contains('Extra', na=False)]))
        
        st.markdown("#### 💰 Desglose del Impacto Financiero (Contingencia COT)")
        
        monto_principal_bs = df_reporte[df_reporte['Estatus_Fiscal'].str.contains('NO PAGADO|Pendiente')]['Monto(Bs.)'].sum()
        total_mora_bs = df_reporte['Multa_Acumulada(Bs.)'].sum()
        total_art103_eur = df_reporte['Multa_Art_103(EUR)'].sum()
        total_art108_eur = df_reporte['Multa_Art_108(EUR)'].sum()
        
        tasa_uso = tasa_oficial if tasa_oficial else 1.0 
        
        total_art103_bs = total_art103_eur * tasa_uso
        total_art108_bs = total_art108_eur * tasa_uso
        total_deuda_bs = df_reporte['Total_Deuda(Bs.)'].sum()
        total_deuda_eur = pd.to_numeric(df_reporte['Deuda_Total(EUR)'], errors='coerce').sum()

        df_desglose = pd.DataFrame({
            "Concepto de la Contingencia": [
                "🏛️ 1. Deuda Principal (Impuestos Omitidos / Por Pagar)",
                "📈 2. Multas por Mora (5% Diario sobre el monto)",
                "📄 3. Fijas COT Art. 103 (100 EUR por pago tardío)",
                "🏢 4. Fijas COT Art. 108 (200 EUR adicionales - Especiales)",
                "🚨 TOTAL RIESGO ESTIMADO"
            ],
            "Monto Total (Bs.)": [
                monto_principal_bs,
                total_mora_bs,
                total_art103_bs,
                total_art108_bs,
                total_deuda_bs
            ],
            "Equivalente (EUR)": [
                monto_principal_bs / tasa_uso if tasa_oficial else 0,
                total_mora_bs / tasa_uso if tasa_oficial else 0,
                total_art103_eur,
                total_art108_eur,
                total_deuda_eur
            ]
        })

        df_desglose["Monto Total (Bs.)"] = df_desglose["Monto Total (Bs.)"].apply(lambda x: f"Bs. {x:,.2f}")
        df_desglose["Equivalente (EUR)"] = df_desglose["Equivalente (EUR)"].apply(lambda x: f"€ {x:,.2f}")

        st.dataframe(df_desglose, hide_index=True, use_container_width=True)

        st.markdown("---")
        
        excel_data = generar_excel_multitabla(df_reporte)
        st.download_button(
            label="📥 Descargar Reporte de Auditoría (Excel)", 
            data=excel_data, 
            file_name=f"Auditoria_Seniat_{info['digito']}_{info['fecha_auditoria'].replace('/','-')}.xlsx", 
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="btn_descargar_auditoria" 

        )
