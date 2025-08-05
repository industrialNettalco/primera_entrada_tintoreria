import pandas as pd
import db_data
import warnings
import os
import json
from openai import OpenAI
from google import genai
import google.generativeai as genai
from dotenv import load_dotenv
import streamlit as st

# Cargar las variables de entorno desde el archivo .env
warnings.filterwarnings("ignore")
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)

DEEPSEEK_MODEL_V3 = "deepseek-chat"
GEMINI_MODEL_2_0_FLASH =  "gemini-2.0-flash"
GEMINI_MODEL_2_5_FLASH = "gemini-2.5-flash"

def get_observation(cod_agrp):
    observations_df = db_data.get_observation_df(cod_agrp)
    if observations_df.empty:
        return ""
    
    observations_df['TFECHINGR'] = pd.to_datetime(observations_df['TFECHINGR'])
    last_data = observations_df['TFECHINGR'].idxmax()
    observation = observations_df.loc[[last_data]]
    return observation['TOBSVLIBE'].values[0]

def set_dfs(ol):
    ol_est_df = db_data.ol_df(ol)
    if ol_est_df.empty:
        st.error("OL no valido o sin datos en órdenes de laboratorio")
        return pd.DataFrame(),0,0,0,0
    st.markdown("Datos encontrados para el OL ingresado: OL EST")
    st.markdown(pd.DataFrame(ol_est_df).T.to_markdown(index=True))

    recipe = db_data.get_recipe_from_carton_laboratorio(str(ol_est_df['EP']),ol_est_df['CODIGO_COLOR'])

    if recipe is None:
        st.markdown("No se encontraron coincidencias con EP y Color")
        recipe = db_data.get_recipe_from_carton_laboratorio_just_color(ol_est_df['CODIGO_COLOR'])
        if recipe is None:
            recipe = db_data.get_recipe_from_color_master(ol_est_df['CODIGO_COLOR'])
            if recipe is None:
                st.warning("No hay labDip en Color Master")
                return pd.DataFrame(),0,0,0,0
        else:
            st.markdown("Se encontraron datos coincidentes solo por Color en cartón de laboratorio")
    else:
        st.markdown("Se encontraron datos coincidentes con EP y Color! en cartón de laboratorio")

    recipe_df = db_data.recipe_data_df(recipe)
    receta_base_df = recipe_df.iloc[0]
    st.markdown("Datos encontrados para la receta base: RECETA BASE")
    st.markdown(pd.DataFrame(receta_base_df).T.to_markdown(index=True))
    tricromia_df = db_data.colorante_df(recipe_df['TCODIRECE'][0])


    tricromia_df['TCODIAGRP'] = tricromia_df['TCODIPROD'].str[1:].apply(db_data.codi_agru)
    receta_colorantes_df = tricromia_df
    st.markdown("Tricromía para la receta base: RECETA COLORANTES BASE")
    st.markdown(receta_colorantes_df.to_markdown(index=True))

    tricromia_df['TOBS'] = tricromia_df['TCODIAGRP'].apply(lambda x: get_observation(x))
    comparacion_colorantes_df = tricromia_df[['TCODIPROD', 'TCODIAGRP', 'TDESCPROD', 'TOBS']]
    st.markdown("Observaciones para tricromía: COMPARACION COLORANTES EST")
    st.markdown(comparacion_colorantes_df.to_markdown(index=True))

    comparacion_lote_est_df = db_data.lote_std_df(str(ol_est_df['LOTE_STD']))
    st.markdown("COMPARACION LOTE STD")
    st.markdown(comparacion_lote_est_df.to_markdown(index=True))

    return ol_est_df, receta_base_df, receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df


def decide_by_observation_deepseek(receta_df, comparacion_df, comparacion_lote_df):
    client = OpenAI(api_key=os.getenv("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com")

    receta_df = receta_df[['TCODIPROD','TCODIAGRP', 'TDESCPROD', 'COLORANTE_AJUSTADO']]
    tabla_receta_colorantes = receta_df.to_markdown(index=False)
    tabla_comparacion_colorantes = comparacion_df.to_markdown(index=False)
    tabla_comparacion_lote = comparacion_lote_df.to_markdown(index=False)

    context = f"""
    OBJETIVO: Ajustar automáticamente los valores de 'COLORANTE_AJUSTADO' en 'receta_colorantes_df' basado en observaciones de 2 tablas de referencia y devolver SOLO la tabla final con los resultados.

    --- TABLAS DE ENTRADA ---
    1. RECETA COLORANTES BASE: Contiene los colorantes a ajustar (COLORANTE_AJUSTADO) y sus descripciones (TDESCPROD).
    {tabla_receta_colorantes}

    2. COMPARACION COLORANTES EST: Proporciona observaciones en columna 'TOBS' para ajustes.
    {tabla_comparacion_colorantes}

    3. COMPARACION LOTE STD: Proporciona ajustes estructurados en 3 columnas:
    - TINDIMATZ: Matiz a modificar (AM/AZ/RO/=).
    - TPORCMATZ: % de ajuste para el matiz.
    - TPORCINTE: % de ajuste para intensidad (afecta a TODOS los colorantes).
    {tabla_comparacion_lote}

    --- REGLAS DE AJUSTE ---
    A) INTENSIDAD (aplica a TODOS los colorantes):
    - Si TOBS contiene "% más/menos intenso" o sinónimos: aplicar inverso (ej: "+2%" -> -0.02).
    - Si TPORCINTE ≠ 0: usar ese % directamente.

    B) MATIZ (aplica SOLO al color mencionado):
    - Si TOBS contiene "% más/menos [color]": buscar en TDESCPROD y aplicar inverso.
    - Si TINDIMATZ ≠ "=": aplicar TPORCMATZ al matiz especificado.

    C) PRIORIDADES:
    1. Aplicar primero intensidad, luego matiz.
    2. Ignorar observaciones ambiguas o casos "Ok".
    3. sumar o restar cantidades no multiplicar, (ej: encontramos +3% intens. -> TCONCPROD - 0.03)

    d) MISMO LOTE:
    - En caso encuentres que tanto el IDLOTE_PADRAO y TLOTECOMP son el mismo aplicar ajuste, en caso sea distinto no aplicar

    --- FORMATO DE SALIDA ---
    RESPUESTA TEXTUAL ÚNICA:
    1. Explicación breve de cambios aplicados, señalar porcentajes encontrados y aplicados.
    2. TABLA FINAL con las siguientes columnas:
    - Todas las columnas originales de receta_colorantes_df.
    - 4 columnas añadidas:
        * INT_TOBS
        * MATIZ_TOBS
        * INT_LOTE
        * MATIZ_LOTE
    - COLUMNA FINAL: 'COL_FINAL' (resultado calculado).
    
    REQUISITOS:
    - NO incluir código.
    - Formatear la tabla para claridad (ancho de columnas ajustado).
    """

    print("contexto para la ia:")
    print(context)

    response = client.chat.completions.create(
        model= DEEPSEEK_MODEL_V3,
        messages=[
            {"role": "system", "content": context}
        ]
    )

    respuesta_texto = response.choices[0].message.content
    return respuesta_texto


def decide_by_observation_gemini(receta_df, comparacion_df, comparacion_lote_df):
    model = genai.GenerativeModel(GEMINI_MODEL_2_5_FLASH)

    receta_df = receta_df[['TCODIPROD','TCODIAGRP', 'TDESCPROD', 'COLORANTE_AJUSTADO']]
    tabla_receta_colorantes = receta_df.to_markdown(index=False)
    tabla_comparacion_colorantes = comparacion_df.to_markdown(index=False)
    tabla_comparacion_lote = comparacion_lote_df.to_markdown(index=False)

    context = f"""
    OBJETIVO: Ajustar automáticamente los valores de 'COLORANTE_AJUSTADO' en 'receta_colorantes_df' basado en observaciones de 2 tablas de referencia y devolver SOLO la tabla final con los resultados.

    --- TABLAS DE ENTRADA ---
    1. RECETA COLORANTES BASE: Contiene los colorantes a ajustar (COLORANTE_AJUSTADO) y sus descripciones (TDESCPROD).
    {tabla_receta_colorantes}

    2. COMPARACION COLORANTES EST: Proporciona observaciones en columna 'TOBS' para ajustes.
    {tabla_comparacion_colorantes}

    3. COMPARACION LOTE STD: Proporciona ajustes estructurados en 3 columnas:
    - TINDIMATZ: Matiz a modificar (AM/AZ/RO/=).
    - TPORCMATZ: % de ajuste para el matiz.
    - TPORCINTE: % de ajuste para intensidad (afecta a TODOS los colorantes).
    {tabla_comparacion_lote}

    --- REGLAS DE AJUSTE ---
    A) INTENSIDAD (aplica a TODOS los colorantes):
    - Si TOBS contiene "% más/menos intenso" o sinónimos: aplicar inverso (ej: "+2%" -> -0.02).
    - Si TPORCINTE ≠ 0: usar ese % directamente.

    B) MATIZ (aplica SOLO al color mencionado):
    - Si TOBS contiene "% más/menos [color]": buscar en TDESCPROD y aplicar inverso.
    - Si TINDIMATZ ≠ "=": aplicar TPORCMATZ al matiz especificado.

    C) PRIORIDADES:
    1. Aplicar primero intensidad, luego matiz.
    2. Ignorar observaciones ambiguas o casos "Ok".
    3. sumar o restar cantidades no multiplicar, (ej: encontramos +3% intens. -> TCONCPROD - 0.03)

    d) MISMO LOTE:
    - En caso encuentres que tanto el IDLOTE_PADRAO y TLOTECOMP son el mismo aplicar ajuste, en caso sean distintos no aplicar

    --- FORMATO DE SALIDA ---
    RESPUESTA TEXTUAL ÚNICA:
    1. Explicación breve de cambios aplicados, señalar porcentajes encontrados y aplicados.
    2. TABLA FINAL con las siguientes columnas:
    - Todas las columnas originales de receta_colorantes_df.
    - 4 columnas añadidas:
        * INT_TOBS
        * MATIZ_TOBS
        * INT_LOTE
        * MATIZ_LOTE
    - COLUMNA FINAL: 'COL_FINAL' (resultado calculado).
    
    REQUISITOS:
    - NO incluir código.
    - Formatear la tabla para claridad (ancho de columnas ajustado).
    """

    print("contexto para la ia:")
    print(context)
    response = model.generate_content(context)
    respuesta_texto = response.text
    return respuesta_texto


# ------------------------------------ Interfaz ----------------------------------------
st.set_page_config(page_title="1er Matizado", page_icon="📊", layout="wide")
st.set_option('deprecation.showPyplotGlobalUse', False)
st.title("Análisis de Matizado - Primera Entrada")

col1, col2 = st.columns([2, 5])  # 4/5 del ancho para el input, 1/5 para el botón

with col1:
    user_input = st.text_input(
        "Ingrese un código OL válido:", 
        placeholder="Escriba aquí...",
        key="user_input_field",
    )

if user_input: 
    ol_est_df, receta_base_df, receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df = set_dfs(user_input) # 103629, 
    
    if not ol_est_df.empty:
        # Ajustar relacion de Baño
        receta_colorantes_df['COLORANTE_AJUSTADO'] = receta_colorantes_df['TCONCPROD'] * (1 + (float(ol_est_df['RB']) - receta_base_df['TRELABANO']) / 100)
        st.markdown("Colorante ajustado por Relación de Baño")
        st.markdown(receta_colorantes_df.to_markdown(index=True))

        if (len(receta_colorantes_df) <= 20 and len(comparacion_colorantes_df) <= 20):
            #chat_response = decide_by_observation_deepseek(receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df)
            #print(chat_response)
            #valores = chat_response.split("```")
            #print("-------------------- fin de deepseek ------------------")
            with st.spinner("IA analizando un momento por favor..."):
                chat_response2 = decide_by_observation_gemini(receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df)
                st.markdown(chat_response2)
        else:
            st.error("Mucha data en tricromía o comparación de colorantes, Algo Salio Mal")

