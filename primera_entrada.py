import pandas as pd
import db_data2
import warnings
import os
import json
from openai import OpenAI
from google import genai
import google.generativeai as genai
from dotenv import load_dotenv
import streamlit as st
import cx_Oracle
import pymysql

# --------------------- CONFIGURACIONES ------------------------

# Cargar las variables de entorno desde el archivo .env
warnings.filterwarnings("ignore")
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)

DEEPSEEK_MODEL_V3 = "deepseek-chat"
GEMINI_MODEL_2_0_FLASH =  "gemini-2.0-flash"
GEMINI_MODEL_2_5_FLASH = "gemini-2.5-flash"

DB_USER = os.getenv("DB_NET_USER")
DB_PASSWORD = os.getenv("DB_NET_PASSWORD")
DB_HOST = os.getenv("DB_NET_HOST")
DB_PORT = os.getenv("DB_NET_PORT")
DB_NAME = os.getenv("DB_NET_NAME")

DB_PRENDAS_USER = os.getenv("DB_PRENDAS_USER")
DB_PRENDAS_PASSWORD = os.getenv("DB_PRENDAS_PASSWORD")
DB_PRENDAS_HOST = os.getenv("DB_PRENDAS_HOST")
DB_PRENDAS_PORT = os.getenv("DB_PRENDAS_PORT")
DB_PRENDAS_NAME = os.getenv("DB_PRENDAS_NAME")

dsn = cx_Oracle.makedsn(DB_HOST, DB_PORT, sid=DB_NAME)
connection = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)

db_config = {
    'host': DB_PRENDAS_HOST,
    'port': int(DB_PRENDAS_PORT),
    'user': DB_PRENDAS_USER,
    'password': DB_PRENDAS_PASSWORD,
    'database': DB_PRENDAS_NAME,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_general_ci'
}

# -------------------------------------------------------------------

def get_recipes_by_colors(colors):
    recipes_df = pd.DataFrame()
    for color in colors:
        recipe_df = db_data2.get_recipes_complete(color)
        recipes_df = pd.concat([recipes_df, recipe_df], ignore_index=False)
    
    return recipes_df

def get_color_from_solidity(color):
    new_color = ""
    color_description = db_data2.get_description_color(color)
    print("descripcionoooonnononon ->", color_description)
    if color_description:
        if 'sol' in color_description.lower():
            print("entro al if")
            new_color = color_description[-5:]
    print(new_color)
    return new_color

def get_color_from_matching(color):
    new_color = ""
    color_description = db_data2.get_description_color(color)
    print("descripcionoooonnononon ->", color_description)
    if color_description:
        last_digits = color_description[-7:]
        print("last digits, ", last_digits)
        if not 'tcx' in last_digits.lower() and 'c' in last_digits.lower():
            print("entro al if")
            new_color = last_digits[-5:]
    print(new_color)
    return new_color

def get_recipes_by_color(color, receta):
    recipe_df = pd.DataFrame()
    #recipe_df = db_data2.get_recipe_from_carton_laboratorio(color)
    recipe_df = db_data2.get_recipes_complete(color)
    if not recipe_df.empty:
        recipe_df = recipe_df[recipe_df['TCODIRECE'] != receta]
    if recipe_df.empty:
        #recipe_df = db_data2.get_recipes_complete(color)
        if recipe_df.empty:
            solidity_color = get_color_from_solidity(color)
            if not solidity_color:
                machine_color = get_color_from_matching(color)
                if not machine_color:
                    st.warning("No se encontró similitud por color, solidez ni por código Matching")
                else:
                    recipe_df = db_data2.get_recipes_complete(machine_color)
                    print("colooor ->>>>>", machine_color, "otra cosa")
                    print(type(machine_color))
                    print(recipe_df)
                    st.markdown("Recetas No encontradas por Color, pero se encontró por CÓDIGO MATCHING")
            else:
                recipe_df = db_data2.get_recipes_complete(solidity_color)
                st.markdown("Recetas No encontradas por Color, pero se encontró por SOLIDEZ")
    else:
        st.markdown("Recetas encontradas con el mismo Color")

    return recipe_df

def get_data_from_recipes(recipes_df):
    recipes_df = recipes_df["TCODIRECE"]

    list_df = []
    for recipe in recipes_df:
        data_df = db_data2.recipe_data_df(recipe)
        list_df.append(data_df)
    
    data_df = pd.concat(list_df, ignore_index=False)
    return data_df

def filter_by_ep(data_df, ep):
    df = data_df.copy()
    data_df["TCODIARTI"] = data_df["TCODIARTI"].astype(str)
    ep_str = str(ep)
    data_by_ep = data_df[data_df["TCODIARTI"] == ep_str]
    if data_by_ep.empty:
        st.warning("No se encontró datos con EP similar")
        return df, False
    else:
        st.markdown("Datos filtrados por similaridad de EP:")
        st.dataframe(data_by_ep)
        return data_by_ep, True

def filter_by_repro(data_df):
    df = data_df.copy()
    df = df[~df['TRECE_SEQ'].str.contains('REPRO', case=False, na=False)]
    df = df[~df['TAUXIRECE'].str.contains('REPRO', case=False, na=False)]
    df = df[df['TCODIRECE'].str.startswith('SL')]
    if df.empty:
        return data_df
    st.markdown("filtramos solo los SL")
    st.dataframe(df)
    return df

def filter_by_lote(data_df, lote_std, flag):
    df = data_df.copy()
    data_df["TCODILOTE"] = data_df["TCODILOTE"].astype(str)
    lote_std_str = str(lote_std)
    data_by_lote_std = data_df[data_df["TCODILOTE"] == lote_std_str]
    if data_by_lote_std.empty:
        if flag == False:
            st.markdown("No se encontró datos con LOTE")
            return df
        lote_comp = get_lote_comp_from_data(lote_std)
        data_by_lote_comp = filter_by_lote(data_df, lote_comp, False)
        return data_by_lote_comp
    else:
        st.markdown("Datos filtrados por LOTE estandar:")
        st.dataframe(data_by_lote_std)
        return data_by_lote_std

def get_lote_comp_from_data(lote_std):
    df = db_data2.lote_std_df(int(lote_std))
    lote_comp = df['TLOTECOMP'].iloc[0]
    return lote_comp

def filter_by_rb(data_df, rb):
    df = data_df.copy()
    st.markdown("Datos filtrado por RB:")
    data_df["TRELABANO"] = data_df["TRELABANO"].astype(int)
    rb_int = int(rb)
    data_less_df = data_df[data_df["TRELABANO"] <= 8]
    if not data_less_df.empty:
        min_value = data_less_df["TRELABANO"].min()
        new_data_df = data_less_df[data_less_df["TRELABANO"] == min_value]
        st.dataframe(new_data_df)
        return new_data_df
    else:
        data_df["DIFERENCIA"] = (data_df["TRELABANO"] - rb_int).abs()
        min_diff = data_df["DIFERENCIA"].min()
        closest_df = data_df[data_df["DIFERENCIA"] == min_diff]
        closest_df = closest_df.drop(columns=["DIFERENCIA"])
        st.dataframe(closest_df)
        return closest_df

def filter_by_stage(data_df):
    st.markdown("Datos filtrados por Etapa de Receta:")
    filtro1 = data_df[(data_df['TESTARECE'] == 'G') & (data_df['TTIPORECE'] == 'P')]
    if not filtro1.empty:
        st.markdown("filtrado por recetas en producción")
        st.dataframe(filtro1)
        return filtro1
    else:
        mask_lab_dip = data_df['TRECE_SEQ'].str.contains(r'^la', case=False, na=False, regex=True)
        filtro2 = data_df[mask_lab_dip]
        if not filtro2.empty:
            st.markdown("filtrado por recetas en LAB DIP")
            st.dataframe(filtro2)
            return filtro2
        else:
            mask_muestra = data_df['TRECE_SEQ'].str.contains('MUESTRA', case=False, na=False)
            filtro3 = data_df[mask_muestra]
            if not filtro3.empty:
                st.markdown("filtrado por recetas en MUESTRA")
                st.dataframe(filtro3)
                return filtro3
            else:
                mask_desarrollo = data_df['TRECE_SEQ'].str.contains('DESARROLLO', case=False, na=False)
                filtro4 = data_df[mask_desarrollo]
                if not filtro4.empty:
                    st.markdown("filtrado por recetas en DESARROLLO")
                    st.dataframe(filtro4)
                    return filtro4
                else:
                    st.markdown("No se pudo filtrar por etapas")
                    return data_df

def filter_by_stage_labdip(data_df):
    st.markdown("Datos filtrados por Etapa de Receta:")
    
    mask_lab_dip = data_df['TRECE_SEQ'].str.contains(r'^la', case=False, na=False, regex=True)
    filtro1 = data_df[mask_lab_dip]
    if not filtro1.empty:
        st.markdown("filtrado por recetas en LAB DIP")
        st.dataframe(filtro1)
        return filtro1
    else:
        filtro2 = data_df[(data_df['TESTARECE'] == 'G') & (data_df['TTIPORECE'] == 'P')]
        if not filtro2.empty:
            st.markdown("filtrado por recetas en producción")
            st.dataframe(filtro2)
            return filtro2
        else:
            mask_muestra = data_df['TRECE_SEQ'].str.contains('MUESTRA', case=False, na=False)
            filtro3 = data_df[mask_muestra]
            if not filtro3.empty:
                st.markdown("filtrado por recetas en MUESTRA")
                st.dataframe(filtro3)
                return filtro3
            else:
                mask_desarrollo = data_df['TRECE_SEQ'].str.contains('DESARROLLO', case=False, na=False)
                filtro4 = data_df[mask_desarrollo]
                if not filtro4.empty:
                    st.markdown("filtrado por recetas en DESARROLLO")
                    st.dataframe(filtro4)
                    return filtro4
                else:
                    st.markdown("No se pudo filtrar por etapas")
                    return data_df

def filter_by_date(data_df):
    st.markdown("Datos filtrados por Fecha más reciente:")
    data_df['TFECHACTU'] = pd.to_datetime(data_df['TFECHACTU'])
    df_mas_reciente = data_df.sort_values('TFECHACTU', ascending=False).head(1)
    df_mas_reciente = df_mas_reciente.reset_index(drop=True)
    st.dataframe(df_mas_reciente)
    return df_mas_reciente

def set_good_colors(data_df):
    for idx, row in data_df.iterrows():
        if pd.notna(row["TCODIAGRP"]):
            if row["TDESCPROD"].startswith('NO'):
                colors = db_data2.get_colors_from_cod_agr(row["TCODIAGRP"])
                good_color = colors[colors["TDESCITEM"].str.startswith('SY')]
                if not good_color.empty:
                    new_code = 'S' + good_color["TCODIALTR"].iloc[0]
                    new_desc = good_color["TDESCITEM"].iloc[0]
                    if new_code != data_df.loc[idx, "TCODIPROD"]:
                        data_df.loc[idx, "FLAG_OBS"] = True
                    data_df.loc[idx, "TCODIPROD"] = new_code
                    data_df.loc[idx, "TDESCPROD"] = new_desc
                
    return data_df

def get_observation(cod_color):
    cod_color = cod_color[1:]
    print("codigo de colorante")
    observations_df = db_data2.get_observation_df(cod_color)
    if observations_df.empty:
        return ""
    
    observations_df['TFECHINGR'] = pd.to_datetime(observations_df['TFECHINGR'])
    last_data = observations_df['TFECHINGR'].idxmin()
    observation = observations_df.loc[[last_data]]
    return observation['TOBSVLIBE'].values[0]

def get_finals_dfs(recipe, ol_lote_std):
    tricromia_df = db_data2.colorante_df(recipe)
    tricromia_df['TCODIAGRP'] = tricromia_df['TCODIPROD'].str[1:].apply(db_data2.codi_agru)
    receta_colorantes_base_df = tricromia_df.copy()
    receta_colorantes_df = tricromia_df
    st.markdown("Tricromía para la receta base: RECETA COLORANTES BASE")
    st.dataframe(receta_colorantes_df)
    receta_colorantes_df['FLAG_OBS'] = False
    receta_colorantes_df = set_good_colors(receta_colorantes_df)
    st.markdown("Cambio por colorantes que NO digan NO USAR")
    st.dataframe(receta_colorantes_df)

    tricromia_df['TOBS'] = tricromia_df['TCODIPROD'].apply(lambda x: get_observation(x))
    comparacion_colorantes_df = tricromia_df[['TCODIPROD', 'TCODIAGRP', 'TDESCPROD', 'TOBS']]
    st.markdown("Observaciones para tricromía: COMPARACION COLORANTES EST")
    st.dataframe(comparacion_colorantes_df)

    comparacion_lote_est_df = db_data2.lote_std_df(int(ol_lote_std))
    st.markdown("COMPARACION LOTE STD")
    st.dataframe(comparacion_lote_est_df)

    return receta_colorantes_base_df, receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df

def decide_by_observation_gemini_four(colorantes_df, comparacion_lote_df, lote_receta):
    model = genai.GenerativeModel(GEMINI_MODEL_2_5_FLASH)
    tabla_colorantes = colorantes_df[["TCODIPROD", "TDESCPROD", "TCONCPROD", "AJUTE_RB", "CONC_RB"]]
    tabla_colorantes = tabla_colorantes.to_markdown(index=False)
    list_observations = colorantes_df[colorantes_df["FLAG_OBS"] == True]["TOBS"].tolist()
    
    color_observations = "Sin ajuste"
    if len(list_observations) != 0:
        color_observations = ", ".join(list_observations)

    lote_intensidad = "Sin ajuste de intensidad"
    lote_matiz = "Sin ajuste de matiz"
    
    if comparacion_lote_df['TLOTECOMP'].iloc[0] is not None and comparacion_lote_df['TLOTECOMP'].iloc[0] == lote_receta:
        if comparacion_lote_df['TPORCINTE'].iloc[0] != 0:
            valor = comparacion_lote_df['TPORCINTE'].iloc[0]
            valor = valor * -1
            lote_intensidad = "aplicar " + str(valor) + "% de intensidad"
        
        if comparacion_lote_df['TINDIMATZ'].iloc[0].strip() != '=':
            color_matiz = comparacion_lote_df['TINDIMATZ'].iloc[0].strip()
            valor_matiz = comparacion_lote_df['TPORCMATZ'].iloc[0]
            valor_matiz = valor_matiz * -1
            lote_matiz = "aplicar " + str(valor_matiz) + "% al color " + color_matiz

    context = f"""
    OBJETIVO: Ajustar automáticamente los valores de 'CONC_RB' basado en observaciones de tablas de referencia y devolver la tabla final con los resultados.

    --- TABLAS DE ENTRADA ---
    1. TABLA DE COLORANTES: Contiene los colorantes ajustados, a esta tabla se le aplicaran los ajuste

    --- REGLAS DE AJUSTE ---
    A) INTENSIDAD:
    - Aplicar ajuste a TODOS los colorantes

    B) MATIZ:
    - solo para el color mencionado
    - Tener en cuenta lo siguiente al aplicar ajuste:
        - Si hay 2 colores similares ejemplo 1 rojo y otro rojo vino y el ajuste dice 3% +rojo, entonces aplicar solo al primer rojo, no deberia aplicar al rojo vino
        - Si en caso diga 2% +rojo y en los colorantes no hay ninguno que diga solo rojo, pero hay otro que dice rojo vino, aplicar a este al ser el más cercano

    C) PRIORIDADES:
    1. Aplicar primero intensidad, luego matiz.
    2. Habrá ajuste por comparacion de colorantes y por comparacion de lote
    3. Para todos los ajustes, tanto matiz como intensidad, aplicar el inverso (ej: 2% +rojo -> se debe aplicar -2% rojo)
    4. En el ajuste de comparacion de lote si aplicar lo que indica

    **IMPORTANTE** Aplicar: %Colorante Estimado = %Colorante Ajustado RB × (1 ± x%)  

    --- FORMATO DE SALIDA ---
    RESPUESTA TEXTUAL ÚNICA (NO código):
    1. Explicación BREVE y RESUMIDA de cambios aplicados.
    2. Mencionar si se aplica ajuste por colorantes, lote, ambos o ninguno; esto en letras grandes y/o en negrita
    2. TABLA FINAL con las siguientes columnas:
    - Todas las columnas originales de tabla de colorantes más 2 columnas de matiz e intensidad que se aplicarán:
        * AJUSTE_INTENSIDAD
        * AJUSTE_MATIZ
    - COLUMNA FINAL: 'COL_FINAL' (resultado calculado).
        **IMPORTANTE** Los valores deben redondearse a 4 decimales en todos los casos, en caso tenga menos decimales llenar con ceros.

    ---------------------------------
    *TABLA DE COLORANTES*
    {tabla_colorantes}

    *AJUSTE COMPARACION DE COLORANTES*
    {color_observations}

    *AJUSTE COMPARACION DE LOTE*
    {lote_intensidad}
    {lote_matiz}
    """

    print("contexto para la ia:")
    print(context)
    response = model.generate_content(context)
    respuesta_texto = response.text
    return respuesta_texto

def decide_by_observation_gemini_five(receta_colorantes_base_df, colorantes_df, comparacion_lote_df, lote_receta):
    model = genai.GenerativeModel(GEMINI_MODEL_2_5_FLASH)
    tabla_colorantes = colorantes_df[["TCODIPROD", "TDESCPROD", "TCONCPROD", "AJUTE_RB", "CONC_RB"]]
    tabla_colorantes = tabla_colorantes.to_markdown(index=False)
    list_observations = colorantes_df.loc[colorantes_df["FLAG_OBS"] == True, "TOBS"]

    lista_lotes = []
    print("revisamos si encontramos lotes")
    for index, observacion in list_observations.items():
        if 'lote' in str(observacion).lower() and '%' in str(observacion):
            fila_temp = receta_colorantes_base_df.iloc[index]
            cod_agrup = fila_temp['TCODIAGRP']
            cod_color = fila_temp['TCODIPROD'][1:]
            print("fila de colorante")
            print("COD_AGRP:", cod_agrup, " - COD_COLOR:", cod_color)

            lista_lotes = db_data2.get_lotes_df(int(cod_agrup), str(cod_color))
            print("lista de lotes -> ", lista_lotes)
    
    color_observations = "Sin ajuste"
    if len(list_observations) != 0:
        color_observations = ", ".join(list_observations)

    lote_intensidad = "Sin ajuste de intensidad"
    lote_matiz = "Sin ajuste de matiz"
    
    if comparacion_lote_df['TLOTECOMP'].iloc[0] is not None and comparacion_lote_df['TLOTECOMP'].iloc[0] == lote_receta:
        if comparacion_lote_df['TPORCINTE'].iloc[0] != 0:
            valor = comparacion_lote_df['TPORCINTE'].iloc[0]
            valor = valor * -1
            lote_intensidad = "aplicar " + str(valor) + "% de intensidad"
        
        if comparacion_lote_df['TINDIMATZ'].iloc[0].strip() != '=':
            color_matiz = comparacion_lote_df['TINDIMATZ'].iloc[0].strip()
            valor_matiz = comparacion_lote_df['TPORCMATZ'].iloc[0]
            valor_matiz = valor_matiz * -1
            lote_matiz = "aplicar " + str(valor_matiz) + "% al color " + color_matiz

    context = f"""
    OBJETIVO: Ajustar automáticamente los valores de 'CONC_RB' basado en observaciones de tablas de referencia y devolver la tabla final con los resultados.

    --- TABLAS DE ENTRADA ---
    1. TABLA DE COLORANTES: Contiene los colorantes ajustados, a esta tabla se le aplicaran los ajuste

    --- REGLAS DE AJUSTE ---
    A) Tabla comparación de colorantes
    Encontrarás 0 o más observaciones, lo que debes hacer es leer las observaciones y notar si hay ajuste por intensidad, matiz o lote
     1. Intensidad: ejemplo (2% + int)
        - Aplicar ajuste al colorante donde se encontró la observación, obtener la fila de la observación y aplicar ajuste a esa misma fila de la tabla
     2. Matiz: ejemplo (3% +azul)
        - Aplicar ajuste solo a un color, al color más cercano a la observación, ejemplo:
        - Si hay 2 colores similares 1 rojo y otro rojo vino y el ajuste dice 3% +rojo, entonces aplicar solo al color rojo, no deberia aplicar al rojo vino
        - Si en caso diga 2% +rojo y en los colorantes no hay ninguno que diga solo rojo, pero hay otro que dice rojo vino, aplicar a este al ser el más cercano
     3. Lote: ejemplo (2% +azul que el lote ...)
        - Debe mencionar 2 LOTES en observación y hacer un COMPARATIVO de lotes, un lote base y uno comparado, deben estar AMBOS LOTES como comparativo, en caso no estén 2 lotes como comparativo, entonces no es un ajuste por lote.
        - Hay una tabla adicional "lista de lotes" al final que muestra una lista de lotes, si alguno de esos lotes coincide con el lote COMPARADO mencionado en la observación entonces aplicar ajuste ya sea de matiz o intensidad 
    
    B) Tabla comparación de Lote 
     1. Intensidad
     - Aplicar ajuste a TODOS los colorantes
     2. Matiz
     - Aplicar ajuste solo a 1 color, al color más cercano a la observación, aplicar misma lógica que ajuste de matiz de comparación de colorantes:

    C) PRIORIDADES:
    1. Aplicar primero intensidad, luego matiz.

    **IMPORTANTE** 
    - Siempre aplicar el inverso de lo que se encuentra (ej: 2% +rojo -> se debe aplicar -2% rojo)
    - Aplicar: %Colorante Estimado = %Colorante Ajustado RB × (1 ± x%)  

    --- FORMATO DE SALIDA ---
    RESPUESTA TEXTUAL ÚNICA (NO código):
    1. Explicación BREVE y RESUMIDA de cambios aplicados.
    2. Mencionar si se aplica ajuste por colorantes, lote, ambos o ninguno; esto en letras grandes y/o en negrita
    2. TABLA FINAL con las siguientes columnas:
    - Todas las columnas originales de tabla de colorantes más 2 columnas de matiz e intensidad que se aplicarán:
        * AJUSTE_INTENSIDAD
        * AJUSTE_MATIZ
    - COLUMNA FINAL: 'COL_FINAL' (resultado calculado).
        **IMPORTANTE** Los valores deben redondearse a 4 decimales en todos los casos, en caso tenga menos decimales llenar con ceros.

    ---------------------------------
    *TABLA DE COLORANTES*
    {tabla_colorantes}

    *AJUSTE COMPARACION DE COLORANTES*
    {color_observations}

    *AJUSTE COMPARACION DE LOTE*
    {lote_intensidad}
    {lote_matiz}

    *Adicional: Lista de lotes
    {lista_lotes}
    """

    print("contexto para la ia:")
    print(context)
    response = model.generate_content(context)
    respuesta_texto = response.text
    return respuesta_texto

if "manual_ol_df" not in st.session_state:
    st.session_state.manual_ol_df = pd.DataFrame()

if "use_manual_ol" not in st.session_state:
    st.session_state.use_manual_ol = False

def set_manual_ol():
    st.markdown("### Ingrese los datos de la OL manualmente")
    with st.form("manual_ol_form"):
        manual_receta = st.text_input("RECETA", placeholder="Ej. SL00158006", key="manual_receta")
        manual_codigo_color = st.text_input("CODIGO_COLOR", placeholder="Ej. 84207", key="manual_codigo_color")
        manual_ep = st.text_input("EP", placeholder="Ej. 987801", key="manual_ep")
        manual_lote_std = st.text_input("LOTE_STD", placeholder="Ej. 17363", key="manual_lote_std")
        manual_rb = st.text_input("RB", placeholder="Ej. 10", key="manual_rb")

        submitted = st.form_submit_button("Confirmar OL Manual")

        if submitted:
            # Verifica que todos los campos estén completos
            if not all([manual_receta, manual_codigo_color, manual_ep, manual_lote_std, manual_rb is not None]):
                st.error("Por favor, complete todos los campos.")
            else:
                # Crea el DataFrame 'ol' con los datos ingresados manualmente
                manual_ol = {
                    "RECETA": manual_receta,
                    "CODIGO_COLOR": manual_codigo_color,
                    "EP": manual_ep,
                    "LOTE_STD": manual_lote_std,
                    "RB": manual_rb
                }
                manual_ol_series = pd.Series(manual_ol)
                st.session_state.manual_ol_df = manual_ol_series
                st.session_state.use_manual_ol = True
                st.rerun()

def verify_user(user: str, pwd: str):
    try:
        cursor = connection.cursor()
        username = cursor.var(cx_Oracle.STRING)
        p_menserro = cursor.var(cx_Oracle.STRING)
        cursor.callproc("prc_login",[user, pwd, username, p_menserro])
        if p_menserro.getvalue():
            return p_menserro.getvalue()
        else:
            return username.getvalue(), "Verificacion Correcta"

    except Exception as e:
        print(e)
    finally:
        cursor.close()

def create_ol_proc(red_crudo, color, cliente, ep, pi, lote, rb):
    try:
        cursor = connection.cursor()
        ol = cursor.var(cx_Oracle.STRING)
        receta = cursor.var(cx_Oracle.STRING)
        p_mensavis = cursor.var(cx_Oracle.STRING)
        p_menserro = cursor.var(cx_Oracle.STRING)
        cursor.callproc("lbpkg_gestion_ols.prc_crea_ol_manual",[color, ep, red_crudo, lote, rb, cliente, pi, ol, receta, p_mensavis, p_menserro])
        print(p_mensavis.getvalue())
        print(p_menserro.getvalue())
        #print(ol.getvalue())
        #print(receta.getvalue())
        if p_menserro.getvalue():
            ol = p_menserro.getvalue().split("OL")[1].split(' ')[1]
            receta = p_menserro.getvalue().split("Receta")[1].split(' ')[1]
            return ol, receta
        else:
            return ol.getvalue(), receta.getvalue()

    except Exception as e:
        print(e)
    finally:
        cursor.close()

def es_valor_valido(valor):
    """
    Verifica si un valor es válido (no NaN, None, Null o 0)
    """
    # Verificar si es cero (incluyendo 0.0)
    if valor == 0:
        return False
    
    # Verificar NaN (funciona para float y numpy.nan)
    if pd.isna(valor):
        return False
    
    # Verificar None o Null
    if valor is None:
        return False
    
    if str(valor).strip() == "":
        return False
    
    # Si pasó todas las verificaciones, es válido
    return True

def asignar_valores(row):
    if 'OL' in row and 'RECETA' in row:
        return row['OL'], row['RECETA']
    elif es_valor_valido(row["TREDUCRUD"]) and es_valor_valido(row["Color"]) and es_valor_valido(row["TCODICLIE"]) and es_valor_valido(row["EP"]) and es_valor_valido(row["PI"]) and es_valor_valido(row["Lote"]) and es_valor_valido(row["RB"]):
        print("validooooo con el color:", row["Color"])
        return create_ol_proc(row["TREDUCRUD"], row["Color"], row["TCODICLIE"], row["EP"], row["PI"], row["Lote"], row["RB"])
        #return "111", "SL00222"
    else:
        print("noooooo val:", row["Color"])
        return " ", " "

def create_ols(uploaded_file):
    if uploaded_file is not None:
        # Determinar el tipo de archivo y cargarlo adecuadamente
        try:
            if uploaded_file.type == "text/csv":
                # Es un archivo CSV
                df = pd.read_csv(uploaded_file, dtype={'Color': str, 'Lote': str, 'TCODICLIE': str})
                st.success("Archivo CSV cargado correctamente!")
            elif uploaded_file.type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                    "application/vnd.ms-excel"]:
                # Es un archivo Excel (.xlsx o .xls)
                df = pd.read_excel(uploaded_file, dtype={'Color': str, 'Lote': str, 'TCODICLIE': str})
                st.success("Archivo Excel cargado correctamente!")
            else:
                st.error("Formato de archivo no soportado")
            
            df[['OL', 'RECETA']] = df.apply(asignar_valores, axis=1, result_type='expand')
            st.session_state.ols_df = df
            
        except Exception as e:
            st.error(f"Error al cargar el archivo: {str(e)}")

def write_ol_mariadb(ol):
    try:
        conn = pymysql.connect(**db_config)

        # Primero se verifica que la OL no exista, esto evita que se suba la misma OL en cada momento
        check_query = "SELECT * FROM prdoolsstemp WHERE TCODIOL = %s"
        check_df = pd.read_sql(check_query, conn, params=(ol,))
        if check_df.empty:
            cursor = conn.cursor()

            query = """
            INSERT INTO prdoolsstemp (TCODIOL) 
            VALUES (%s)
            """
            cursor.execute(query, (ol))
            conn.commit()

            cursor.close()
            print("ol", ol, " Subido exitosamente")
        else:
            print("ol", ol, " ignorada")
        conn.close()

    except Exception as e:
        print(e)

def get_ols_from_mariadb():
    try:
        conn = pymysql.connect(**db_config)
        query = "SELECT * FROM prdoolsstemp"
        df = pd.read_sql(query, conn)
        conn.close()
        ols = df["TCODIOL"].to_numpy()
        return ols
    except Exception as e:
        print(e)


def show_frontend():
    col1, col2 = st.columns([2, 5])  # 4/5 del ancho para el input, 1/5 para el botón

    user_input = None
    if st.session_state.ol_selected:
        user_input = st.session_state.ol_selected
        print("seteamos user input:", user_input)
        st.session_state.ol_selected = None

    print("user_input:", user_input)
    if not user_input:
        with col1:
            user_input = st.text_input(
                "Ingrese un código OL válido:", 
                placeholder="Escriba aquí...",
                key="user_input_field",
            )
    
    print("user_input 2:", user_input)
    if user_input:
        if st.session_state.use_manual_ol:
            ol = st.session_state.manual_ol_df
            st.session_state.use_manual_ol = False
        else:
            ol = db_data2.ol_df(user_input)
            ol = db_data2.ol_description_df(ol)
        if ol.empty:
            st.warning("No se encuentra esa OL en la base de datos")
            set_manual_ol()
            return
        
        st.markdown("Datos de OL:")
        st.dataframe(ol)
        ol = ol.loc[0]

        again = True
        bad_recipes = []
        receta_colorantes_df = pd.DataFrame() 
        comparacion_colorantes_df = pd.DataFrame()
        comparacion_lote_est_df = pd.DataFrame()

        while again:
            receta_base_df = get_recipes_by_color(ol["CODIGO_COLOR"], ol["RECETA"])
            
            for recipe in bad_recipes:
                receta_base_df = receta_base_df[receta_base_df["TCODIRECE"] != recipe]
            st.dataframe(receta_base_df)

            if receta_base_df.empty:
                return
            
            if receta_base_df["TCODIRECE"].isin([ol["RECETA"]]).any():
                receta_base_df = receta_base_df[receta_base_df["TCODIRECE"] != ol["RECETA"]]
            
            if receta_base_df.empty:
                st.warning("La única receta encontrada es a si misma, no hay más recetas por analizar")
                return
            

            #st.markdown(receta_base_df.to_markdown(index=True))
            flag_filter = False
            if len(receta_base_df) != 1:
                receta_base_df = filter_by_repro(receta_base_df)
            if len(receta_base_df) != 1:
                receta_base_df, flag_filter = filter_by_ep(receta_base_df, ol["EP"])
            if flag_filter:
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_lote(receta_base_df, ol["LOTE_STD"], True) 
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_rb(receta_base_df, ol['RB']) 
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_stage(receta_base_df)
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_date(receta_base_df)
            else:
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_stage_labdip(receta_base_df)
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_lote(receta_base_df, ol["LOTE_STD"], True) 
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_rb(receta_base_df, ol['RB'])      
                if len(receta_base_df) != 1:
                    receta_base_df = filter_by_date(receta_base_df)
        
            # Aqui acaba la busqueda de la receta correcta, ahora se buscará ajustar el colorante:
            st.markdown("----")
            receta_colorantes_base_df, receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df = get_finals_dfs(receta_base_df['TCODIRECE'].iloc[0], ol['LOTE_STD'])
            if receta_colorantes_df.empty:
                bad_recipes.append(receta_base_df['TCODIRECE'].iloc[0])
                st.warning("No hay datos de colorantes para esta receta.")
                again = False
                return 
            else: 
                again = False
        receta_colorantes_df['AJUTE_RB'] = (float(ol['RB']) - receta_base_df['TRELABANO'].iloc[0]) / 100
        receta_colorantes_df['CONC_RB'] = round(receta_colorantes_df['TCONCPROD'] * (1 + (float(ol['RB']) - receta_base_df['TRELABANO'].iloc[0]) / 100), 4)
        #st.markdown("Colorante ajustado por Relación de Baño")
        #st.markdown(receta_colorantes_df.to_markdown())
        with st.spinner("IA analizando, un momento por favor..."):
            chat_response = decide_by_observation_gemini_five(receta_colorantes_base_df, receta_colorantes_df, comparacion_lote_est_df, int(receta_base_df["TCODILOTE"].iloc[0]))

            st.markdown(chat_response)
            st.markdown("Receta Base Encontrada:")
            st.dataframe(receta_base_df)

def show_sidebar():
    with st.sidebar:
        st.header("Creación y Carga de OLs")

        ol_options = get_ols_from_mariadb()
        selection = st.selectbox(
            "Seleccione una OL:",
            options=ol_options,
            index=None,
            key="sidebar_selectbox"
        )

        if selection:
            st.session_state.ol_selected = selection

        st.markdown("-----")

        uploaded_file = st.file_uploader(
            "Sube tu archivo CSV o Excel",
            type=['csv', 'xlsx', 'xls'],
            accept_multiple_files=False,
            key="file_uploader"
        )

        if uploaded_file:
            with st.spinner("Creando OLs"):
                create_ols(uploaded_file)
                st.session_state.button_ols_disable = False
            
        if st.button("Obtener Ols creadas", disabled=st.session_state.button_ols_disable):
            st.session_state.show_ols_df = True

        set_ols = st.text_input("Cargar OLs")
        if set_ols:    
            ols = set_ols.split(' ')
            for ol in ols:
                write_ol_mariadb(ol)
            st.success("OLs cargados, recargar pagina para visualizar en desplegable")


# ------------------------------- Estados iniciales ---------------------------------

if "button_ols_disable" not in st.session_state:
    st.session_state.button_ols_disable = True

# Sirve cuando generaremos la gráfica setearemos el valor de nuestro df calculado aquí
if "ols_df" not in st.session_state:
    st.session_state.ols_df = pd.DataFrame()

if "show_ols_df" not in st.session_state:
    st.session_state.show_ols_df = False

if "ol_selected" not in st.session_state:
    st.session_state.ol_selected = None


# ------------------------------------------------------------------------------------

st.set_page_config(page_title="1er Matizado", page_icon="📊", layout="wide")
st.set_option('deprecation.showPyplotGlobalUse', False)
st.title("Análisis de Matizado - Primera Entrada")

show_sidebar()

show_frontend()
if st.session_state.show_ols_df:
    st.dataframe(st.session_state.ols_df)
    st.session_state.show_ols_df = False

