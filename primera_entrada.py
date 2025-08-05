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

# Cargar las variables de entorno desde el archivo .env
warnings.filterwarnings("ignore")
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)

DEEPSEEK_MODEL_V3 = "deepseek-chat"
GEMINI_MODEL_2_0_FLASH =  "gemini-2.0-flash"
GEMINI_MODEL_2_5_FLASH = "gemini-2.5-flash"

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

def get_recipes_by_color(color):
    recipe_df = pd.DataFrame()
    #recipe_df = db_data2.get_recipe_from_carton_laboratorio(color)
    recipe_df = db_data2.get_recipes_complete(color)
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
    df = df[~df['TRECE_SEQ'].str.contains('REPRO', na=False)]
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

    return receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df

def decide_by_observation_gemini(receta_df, comparacion_df, comparacion_lote_df, lote_receta):
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

def decide_by_observation_gemini_two(receta_df, comparacion_df, comparacion_lote_df, lote_receta):
    model = genai.GenerativeModel(GEMINI_MODEL_2_5_FLASH)

    receta_df = receta_df[['TCODIPROD','TCODIAGRP', 'TDESCPROD', 'COLORANTE_AJUSTADO']]
    tabla_receta_colorantes = receta_df.to_markdown(index=False)
    tabla_comparacion_colorantes = comparacion_df.to_markdown(index=False)
    tabla_comparacion_lote = comparacion_lote_df.to_markdown(index=False)

    print("tipo de lote comparado: ", type(comparacion_lote_df))
    contexto_lote = ""
    regla_lote = ""
    if comparacion_lote_df['TLOTECOMP'].iloc[0] is not None and comparacion_lote_df['TLOTECOMP'].iloc[0] == lote_receta:
        contexto_lote = F"""
        3. COMPARACION LOTE STD: Proporciona ajustes estructurados en 3 columnas:
        - TINDIMATZ: Matiz a modificar (AM/AZ/RO/=).
        - TPORCMATZ: % de ajuste para el matiz.
        - TPORCINTE: % de ajuste para intensidad (afecta a TODOS los colorantes).
        {tabla_comparacion_lote}
        """

        regla_lote = """
        d) AJUSTE LOTE:
        Aplicar el ajuste por Lote, de acuerdo a los valores de la tabla de COMPARACION LOTE STD
        """
        print("ambas recetas son iguales")
    else:
        regla_lote = "Sin ajuste por LOTE - INT_LOTE = 0, MATIZ_LOTE = 0. Mencionar que no hay ajuste por LOTE porque el Lote de la receta base es distinto al Lote comparado"

    context = f"""
    OBJETIVO: Ajustar automáticamente los valores de 'COLORANTE_AJUSTADO' en 'receta_colorantes_df' basado en observaciones de 2 tablas de referencia y devolver SOLO la tabla final con los resultados.

    --- TABLAS DE ENTRADA ---
    1. RECETA COLORANTES BASE: Contiene los colorantes a ajustar (COLORANTE_AJUSTADO) y sus descripciones (TDESCPROD).
    {tabla_receta_colorantes}

    2. COMPARACION COLORANTES EST: Proporciona observaciones en columna 'TOBS' para ajustes.
    {tabla_comparacion_colorantes}

    {contexto_lote}

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

    {regla_lote}

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

def decide_by_observation_gemini_three(colorantes_df, comparacion_lote_df, lote_receta):
    model = genai.GenerativeModel(GEMINI_MODEL_2_5_FLASH)

    #receta_df = receta_df[['TCODIPROD','TCODIAGRP', 'TDESCPROD', 'COLORANTE_AJUSTADO']]
    #tabla_receta_colorantes = receta_df.to_markdown(index=False)
    #tabla_comparacion_colorantes = comparacion_df.to_markdown(index=False)
    tabla_comparacion_lote = comparacion_lote_df.to_markdown(index=False)
    tabla_colorantes = colorantes_df.to_markdown(index=False)


    contexto_lote = ""
    regla_lote = ""
    if comparacion_lote_df['TLOTECOMP'].iloc[0] is not None and comparacion_lote_df['TLOTECOMP'].iloc[0] == lote_receta:
        contexto_lote = F"""
        2. COMPARACION LOTE STD: Proporciona ajustes estructurados en 3 columnas:
        - TINDIMATZ: Matiz a modificar (AM/AZ/RO/=).
        - TPORCMATZ: % de ajuste para el matiz.
        - TPORCINTE: % de ajuste para intensidad (afecta a TODOS los colorantes).
        {tabla_comparacion_lote}
        """

        regla_lote = """
        d) AJUSTE LOTE:
        Aplicar el ajuste por Lote, de acuerdo a los valores de la tabla de COMPARACION LOTE STD
        """
    else:
        regla_lote = "Sin ajuste por LOTE - INT_LOTE = 0, MATIZ_LOTE = 0. Mencionar que no hay ajuste por LOTE porque el Lote de la receta base es distinto al Lote comparado"


    context = f"""
    OBJETIVO: Ajustar automáticamente los valores de 'COLORANTE_AJUSTADO' basado en observaciones de tablas de referencia y devolver SOLO la tabla final con los resultados.

    --- TABLAS DE ENTRADA ---
    1. TABLA DE COLORANTES: Contiene los colorantes ajustados, sus observaciones(TOBS) y flag de observacion. Tomar en cuenta que las observaciones solo se toma en cuenta
    si y solo si flag de observacion es True en caso sea False no tomar en cuenta las observaciones para el ajuste
    {tabla_colorantes}

    {contexto_lote}

    --- REGLAS DE AJUSTE ---
    A) INTENSIDAD (aplica a TODOS los colorantes):
    - Si TOBS contiene "x% más/menos intenso" o sinónimos: aplicar inverso (ej: "x% más intenso" → disminuir x%.).

    B) MATIZ (aplica SOLO al color mencionado):
    - Si TOBS contiene "% más/menos [color]": buscar en TDESCPROD y aplicar inverso.
    - Si TINDIMATZ ≠ "=": aplicar TPORCMATZ al matiz especificado.

    C) PRIORIDADES:
    1. Aplicar primero intensidad, luego matiz.
    2. Ignorar observaciones ambiguas o casos "Ok".

    {regla_lote}

    **IMPORTANTE** Aplicar: %Colorante Estimado = %Colorante Ajustado RB × (1 ± x%)  

    --- FORMATO DE SALIDA ---
    RESPUESTA TEXTUAL ÚNICA:
    1. Explicación breve y resumida de cambios aplicados, señalar porcentajes encontrados y aplicados.
    2. TABLA FINAL con las siguientes columnas:
    - Todas las columnas originales de colorante ajustado menos:
        * TCONCPROD
        * FLAG_OBS
        * TOBS
    - porcentajes de intensidad y matiz aplicados
    - COLUMNA FINAL: 'COL_FINAL' (resultado calculado).
        * Los valores deben redondearse a 6 decimales en todos los casos
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

def show_frontend():
    col1, col2 = st.columns([2, 5])  # 4/5 del ancho para el input, 1/5 para el botón

    with col1:
        user_input = st.text_input(
            "Ingrese un código OL válido:", 
            placeholder="Escriba aquí...",
            key="user_input_field",
        )
    
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
            receta_base_df = get_recipes_by_color(ol["CODIGO_COLOR"])
            
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
                receta_base_df, flag_filter = filter_by_ep(receta_base_df, ol["EP"])
            if len(receta_base_df) != 1:
                receta_base_df = filter_by_repro(receta_base_df)
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
            receta_colorantes_df, comparacion_colorantes_df, comparacion_lote_est_df = get_finals_dfs(receta_base_df['TCODIRECE'].iloc[0], ol['LOTE_STD'])
            if receta_colorantes_df.empty:
                bad_recipes.append(receta_base_df['TCODIRECE'].iloc[0])
                st.warning("No hay datos de colorantes para esta receta."   )
                again = False
                return 
            else: 
                again = False
        receta_colorantes_df['COLORANTE_AJUSTADO'] = round(receta_colorantes_df['TCONCPROD'] * (1 + (float(ol['RB']) - receta_base_df['TRELABANO'].iloc[0]) / 100), 4)
        st.markdown("Colorante ajustado por Relación de Baño")
        st.markdown(receta_colorantes_df.to_markdown())


        with st.spinner("IA analizando un momento por favor..."):
            print(receta_base_df["TCODILOTE"].iloc[0])
            chat_response = decide_by_observation_gemini_three(receta_colorantes_df, comparacion_lote_est_df, int(receta_base_df["TCODILOTE"].iloc[0]))

            st.markdown(chat_response)


st.set_page_config(page_title="1er Matizado", page_icon="📊", layout="wide")
st.set_option('deprecation.showPyplotGlobalUse', False)
st.title("Análisis de Matizado - Primera Entrada")

show_frontend()
