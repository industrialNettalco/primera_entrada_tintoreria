import pandas as pd
import numpy as np
import cx_Oracle
import os
from dotenv import load_dotenv

load_dotenv()

db_config = {
    'user' : os.getenv("DB_NET_USER"),
    'password' : os.getenv("DB_NET_PASSWORD"),
    'dsn' : cx_Oracle.makedsn(os.getenv("DB_NET_HOST"), int(os.getenv("DB_NET_PORT")), os.getenv("DB_NET_NAME"))
}

def connection():
    try:
        conn = cx_Oracle.connect(user=db_config["user"], password=db_config["password"], dsn=db_config["dsn"])
        return conn
    except Exception as e:
        #print(e)
        return None

def ol_df(ol):
    conn = connection()
    if conn:
        try:
            query = f"""
            select c.numero_ordem as ol,
                rtrim(b.codigocolormaster) as receta,
                trim(c.codigo_cor) as codigo_color,
                to_char(c.especificacao_produt) as ep,
                qnfcn_conv_char_a_number(c.lote_produto) as lote_std,
                c.rb_padrao / 100 as rb,
                e.tcodiclie,
                e.tabrvclie,
                c.tcantinte
            from ordem_laboratorio   c,
                ligacao_colormaster b,
                tidocolo            d,
                qndoclie            e
            where b.id_ligacaocolormaste = to_number(c.ordem_cliente)
            and c.numero_ordem = {ol}
            and trim(c.codigo_cor) = d.tcodicolo(+)
            and d.tcodiclie = e.tcodiclie(+)
            order by c.numero_ordem desc
            """
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                df["LOTE_STD"] = df["LOTE_STD"].astype(str)
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    return pd.DataFrame()

def get_description_color(color_id):
    conn = connection()
    if conn:
        try:
            query = "SELECT tdesccolo FROM tidocolo WHERE tcodicolo = :1"
            df = pd.read_sql(query, conn, params= (color_id, ))
            conn.close()
            if not df.empty:
                color_description = df["TDESCCOLO"].iloc[0]
                return color_description
            else:
                return ""
        except Exception as e:
            return ""
    return ""

def get_description_ep(ep):
    ep = ep[:-1]
    conn = connection()
    if conn:
        try:
            query = "SELECT tdesctela FROM tedotela WHERE tcoditela = :1"
            df = pd.read_sql(query, conn, params= (ep, ))
            conn.close()
            if not df.empty:
                ep_description = df["TDESCTELA"].iloc[0]
                return ep_description
            else:
                return ""
        except Exception as e:
            return ""
    return ""

def ol_description_df(ol_df):
    if ol_df.empty:
        return ol_df
    ol_df["DESCRIPCION_COLOR"] = ol_df["CODIGO_COLOR"].apply(get_description_color)
    ol_df["DESCRIPCION_TELA"] = ol_df["EP"].apply(get_description_ep)
    ol_df = ol_df[["OL","RECETA", "TCODICLIE", "TABRVCLIE", "CODIGO_COLOR", "DESCRIPCION_COLOR", "EP", "DESCRIPCION_TELA", "LOTE_STD", "RB", "TCANTINTE"]]
    return ol_df

def get_recipe_from_carton_laboratorio(color_ol):
    conn = connection()
    if conn:
        try:
            query = "SELECT THORACARG, TCODIRECE, TCODICOLO, TCODIARTI, TCODILOTE, TRELABANO FROM rtdopartida WHERE TCODICOLO = :1"
            df = pd.read_sql(query, conn, params= (color_ol, ))
            conn.close()

            if not df.empty:
                return df
            else:
                return None
        except Exception as e:
            return None
    return None

def get_recipe_from_color_master(cod_color):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIRECE, TCODICOLO, TCODIARTI, TCODILOTE, TRELABANO, TESTARECE, TTIPORECE, TRECE_SEQ, TFECHACTU FROM TIDOINTERECE WHERE TCODICOLO = :1"
            df = pd.read_sql(query, conn, params= (cod_color, ))
            conn.close()
            if not df.empty:
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    return pd.DataFrame()

def get_recipe_from_high_solidity(cod_color):
    conn = connection()
    if conn:
        try:
            query_color = "'%" + cod_color + "%'"
            query = f"SELECT * FROM epdocoln a WHERE a.tdesccoln LIKE {query_color} AND a.tdesccoln LIKE '%SOL%'"
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    return pd.DataFrame()

def get_recipe_from_machine_code(cod_color):
    conn = connection()
    if conn:
        try:
            query_color = "'%" + cod_color + "%'"
            query = f"SELECT * FROM  epdocoln a WHERE a.tdesccoln LIKE {query_color} AND a.tdesccoln not LIKE '%SOL%' AND substr(a.tnumecoln, 1, 1) = '0'"
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    return pd.DataFrame()

def recipe_data_df(recipe):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIRECE, TCODICOLO, TCODIARTI, TCODILOTE, TRELABANO FROM TIDOINTERECE WHERE TCODIRECE = :1"
            df = pd.read_sql(query, conn, params= (recipe, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return pd.DataFrame()

def get_recipes_complete(cod_color):
    conn = connection()
    if conn:
        try:
            query = f"""select a.*,
                        b.tdesctela,
                        c.tdesccolo,
                        d.tdesctipoteji
                    from tidointerece a,
                        tedotela     b,
                        tidocolo     c,
                        tedotipoteji@dba2 d
                    where substr(a.tcodiarti, 1, length(a.tcodiarti) - 1) = b.tcoditela (+)
                    and a.tcodicolo = c.tcodicolo (+)
                    and b.ttipoteji = d.ttipoteji (+)
                    and a.tcodicolo = :1
                    """
            df = pd.read_sql(query, conn, params= (cod_color, ))
            conn.close()
            if df.empty:
                return pd.DataFrame()
            df = df[["TCODIRECE", "TCODICOLO", "TDESCCOLO", "TCODIARTI", "TDESCTELA", "TCODILOTE", "TRELABANO", "TESTARECE", "TTIPORECE", "TAUXIRECE", "TRECE_SEQ", "TFECHACTU", "TDESCTIPOTEJI"]]
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return pd.DataFrame()

def colorante_df(receta_base):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIRECE, TCODIPROD, TDESCPROD, TCONCPROD FROM TIDOINTERECECOLR WHERE TCODIRECE = :1"
            df = pd.read_sql(query, conn, params= (receta_base, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

def get_observation_df(cod_agrupador):
    conn = connection()
    if conn:
        try:
            query = "SELECT TFECHINGR, TGRUPCOLR, TCODICOLR, TDESCCOLR, TOBSVLIBE FROM lbvolibelotecolr WHERE TCODICOLR = :1"
            df = pd.read_sql(query, conn, params = (cod_agrupador, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

#print(get_observation_df("8377"))

def get_lotes_df(cod_agrupador, cod_color):
    conn = connection()
    if conn:
        try:
            query = "SELECT TLOTECOLR, TFECHINGR, TGRUPCOLR, TCODICOLR, TDESCCOLR, TOBSVLIBE FROM lbvolibelotecolr WHERE TGRUPCOLR = :1 AND TCODICOLR = :2"
            df = pd.read_sql(query, conn, params = (cod_agrupador, cod_color, ))
            conn.close()
            if df.empty:
                return ""
            lotes = df['TLOTECOLR'].unique().tolist()
            return lotes
        except Exception as e:
            #print(e)
            return ""
    return None

def codi_agru(codi_arti):
    conn = connection()
    if conn:
        try:
            query = "SELECT TRIM(TCODIALTR), TCODIAGRP FROM INVOREDUQUIMCOLO WHERE TRIM(TCODIALTR) = :1"
            df = pd.read_sql(query, conn, params = (codi_arti,))
            conn.close()
            if not df.empty:
                return df['TCODIAGRP'][0]
            else:
                return None
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

def lote_std_df(lote_receta):
    conn = connection()
    if conn:
        try:
            query = "SELECT IDLOTE_PADRAO, TPORCMATZ, TINDIMATZ, TPORCINTE, TLOTECOMP FROM LOTES_PADRAO WHERE IDLOTE_PADRAO = :1"
            df = pd.read_sql(query, conn, params= (lote_receta, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

def get_colors_from_cod_agr(codi_agru):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIALTR, TDESCITEM FROM INVOREDUQUIMCOLO WHERE TCODIAGRP = :1"
            df = pd.read_sql(query, conn, params = (codi_agru,))
            conn.close()
            if not df.empty:
                return df
            else:
                return None
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None