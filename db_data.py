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

db_config_dbin = {
    'user' : os.getenv("DBIN_USER"),
    'password' : os.getenv("DBIN_PASSWORD"),
    'dsn' : cx_Oracle.makedsn(os.getenv("DBIN_HOST"), int(os.getenv("DBIN_PORT")), os.getenv("DBIN_NAME"))
}

def connection():
    try:
        conn = cx_Oracle.connect(user=db_config["user"], password=db_config["password"], dsn=db_config["dsn"])
        return conn
    except Exception as e:
        #print(e)
        return None

def connection_dbin():
    try:
        conn = cx_Oracle.connect(user=db_config_dbin["user"], password=db_config_dbin["password"], dsn=db_config_dbin["dsn"])
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
                rtrim(b.codigocolormaster) as RECETA,
                trim(c.codigo_cor) as CODIGO_COLOR,
                to_char(c.especificacao_produt) as EP,
                qnfcn_conv_char_a_number(c.lote_produto) as LOTE_STD,
                c.rb_padrao / 100 as rb
            from ordem_laboratorio   c,
                ligacao_colormaster b
            where b.id_ligacaocolormaste = to_number(c.ordem_cliente)
            and c.numero_ordem = {ol}
            order by c.numero_ordem desc
            """
            print(query)
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    return pd.DataFrame()

def get_recipe_from_carton_laboratorio(num_ep, color_ol):
    conn = connection()
    if conn:
        try:
            query = "SELECT THORACARG, TCODIRECE, TCODICOLO, TCODIARTI, TCODILOTE, TRELABANO FROM rtdopartida WHERE TCODIARTI = :1 AND TCODICOLO = :2"
            df = pd.read_sql(query, conn, params= (num_ep,color_ol))
            conn.close()

            if not df.empty:
                print("toda la data encontrada con ep y color:")
                print(df)
                df_filtrado = df[df['TCODIRECE'].str.startswith('SL')]
                fila_mas_reciente = df_filtrado.loc[df_filtrado['THORACARG'].idxmax()]
                codigo_receta = fila_mas_reciente['TCODIRECE']
                return codigo_receta
            else:
                return None
        except Exception as e:
            return None
    return None

def get_recipe_from_carton_laboratorio_just_color(color_ol):
    conn = connection()
    if conn:
        try:
            query = "SELECT THORACARG, TCODIRECE, TCODICOLO, TCODIARTI, TCODILOTE, TRELABANO FROM rtdopartida WHERE TCODICOLO = :1"
            df = pd.read_sql(query, conn, params= (color_ol, ))
            conn.close()

            if not df.empty:
                print("toda la data encontrada con color:")
                print(df)
                df_filtrado = df[df['TCODIRECE'].str.startswith('SL')]
                fila_mas_reciente = df_filtrado.loc[df_filtrado['THORACARG'].idxmax()]
                codigo_receta = fila_mas_reciente['TCODIRECE']
                return codigo_receta
            else:
                return None
        except Exception as e:
            return None
    return None

def get_recipe_from_color_master(cod_color):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIRECE, TCODICOLO, TFECHINTE FROM TIDOINTERECE WHERE TCODICOLO = :1"
            df = pd.read_sql(query, conn, params= (cod_color, ))
            conn.close()
            print("df completo en color master: ")
            print(df)
            if not df.empty:
                last_data = df.loc[df['TFECHINTE'].idxmax()]
                codigo_receta = last_data['TCODIRECE']
                return codigo_receta
            else:
                return None
        except Exception as e:
            return None
    return None

def temp_df():
    conn = connection()
    if conn:
        try:
            query = """select a.*,
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
                    """
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return pd.DataFrame()

temp = temp_df()
print("datos del nuevo query:")
print(temp)

def recipe_data_df(recipe):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIRECE, TCODICOLO, TCODIARTI, TCODILOTE, TRELABANO FROM TIDOINTERECE WHERE TCODIRECE = :1"
            df = pd.read_sql(query, conn, params= (recipe, ))
            conn.close()
            print("df completo de receta encontrado: ")
            print(df)
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

def get_temp_tick():
    conn = connection_dbin()
    if conn:
        try:
            query = "SELECT ttickbarr FROM acdoprendas WHERE trunc(tfechingr) = trunc(sysdate - 1)"
            df = pd.read_sql(query, conn)
            conn.close()
            print(df)
            return df
        except Exception as e:
            #print(e)
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

def colorante_df_two(cod_col):
    conn = connection()
    if conn:
        try:
            query = "SELECT TCODIRECE, TCODIPROD, TDESCPROD, TCONCPROD FROM TIDOINTERECECOLR WHERE TCODIPROD = :1"
            df = pd.read_sql(query, conn, params= (cod_col, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
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

def get_observation_df(cod_agrupador):
    conn = connection()
    if conn:
        try:
            query = "SELECT TFECHINGR, TGRUPCOLR, TCODICOLR, TDESCCOLR, TOBSVLIBE FROM lbvolibelotecolr WHERE TGRUPCOLR = :1"
            df = pd.read_sql(query, conn, params = (cod_agrupador, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

def get_observation_df_two(cod_color):
    conn = connection()
    if conn:
        try:
            query = "SELECT TFECHINGR, TFECHLIBE, TGRUPCOLR, TCODICOLR, TDESCCOLR, TOBSVLIBE FROM lbvolibelotecolr WHERE TCODICOLR = :1"
            df = pd.read_sql(query, conn, params = (cod_color, ))
            conn.close()
            return df
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

#print("esta esss")
#use_color= get_colors_from_cod_agr("127763")
#print(get_colors_from_cod_agr("4187"))
#print(colorante_df_two("S8460"))
#print(get_observation_df("4187"))
#print(get_observation_df_two("8460"))
#print(get_temp_tick())