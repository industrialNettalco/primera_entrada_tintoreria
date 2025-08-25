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
                rtrim(b.codigocolormaster) as receta,
                trim(c.codigo_cor) as codigo_color,
                to_char(c.especificacao_produt) as ep,
                qnfcn_conv_char_a_number(c.lote_produto) as lote_std,
                c.rb_padrao / 100 as rb,
                e.tabrvclie,
                e.tcodiclie,
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

def get_data_from_specific_ols():
    ols = [158656, 150834, 158649, 158655, 158659, 158086, 158581, 157564, 158653, 158675, 158679, 158803, 158279, 157841, 158663, 158617, 154478, 157136, 158855, 158848, 158854, 155385, 157518, 158763, 158838, 158838]
    data_total = []
    for ol in ols:
        df = ol_df(ol)
        data_total.append(df)
    
    df_final = pd.DataFrame()
    if data_total:
        df_final = pd.concat(data_total, ignore_index=True)

    df_final = ol_description_df(df_final)

    df_final.to_excel('ols_data.xlsx', index=False)
    #return df_final

#print("datos del query:")
#print(get_data_from_specific_ols())
#print(ol_df(150834))

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

#temp = temp_df()
#print("datos del nuevo query:")
#print(temp)

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

#print("datos de colorantes:")
#print(colorante_df("SL00155876"))

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
            query = "SELECT TLOTECOLR, TFECHINGR, TGRUPCOLR, TCODICOLR, TDESCCOLR, TOBSVLIBE FROM lbvolibelotecolr WHERE TGRUPCOLR = :1"
            df = pd.read_sql(query, conn, params = (cod_agrupador, ))
            conn.close()
            return df
        except Exception as e:
            #print(e)
            return pd.DataFrame()
    return None

#print("libreacion de colorantes:")
#df = get_observation_df(1007777)
#print(df)

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

#print("libreacion de colorantes:")
#df = get_lotes_df(1007777, "8438")
#print(df)

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

# Tabla de seguimiento de ordenes de laboratorio + descripcion de articulos
def seg_ord_plus_descr_ep():
    conn = connection()
    if conn:
        try:
            query = """
            SELECT
                l.*,
                t.tdesctela AS DESCRIPCION_ARTICULO
            FROM
                LBVODETAPEDILABO l
            LEFT JOIN
                TEDOTELA t ON l.TCODIARTI = t.tcoditela
            """
            df = pd.read_sql(query, conn)
            conn.close()
            if not df.empty:
                return df
            else:
                return ""
        except Exception as e:
            return ""
    return ""

def temp_fall_sgt():
    conn = connection()
    if conn:
        try:
            query = """
            select a.*,
                to_char(nvl(a.tfechdespprog, a.tcontdate), 'yyyy-mm') as tmesedesp,
                b.articulo as tcodihilareal,
                b.colrns as tnumecolohila,
                b.tdesccoln as tdesccolohila,
                (select decode(max(x.codigo_maquina), '0AC10', 'Terceros', '0AC01', 'Nettalco', '0AC02', 'Nettalco', '0AC11', 'Nettalco', '0AC12', 'Nettalco', '0AC09', 'Nettalco', max(x.codigo_maquina))
                    from ob_fases x
                    where a.tnumeroobhilo = x.numero_ob
                    and x.codigo_fase in (410, 411)) as tmaqutenihilo,
                c.tdescarti,
                c.tdesccoln,
                d.tipoarticulo as ttiporeducrud,
                to_char(min(nvl(a.tfechdespprog, a.tcontdate)) over(partition by a.tnumeud), 'iyyy-iw') as tsemadesp,
                case
                    when trim(e.codigo_alternativo) like '%50' then
                    'Si'
                    else
                    'No'
                end as tprodhilo,
                case
                    when b.colrns like '0%' and substr(b.colrns, 2, 1) <> '9' then 'Si'
                    else 'No'
                    end as tcodimatc  
            from dtdoasigtelanuev      a,
                sgt_reducido_articulo b,
                sgt_reducido_articulo c,
                sgt_reducido_articulo d,
                itens_estoque e
            where a.treduhilocolo = b.reducido(+)
            and a.tcodireduacab = c.reducido(+)
            and a.tcodireducrud = d.reducido(+)
            and a.treduhilocolo = e.codigo_reduzido (+)
            --and (a.tsectxtin = 'XTN' and (a.tcodisectdepecoln in ('XTN', 'FAL') or a.tcodisect = 'TIN') or a.tsectxtin = 'FAL' and a.tcodisectdepecoln in ('FAL'))
            """
            df = pd.read_sql(query, conn)
            conn.close()
            print("df completo fall sgt: ")
            print(df)
            if not df.empty:    
                return df
                #last_data = df.loc[df['TFECHINTE'].idxmax()]
                #codigo_receta = last_data['TCODIRECE']
                #return codigo_receta
            else:
                print("No es esa tabla")
                return None
        except Exception as e:
            return None
    return None

#print("probando query!")
#print(temp_fall_sgt())

#print(temp_fall_sgt())
#print("probando query!")
#df = seg_ord_plus_descr_ep()
#print(df.columns)
#print(len(df))

#print(get_colors_from_cod_agr(1007777))

#print("esta esss")
#use_color= get_colors_from_cod_agr("127763")
#print(get_colors_from_cod_agr("4187"))
#print(colorante_df_two("S8460"))
#print(get_observation_df("4187"))
#print(get_observation_df_two("8460"))
#print(get_temp_tick())