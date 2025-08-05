import pandas as pd
import warnings
import db_data2

warnings.filterwarnings("ignore")

def get_fails(color):
    fails = 0
    recipe_df = pd.DataFrame()
    #recipe_df = db_data2.get_recipe_from_carton_laboratorio(color)
    recipe_df = db_data2.get_recipe_from_color_master(color)
    if recipe_df.empty:
        #recipe_df = db_data2.get_recipe_from_color_master(color)
        if recipe_df.empty:
            solidity_df = db_data2.get_recipe_from_high_solidity(color)
            if solidity_df.empty:
                machine_df = db_data2.get_recipe_from_machine_code(color)
                if machine_df.empty:
                    fails += 1
    return fails

# Descargar el archivo y cargarlo en un DataFrame
df = pd.read_csv("OLS_2.csv")
df_valid = df['OL'].dropna()
df_valid = df_valid[df_valid != "(en blanco)"]

fails = 0

for ol in df_valid:
    ol_df = db_data2.ol_df(ol)
    color = ol_df['CODIGO_COLOR'].iloc[0]
    fails += get_fails(color)

print("Total de errados: ", fails)
print("Total de datos", len(df_valid))
porcentaje_fails = fails/len(df_valid)*100
porcentaje_fails = round(porcentaje_fails, 2)
porcentaje_fails_str = str(porcentaje_fails) + '%'
print("Esto equivale al", porcentaje_fails_str, "de OLs sin poder procesar")
