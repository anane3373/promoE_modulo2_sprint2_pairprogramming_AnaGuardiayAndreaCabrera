from IPython.core.interactiveshell import InteractiveShell 
InteractiveShell.ast_node_interactivity = "all" 

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import mysql.connector

# PAIR 1

url_api = "https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date=2011-01-01T00:00&end_date=2011-12-31T23:59&time_trunc=day"
response = requests.get(url=url_api)
response.status_code


response.json().keys()
type(response.json()['included'])
len(response.json()['included']) 
response.json()['included'][0].keys()
response.json()['included'][0]['type']
response.json()['included'][0]['attributes'].keys()
type(response.json()['included'][0]['attributes']['values'])
response.json()['included'][0]['attributes']['values'][0].keys()

response_data = response.json()['included']
renovable = response_data[0]
df = pd.json_normalize(renovable['attributes']['values'])

def sacar_info_energia():
    df_final = pd.DataFrame() 
    for year in range(2011, 2023): 
        url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day" 
        response = requests.get(url=url) 
        status_code = response.status_code 
                
        if status_code == 200: 
            response_data = response.json()['included'] 
            len_response_data = len(response_data) 
            
            for i in range(len_response_data): 
                attributes_data = response_data[i]['attributes'] 
                type_energy = attributes_data['type']
                values_data = attributes_data['values'] 
                df = pd.json_normalize(values_data) 
                df["tipo_energia"] = type_energy 
                df_final = pd.concat([df_final, df], axis = 0) 
        else: 
            continue
    return df_final.reset_index(drop = True) 

df = sacar_info_energia()

cod_comunidades = {
    'Ceuta': 8744,
    'Melilla': 8745,
    'Andalucía': 4,
    'Aragón': 5,
    'Cantabria': 6,
    'Castilla - La Mancha': 7,
    'Castilla y León': 8,
    'Cataluña': 9,
    'País Vasco': 10,
    'Principado de Asturias': 11,
    'Comunidad de Madrid': 13,
    'Comunidad Foral de Navarra': 14,
    'Comunitat Valenciana': 15,
    'Extremadura': 16,
    'Galicia': 17,
    'Illes Balears': 8743,
    'Canarias': 8742,
    'Región de Murcia': 21,
    'La Rioja': 20
}

for k,v in cod_comunidades.items():
    print(k,v)
    
def sacar_info_energia_ccaa(ccaa):
    df_final = pd.DataFrame() 
    for year in range(2011, 2023): 
        for k_nombre_ccaa, v_id_ccaa in ccaa.items():
            url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day&geo_limit=ccaa&geo_ids={v_id_ccaa}" 
            response = requests.get(url=url) 
            status_code = response.status_code 
            
            if status_code == 200: 
                response_data = response.json()['included']
                len_response_data = len(response_data) 
                
                for i in range(len_response_data): 
                    attributes_data = response_data[i]['attributes'] 
                    type_energy = attributes_data['type'] 
                    values_data = attributes_data['values'] 
                    df = pd.json_normalize(values_data) 
                    df["tipo_energia"] = type_energy 
                    df["comunidad"] = k_nombre_ccaa 
                    df["id_comunidad"] = v_id_ccaa 
                    df_final = pd.concat([df_final, df], axis = 0) 
            else: 
                continue
    return df_final.reset_index(drop = True) 

df_ccaa = sacar_info_energia_ccaa(cod_comunidades)

df.to_csv("data/pairETL1_datos_energia_nacional.csv")
df_ccaa.to_csv("data/pairETL1_datos_energia_ccaa.csv")

# PAIR 2
df_energia_ccaa = pd.read_csv("data/pairETL1_datos_energia_ccaa.csv", index_col= 0)
df_energia_nacional = pd.read_csv("data/pairETL1_datos_energia_nacional.csv", index_col= 0)
df_poblacion_ccaa = pd.read_csv("data/poblacion_comunidades.csv", index_col= 0)

df_poblacion_ccaa.columns

df_poblacion_ccaa.drop(columns=["Comunidades_y_Ciudades_Autónomas"], axis = 0, inplace=True)

df_poblacion_ccaa.columns

df_poblacion_ccaa.head(2)

columnas = ["value", "percentage"]

for col in columnas:
    df_energia_nacional[col] = df_energia_nacional[col].round(2)
df_energia_nacional.head()

df_energia_nacional.dtypes

df_energia_nacional["fecha"] = df_energia_nacional['datetime'].str.split("T", n = 1, expand = True).get(0).astype("datetime64", errors = "ignore")
df_energia_nacional.head(2)

df_energia_nacional['fecha'] = pd.to_datetime(df_energia_nacional['fecha'])

df_energia_nacional.drop(columns=["datetime"], axis = 0, inplace=True)

df_energia_nacional.dtypes

df_energia_nacional.head()

df_energia_ccaa.head()

columnas = ["value", "percentage"]

for col in columnas:
    df_energia_ccaa[col] = df_energia_nacional[col].round(2)

df_energia_ccaa["fecha"] = df_energia_ccaa['datetime'].str.split("T", n = 1, expand = True).get(0).astype("datetime64", errors = "ignore")
df_energia_ccaa['fecha'] = pd.to_datetime(df_energia_ccaa['fecha'])
df_energia_ccaa.drop(columns=["datetime"], axis = 0, inplace=True)

df_energia_ccaa.to_pickle("data/pairETL2_datos_energia_ccaa.pkl")
df_energia_nacional.to_pickle("data/pairETL2_datos_energia_nacional.pkl")
df_poblacion_ccaa.to_pickle("data/pairETL2_poblacion_comunidades.pkl")

# PAIR 3
class EnergiaPoblacion:
    
    def __init__(self, start_year = 2011, end_year = 2022, path_to_save = "data"):
        self.start_year = start_year
        self.end_year = end_year
        self.path_to_save = path_to_save
        self.ccaa = {
            'Ceuta': 8744,
            'Melilla': 8745,
            'Andalucía': 4,
            'Aragón': 5,
            'Cantabria': 6,
            'Castilla - La Mancha': 7,
            'Castilla y León': 8,
            'Cataluña': 9,
            'País Vasco': 10,
            'Principado de Asturias': 11,
            'Comunidad de Madrid': 13,
            'Comunidad Foral de Navarra': 14,
            'Comunitat Valenciana': 15,
            'Extremadura': 16,
            'Galicia': 17,
            'Illes Balears': 8743,
            'Canarias': 8742,
            'Región de Murcia': 21,
            'La Rioja': 20
        }
        
    def info_energia_nacional(self):
        """Obtener la información de la evolución de las energías renovables y no renovables en el periodo de tiempo solicitado.

        Returns:
            dataframe: dataframe con la información de todos los años solicitados.
        """
        print(f"Ejecutando función 'info_energia_nacional' para el período de tiempo {self.start_year}-{self.end_year}")
        df_final = pd.DataFrame()
        for year in range(self.start_year, self.end_year + 1):
            url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day"
            response = requests.get(url=url)
            status_code = response.status_code
            print(f"{year} : Respuesta de la petición: {status_code}")
                    
            if status_code == 200:
                response_data = response.json()['included']
                len_response_data = len(response_data)
                
                for i in range(len_response_data):
                    attributes_data = response_data[i]['attributes']
                    type_energy = attributes_data['type']
                    values_data = attributes_data['values']
                    df = pd.json_normalize(values_data)
                    df["tipo_energia"] = type_energy
                    df_final = pd.concat([df_final, df], axis = 0)
            else:
                continue
        return df_final.reset_index(drop = True)
    
    def info_energia_ccaa(self):
        """Obtener la información de la evolución de las energías renovables y no renovables de todas las CCAA en el periodo de tiempo solicitado.

        Returns:
            dataframe: dataframe con la información de todas las CCAA y año solicitado.
        """
        print(f"Ejecutando función 'info_energia_ccaa' para el período de tiempo {self.start_year}-{self.end_year}")
        df_final = pd.DataFrame()
        for year in range(self.start_year, self.end_year + 1):
            for k_nombre_ccaa, v_id_ccaa in self.ccaa.items():
                url = f"https://apidatos.ree.es/es/datos/generacion/evolucion-renovable-no-renovable?start_date={year}-01-01T00:00&end_date={year}-12-31T23:59&time_trunc=day&geo_limit=ccaa&geo_ids={v_id_ccaa}"
                response = requests.get(url=url)
                status_code = response.status_code
                print(f"{year} - {k_nombre_ccaa} : Respuesta de la petición: {status_code}")
                if status_code == 200:
                    response_data = response.json()['included']
                    len_response_data = len(response_data)
                    
                    for i in range(len_response_data):
                        attributes_data = response_data[i]['attributes']
                        type_energy = attributes_data['type']
                        values_data = attributes_data['values']
                        df = pd.json_normalize(values_data)
                        df["tipo_energia"] = type_energy
                        df["comunidad"] = k_nombre_ccaa
                        df["id_comunidad"] = v_id_ccaa
                        df_final = pd.concat([df_final, df], axis = 0)
                else:
                    continue
        return df_final.reset_index(drop = True)
            
    def drop_column(self, df, col):
        """Eliminar la columna solicitada en el dataframe aportado.

        Args:
            df (dataframe): dataframe utilizado.
            col (str): nombre de la columna.

        Returns:
            dataframe: dataframe utilizado sin la columna.
        """
        print(f"Eliminando columna '{col}'")
        return df.drop(columns = [col], axis = 0, inplace = True )
        
    def round_two(self, df, columns):
        """Redondear los valores a dos decimales.

        Args:
            df (dataframe): dataframe utilizado.
            columns (list): lista con las columnas a tratar.

        Returns:
            dataframe: dataframe con los valores redondeados a 2 decimales
        """
        for col in columns:
            print(f"Redondeando columna '{col}' a 2 decimales")
            df[col] = df[col].round(2)
    
    def create_datetime_col(self, df, datetimeCol, newCol):
        """Crear una columna de tipo datetime con el nombre aportado a partir de una columna de tipo object

        Args:
            df (dataframe): dataframe utilizado.
            datetimeCol (str): nombre de la columna origen
            newCol (str): nombre de la nueva columna
        """
        print(f"Creando columna '{newCol}' a partir de la columna '{datetimeCol}'")
        df[newCol] = df[datetimeCol].str.split("T", n = 1, expand = True).get(0).astype("datetime64", errors = "ignore")
        df[newCol] = pd.to_datetime(df[newCol])

    def open_csv(self, path, file_name, years = False):
        """Abrir el archivo csv en la ruta especificada con el nombre aportado.

        Args:
            path (str): ruta donde se encuentra el fichero
            file_name (str): nombre del archivo
            years (bool, optional): Especificamos si debemos buscar el fichero con el intervalo de tiempo deseado al generar la clase. Defaults to False.

        Returns:
            dataframe: dataframe generado a partir del archivo csv leído
        """
        if years:
            path_file = f"{path}/{file_name}_{self.start_year}_{self.end_year}.csv"
        else:
            path_file = f"{path}/{file_name}.csv"
        
        print(f"Leyendo: {path_file}")  
        return pd.read_csv(path_file)
    
    def open_pickle(self, path, file_name, years = False):
        """Abrir el archivo pickle en la ruta especificada con el nombre aportado.

        Args:
            path (str): ruta donde se encuentra el fichero
            file_name (str): nombre del archivo
            years (bool, optional): Especificamos si debemos buscar el fichero con el intervalo de tiempo deseado al generar la clase. Defaults to False.

        Returns:
            dataframe: dataframe generado a partir del archivo pickle leído
        """
        if years:
            path_file = f"{path}/{file_name}_{self.start_year}_{self.end_year}.pkl"
        else:
            path_file = f"{path}/{file_name}.pkl"
            
        print(f"Leyendo: {path_file}")  
        return pd.read_pickle(path_file)
           
    def save_to_csv(self, df, file_name, years = False):
        """Guardar el dataframe aportado como un fichero csv.

        Args:
            df (dataframe): dataframe utilizado.
            file_name (str): nombre del archivo que queremos generar y guardar.
            years (bool, optional): Especificamos si debemos guardar el fichero con el intervalo de tiempo deseado al generar la clase. Defaults to False.
        """
        if years:
            path_file = f"{self.path_to_save}/{file_name}_{self.start_year}_{self.end_year}.csv"
        else:
            path_file = f"{self.path_to_save}/{file_name}.csv"
        
        print(f"Guardando en: {path_file}")  
        df.to_csv(path_file)
        
        
    def save_to_pickle(self, df, file_name, years = False):
        """Guardar el dataframe aportado como un fichero pickle.

        Args:
            df (dataframe): dataframe utilizado.
            file_name (str): nombre del archivo que queremos generar y guardar.
            years (bool, optional): Especificamos si debemos guardar el fichero con el intervalo de tiempo deseado al generar la clase. Defaults to False.
        """
        if years:
            path_file = f"{self.path_to_save}/{file_name}_{self.start_year}_{self.end_year}.pkl"
        else:
            path_file = f"{self.path_to_save}/{file_name}.pkl"
            
        print(f"Guardando en: {path_file}")  
        df.to_pickle(path_file)
        
clase_pair3_poblacion_ccaa = EnergiaPoblacion()
df_pair3_poblacion_ccaa = clase_pair3_poblacion_ccaa.open_csv("data","poblacion_comunidades",False)
df_pair3_poblacion_ccaa.head()

clase_pair3_poblacion_ccaa.drop_column(df_pair3_poblacion_ccaa, "Comunidades_y_Ciudades_Autónomas")
df_pair3_poblacion_ccaa.columns

clase_pair3_poblacion_ccaa.save_to_csv(df_pair3_poblacion_ccaa, "pairETL3_poblacion_comunidades", False)
clase_pair3_poblacion_ccaa.save_to_pickle(df_pair3_poblacion_ccaa, "pairETL3_poblacion_comunidades", False)

clase_pair3_energia_nacional = EnergiaPoblacion(start_year=2011, end_year=2022)
df_pair3_energia_nacional = clase_pair3_energia_nacional.info_energia_nacional()
df_pair3_energia_nacional.head()

clase_pair3_energia_nacional.create_datetime_col(df_pair3_energia_nacional, "datetime", "fecha")
df_pair3_energia_nacional.head()

clase_pair3_energia_nacional.drop_column(df_pair3_energia_nacional, "datetime")
df_pair3_energia_nacional.head()

clase_pair3_energia_nacional.save_to_csv(df_pair3_energia_nacional, "pairETL3_energia_nacional", False)
clase_pair3_energia_nacional.save_to_csv(df_pair3_energia_nacional, "pairETL3_energia_nacional", True)
clase_pair3_energia_nacional.save_to_pickle(df_pair3_energia_nacional, "pairETL3_energia_nacional", False)
clase_pair3_energia_nacional.save_to_pickle(df_pair3_energia_nacional, "pairETL3_energia_nacional", True)

clase_pair3_energia_ccaa = EnergiaPoblacion(2011,2022)
df_pair3_energia_ccaa = clase_pair3_energia_ccaa.info_energia_ccaa()

clase_pair3_energia_ccaa.round_two(df_pair3_energia_ccaa, ["value", "percentage"])
df_pair3_energia_ccaa.head()

clase_pair3_energia_ccaa.create_datetime_col(df_pair3_energia_ccaa, "datetime", "fecha")
df_pair3_energia_ccaa.head()

clase_pair3_energia_ccaa.drop_column(df_pair3_energia_ccaa, "datetime")
df_pair3_energia_ccaa.head()

clase_pair3_energia_ccaa.save_to_csv(df_pair3_energia_ccaa, "pairETL3_energia_ccaa", False)
clase_pair3_energia_ccaa.save_to_csv(df_pair3_energia_ccaa, "pairETL3_energia_ccaa", True)
clase_pair3_energia_ccaa.save_to_pickle(df_pair3_energia_ccaa, "pairETL3_energia_ccaa", False)
clase_pair3_energia_ccaa.save_to_pickle(df_pair3_energia_ccaa, "pairETL3_energia_ccaa", True)

# PAIR 4
def crear_bbdd(nombre_bbdd):

    mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="AlumnaAdalab", 
      auth_plugin = 'mysql_native_password') 
    print("Conexión realizada con éxito")
    
    mycursor = mydb.cursor()

    try:
        mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {nombre_bbdd};")
        print(mycursor)
    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)

mydb = crear_bbdd("energia")

def crear_insertar_tabla(nombre_bbdd, contraseña, query):
 
    cnx = mysql.connector.connect(user='root', password=f"{contraseña}",
                                     host='127.0.0.1', database=f"{nombre_bbdd}",  
                                     auth_plugin = 'mysql_native_password')

    mycursor = cnx.cursor()
    
   
    try: 
        mycursor.execute(query)
        cnx.commit() 
   
    except mysql.connector.Error as err:
        print(err)
        print("Error Code:", err.errno)
        print("SQLSTATE", err.sqlstate)
        print("Message", err.msg)
        
tabla_fechas = '''
CREATE TABLE IF NOT EXISTS `energia`.`fechas` (
  `idfechas` INT NOT NULL,
  `fecha` DATE NULL,
  PRIMARY KEY (`idfechas`))
ENGINE = InnoDB;
'''

tabla_nacional_renovable_no_renovable = '''
CREATE TABLE IF NOT EXISTS `energia`.`nacional_renovable_no_renovable` (
  `idnacional_renovable_no_renovable` INT NOT NULL,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  PRIMARY KEY (`idnacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fechas_idx` (`fechas_idfechas` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fechas`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
'''

tabla_comunidades = '''
CREATE TABLE IF NOT EXISTS `energia`.`comunidades` (
  `idcomunidades` INT NOT NULL,
  `comunidades` VARCHAR(45) NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB;
'''

tabla_comunidades_renovable_no_renovable = '''
CREATE TABLE IF NOT EXISTS `energia`.`comunidades_renovable_no_renovable` (
  `idcomunidades_renovable_no_renovable` INT NOT NULL,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  `comunidades_idcomunidades` INT NOT NULL,
  PRIMARY KEY (`idcomunidades_renovable_no_renovable`),
  INDEX `fk_comunidades_renovable_no_renovable_fechas1_idx` (`fechas_idfechas` ASC) VISIBLE,
  INDEX `fk_comunidades_renovable_no_renovable_comunidades1_idx` (`comunidades_idcomunidades` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_fechas1`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia`.`fechas` (`idfechas`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `energia`.`comunidades` (`idcomunidades`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
'''
tablas = [tabla_fechas, tabla_nacional_renovable_no_renovable, tabla_comunidades, tabla_comunidades_renovable_no_renovable]

for tabla in tablas:
    crear_insertar_tabla("energia","AlumnaAdalab", tabla)
