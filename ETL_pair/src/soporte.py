import pandas as pd
import requests
import mysql.connector

class EnergiaPoblacion:
    
    def __init__(self, start_year = 2011, end_year = 2022, path_to_save = "data"):
        self.start_year = start_year
        self.end_year = end_year
        self.path_to_save = path_to_save
        self.absolute_path = "C:/Users/Andrea/Desktop/Adalab/MODULO_2/promoE_modulo2_sprint2_pairprogramming_AnaGuardiayAndreaCabrera/ETL_pair/"
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
            path_file = f"{self.absolute_path}/{path}/{file_name}_{self.start_year}_{self.end_year}.csv"
        else:
            path_file = f"{self.absolute_path}/{path}/{file_name}.csv"
        
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
            path_file = f"{self.absolute_path}/{path}/{file_name}_{self.start_year}_{self.end_year}.pkl"
        else:
            path_file = f"{self.absolute_path}/{path}/{file_name}.pkl"
            
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
            path_file = f"{self.absolute_path}/{self.path_to_save}/{file_name}_{self.start_year}_{self.end_year}.csv"
        else:
            path_file = f"{self.absolute_path}/{self.path_to_save}/{file_name}.csv"
        
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
            path_file = f"{self.absolute_path}/{self.path_to_save}/{file_name}_{self.start_year}_{self.end_year}.pkl"
        else:
            path_file = f"{self.absolute_path}/{self.path_to_save}/{file_name}.pkl"
            
        print(f"Guardando en: {path_file}")  
        df.to_pickle(path_file)
        
class Cargar:
    
    def __init__(self, nombre_bbdd, contraseña):
        
        self.nombre_bbdd = nombre_bbdd
        self.contraseña = contraseña

   
    def crear_bbdd(self):

        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}') 
        mycursor = mydb.cursor()
        print("Conexión realizada con éxito")

        try:
            mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.nombre_bbdd};")
            
        except:
            print("La BBDD ya existe")
            
      
    def crear_insertar_tabla(self, query):
        
        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password=f'{self.contraseña}', 
                                       database=f"{self.nombre_bbdd}") 
        mycursor = mydb.cursor()
        
        try:
            mycursor.execute(query)
            mydb.commit()
          
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
            

    def check_comunidades(self):
    
        mydb = mysql.connector.connect(user='root',
                                      password=f"{self.contraseña}",
                                      host='127.0.0.1',
                                      database=f"{self.nombre_bbdd}")
        mycursor = mydb.cursor()

        # query para extraer los valores únicos de ciudades de la tabla de localidades 
        query_existe_ciudad = f"""
                SELECT DISTINCT comunidades FROM comunidades
                """
        mycursor.execute(query_existe_ciudad)
        comunidades = mycursor.fetchall()
        return comunidades
    
    # método para sacar el id de una ciudad en concreto 
    def sacar_id_comunidad(self, comunidad):
        
        mydb = mysql.connector.connect(user='root',
                                       password= f'{self.contraseña}',
                                       host='127.0.0.1', 
                                       database=f"{self.nombre_bbdd}")
        mycursor = mydb.cursor()
        
        try:
            query_sacar_id = f"SELECT idcomunidades FROM comunidades WHERE comunidades = '{comunidad}'"
            mycursor.execute(query_sacar_id)
            id_ = mycursor.fetchall()[0][0]
            return id_
        
        except: 
            return "Sorry, no tenemos esa comunidad en la BBDD y por lo tanto no te podemos dar su id. "
     
    
    def sacar_id_fecha(self, fecha):
        mydb = mysql.connector.connect(user='root', password=f'{self.contraseña}',
                                          host='127.0.0.1', database=f"{self.nombre_bbdd}")
        mycursor = mydb.cursor()

        try:
            query_sacar_id = f"SELECT idfechas FROM fechas WHERE fecha = '{fecha}'"
            mycursor.execute(query_sacar_id)
            id_ = mycursor.fetchall()[0][0]
            return id_
        
        except: 
             return "Sorry, no tenemos esa fecha en la BBDD y por lo tanto no te podemos dar su id. "
