# NOTA
# Sustituir el valor de self.absolute_path en la clase EnergiaPoblación
# por la ruta donde se tengan los ficheros

import pandas as pd

from src.soporte import EnergiaPoblacion
from src.soporte import Cargar

# Instanciar las clases
cls_energia_poblacion = EnergiaPoblacion(2011, 2022, "data")
cls_carga = Cargar("energia3","AlumnaAdalab")

# Limpieza Población Comunidades
df_poblacion_ccaa = cls_energia_poblacion.open_csv("data","poblacion_comunidades")
cls_energia_poblacion.drop_column(df_poblacion_ccaa, "Comunidades_y_Ciudades_Autónomas")

# Limpieza Energia Nacional
df_energia_nacional = cls_energia_poblacion.info_energia_nacional()
cls_energia_poblacion.round_two(df_energia_nacional, ["value", "percentage"])
cls_energia_poblacion.create_datetime_col(df_energia_nacional, "datetime", "fecha")
cls_energia_poblacion.drop_column(df_energia_nacional, "datetime")

# Limpieza Energia CCAA
df_energia_ccaa = cls_energia_poblacion.info_energia_ccaa()
cls_energia_poblacion.round_two(df_energia_ccaa, ["value", "percentage"])
cls_energia_poblacion.create_datetime_col(df_energia_ccaa, "datetime", "fecha")
cls_energia_poblacion.drop_column(df_energia_ccaa, "datetime")

# Creación BBDD y tablas

cls_carga.crear_bbdd()

tabla_fechas = '''
CREATE TABLE IF NOT EXISTS `energia3`.`fechas` (
  `idfechas` INT NOT NULL AUTO_INCREMENT,
  `fecha` DATE NULL,
  PRIMARY KEY (`idfechas`))
ENGINE = InnoDB;
'''

tabla_nacional_renovable_no_renovable = '''
CREATE TABLE IF NOT EXISTS `energia3`.`nacional_renovable_no_renovable` (
  `idnacional_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NULL,
  `tipo_energia` VARCHAR(45) NULL,
  `valor` DECIMAL NULL,
  `fechas_idfechas` INT NOT NULL,
  PRIMARY KEY (`idnacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fechas_idx` (`fechas_idfechas` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fechas`
    FOREIGN KEY (`fechas_idfechas`)
    REFERENCES `energia3`.`fechas` (`idfechas`))
ENGINE = InnoDB;
'''

tabla_comunidades = '''
CREATE TABLE IF NOT EXISTS `energia3`.`comunidades` (
  `idcomunidades` INT NOT NULL AUTO_INCREMENT,
  `comunidades` VARCHAR(45) NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB;
'''

tabla_comunidades_renovable_no_renovable = '''
CREATE TABLE IF NOT EXISTS `energia3`.`comunidades_renovable_no_renovable` (
  `idcomunidades_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
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
    REFERENCES `energia3`.`fechas` (`idfechas`),
  CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `energia3`.`comunidades` (`idcomunidades`))
ENGINE = InnoDB;
'''

tablas = [tabla_fechas, tabla_nacional_renovable_no_renovable, tabla_comunidades, tabla_comunidades_renovable_no_renovable]

for tabla in tablas:
    cls_carga.crear_insertar_tabla(tabla)

# Inserción en 'fechas'

df_fechas = pd.concat([df_energia_ccaa['fecha'], df_energia_nacional['fecha']], axis = 0, join = "inner", ignore_index = True)
df_fechas = pd.DataFrame(df_fechas)

duplicados_fechas = df_fechas.duplicated().sum()

if duplicados_fechas > 0:
    df_fechas = df_fechas.drop_duplicates()
    
for indice, fila in df_fechas.iterrows(): 
    
    query_fechas = f"""
            INSERT INTO fechas (fecha) 
            VALUES ("{fila['fecha']}");
            """
    cls_carga.crear_insertar_tabla(query_fechas)
    
# Inserción en 'comunidades'

df_comunidades = df_energia_ccaa[['comunidad']]

duplicados_fechas = df_comunidades.duplicated().sum()

if duplicados_fechas > 0:
    df_comunidades = df_comunidades.drop_duplicates()
    
for indice, fila in df_comunidades.iterrows():
    
    query_comunidad = f"""
                INSERT INTO comunidades (comunidades) 
                VALUES ("{fila['comunidad']}");
                """
                
    comunidades = cls_carga.check_comunidades()
    
    if len(comunidades) == 0 or fila['comunidad'] not in comunidades[0]: 
        cls_carga.crear_insertar_tabla(query_comunidad)
    else:
        print(f"{fila['comunidades']} ya esta en nuestra BBDD")
        
# Inserción en 'comunidades_renovable_no_renovable'

for indice, fila in df_energia_ccaa.iterrows():
    
    id_comunidad = cls_carga.sacar_id_comunidad(fila['comunidad'])
    id_fecha = cls_carga.sacar_id_fecha(fila["fecha"])
        
    query_valores_ccaa = f"""
                INSERT INTO comunidades_renovable_no_renovable (porcentaje, tipo_energia, valor, fechas_idfechas, comunidades_idcomunidades) 
                VALUES ({fila['percentage']}, "{fila['tipo_energia']}", {fila['value']}, {id_fecha}, {id_comunidad});
                """
    cls_carga.crear_insertar_tabla(query_valores_ccaa)
    
# Inserción en 'nacional_renovable_no_renovable'

for indice, fila in df_energia_nacional.iterrows():
    
    id_fecha = cls_carga.sacar_id_fecha(fila["fecha"])
    
    query_valores_nacional = f"""
                INSERT INTO nacional_renovable_no_renovable (porcentaje, tipo_energia, valor, fechas_idfechas)
                VALUES ({fila['percentage']}, "{fila['tipo_energia']}", {fila['value']}, {id_fecha});
                """
    cls_carga.crear_insertar_tabla(query_valores_nacional)