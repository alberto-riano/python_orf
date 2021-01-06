import re
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from functions.functions import read_file, orf_relationship, get_array_conditions
from model.model import classSchema, functionSchema


# Definimos el nombre del fichero principal
filename = 'tb_functions.pl'


# Creamos la sesion de spark en local
appName = 'PySpark PEC4'
master = 'local'

spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

# Inicializamos las variables
class_array = []
function_array = []

# Cargamos el array de clases y funciones
data_class, data_function = read_file(filename)

# Levantamos el contexto de spark y creamos los DF correspondientes
rdd_class = spark.sparkContext.parallelize(data_class)
rdd_function = spark.sparkContext.parallelize(data_function)

df_class = spark.createDataFrame(rdd_class, classSchema)
df_function = spark.createDataFrame(rdd_function, functionSchema)



# 1.1 Calcular cuántos ORFs pertenecen a cada clase.
print("1.1 El total de ORFs que pertenece a cada clase es: ")
df_function.groupby('id_class').agg(f.count('orf').alias('num_orf')).sort(f.desc('num_orf')).show(100)


# 1.2 Dado que el Bacilo de Koch afecta sobre todoo a los pulmones, queremos que mostréis por pantalla cuántos
# ORFs pertenecen a la clase que tiene Respiration como descripción. Mostrad el resultado por pantalla debidamente
# formateado (utilizando el método format() u otro similar), incluyendo un mensaje explicativo de los valores que
# enseñáis.

id_class = df_class.filter(f.col('description') == 'Respiration').first().id_class

df_orf_respiration = df_function.filter(f.col('id_class') == id_class)
print(f"1.2 El id de la clase que corresponde con la descripción de Respiration es: {id_class}.\nHay un total de "
      f"{df_orf_respiration.count()} ORFs pertenecientes a ella:")

df_orf_respiration.show()


# 2.1 El número de clases que contienen como mínimo un ORF con el patrón indicado en su descripción.

clases_condition = get_array_conditions(df_class, df_function)

clases_condition_uniques = list(dict.fromkeys(clases_condition))


print(f"2.1 El número de clases que contienen como mínimo un ORF con el patrón indicado "
      f"en su descripción es: {len(clases_condition_uniques)}")

# 2.2 El número promedio de ORFs con los cuales se relacionan los ORFs con el patrón indicado en su descripción.

porc_orf_relation = round(len(clases_condition) * 100 / df_function.count(), 2)
print(f"2.2 El número promedio de ORFs con los cuales se relacionan los ORFs con el patrón indicado "
      f"en su descripción es: {porc_orf_relation}%")


# 3 Para cada entero M entre 2 y 9 (ambos incluidos), calcula el número de clases que tienen como mínimo una
# dimensión mayor estricta (>) que 0 y a la vez múltiple de M

for num in range(2, 10):

    number_of_checks = 0

    for row_class in df_class.rdd.collect():

        id_class = str(row_class.id_class)
        class_dimension = id_class.replace("[", "").replace("]", "").split(",")

        for i in class_dimension:
            dim = int(i)
            if dim > 0 and (dim % num == 0):
                number_of_checks+=1
                break

    print(f"M = {num}: {number_of_checks} clases ")
