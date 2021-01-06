from pyspark.sql.types import *


class Function:
    def __init__(self, orf, id_class, name, description):
        self.orf = orf
        self.id_class = id_class
        self.name = name
        self.description = description

    def __str__(self):
        return "ORF:\t\t\t"+self.orf+"\nID_Class:\t\t"+self.id_class+"\nNombre:\t\t\t"\
               +self.name+"\nDescripcion:\t"+self.description


class Class:
    def __init__(self, id_class, description):
        self.id_class = id_class
        self.description = description

    def __str__(self):
        return "ID_Class:\t\t\t"+self.id_class+"\nDescripcion:\t\t"+self.description


classSchema = StructType([StructField("id_class", StringType()),\
                          StructField("description", StringType())])


functionSchema = StructType([StructField("orf", StringType()),\
                        StructField("id_class", StringType()),\
                        StructField("name", StringType()),\
                        StructField("description", StringType())])
