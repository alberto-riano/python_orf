import re
from pyspark.sql import functions as f

def read_file(filenamo):

    file = open(filenamo)
    data_class = []
    data_function = []

    for line in file.readlines():

        split_line = line.replace(").", "").replace("\n", "").replace("\"", "").replace("\'", "").split("(")
        type_entry = split_line[0]

        parts = re.split(r'(?![^)(]*\([^)(]*?\)\)),(?![^\[]*\])', split_line[1])

        if type_entry == "function":
            data_function.append((parts[0], parts[1], parts[2].strip(), parts[3].strip()))
        elif type_entry == "class":
            data_class.append((parts[0], parts[1].strip()))
        else:
            print(f"Tipo de entrada {type_entry} no reconocido")

    return data_class, data_function

def orf_relationship(class_description, orf_description):

    # initializing test list
    test_list = orf_description.replace(",", "").split(" ")

    # using list comprehension
    # checking if string contains list element
    res = [ele for ele in test_list if (ele.lower() in class_description.lower())]

    # print result
    return bool(res)

def get_array_conditions(df_class, df_function):

    clases_condition = []

    for row_class in df_class.rdd.collect():

        df_orf_filter = df_function.filter(f.col('id_class') == row_class.id_class)

        for row_function in df_orf_filter.rdd.collect():

            check = orf_relationship(row_class.description, row_function.description)

            if check:
                clases_condition.append(row_class.id_class)

    return clases_condition