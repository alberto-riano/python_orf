
def orf_relationship(class_description, orf_description):

    # initializing test list
    test_list = orf_description.replace(",", "").split(" ")

    # using list comprehension
    # checking if string contains list element
    res = [ele for ele in test_list if (ele.lower() in class_description.lower())]

    # print result
    return bool(res)

a = "Electron transport "
b = "fsdfds transfer flavoprotein alpha subunit"

print(orf_relationship(a, b))

