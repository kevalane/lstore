from lstore.table import Table

table = Table("test", 5, 0)

table.add_record([444444, 1, 2, 3, 4])

for i in range(40):
    table.add_record([44444+i, 1 + i, 2 + i, 3 + i, 4 + i])

# 32nd insert
print(table.get_record(44476, True))

table.update_record(44476, [None, 1, 2, 3, 4])
print(table.page_directory)
print(table.get_record(44476, True))
table.update_record(44476, [None, None, 22, None, None])
print(table.page_directory)
print(table.get_record(44476, True))
table.update_record(44476, [None, None, None, 33, None])
print(table.page_directory)
print(table.get_record(44476, True))
table.update_record(44476, [None, None, 44, None, None])
print(table.page_directory)
print(table.get_record(44476, True))

print(table.get_record(44476, True))