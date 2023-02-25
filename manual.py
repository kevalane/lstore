from lstore.table import Table

table = Table("test", 5, 0)

table.add_record([444444, 1, 2, 3, 4])

for i in range(600):
    table.add_record([i, 1, 2, 3, 4])