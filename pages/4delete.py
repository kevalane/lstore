import streamlit as st

from lstore.db import Database
from lstore.query import Query
 
st.set_page_config(
    page_title="Delete"
)
st.title("DBMS Demo - You're Welcome.py")
st.subheader("Delete")
db = Database()
db.open('./ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)
key = 0
# if st.button("Generate Random Data"):
#     key = 92106429 + randint(0,1000)
key = int(st.number_input("Enter key:",key))

if st.button("Execute"):
    temp = query.delete(key)
    if temp:
        st.write("Record with key {} deleted successfully.".format(key))
    else:
        st.write("Record with key {} wasn't deleted.".format(key))
db.close()  