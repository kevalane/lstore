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

start_range,end_range,col_index = 0 , 0 , 0

    #     start_range= 92106429 + randint(0,1000) 
    #     end_range = start_range + randint(0,100)
    #     col_index = randint(0,20)
start_range = st.number_input("Enter Start Key",start_range,step = 1)
end_range = st.number_input("Enter End Key",end_range,step = 1)
col_index = st.number_input("Column Index to be summed up",step = 1)
if st.button("Execute"):
    sum = query.sum(start_range,end_range,col_index)
    st.write("The requested sum ={}".format(sum))