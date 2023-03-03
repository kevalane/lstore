import streamlit as st

from lstore.db import Database
from lstore.query import Query

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)
# st.sidebar("Select an operation")
st.title("DBMS Demo - YW.py")
st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")


db = Database()
db.open('./ECS165')
# grades_table = db.create_table('Grades', 5, 0)

op = st.selectbox("Select Operation",["Create a new table","Get an already exisitng table"]) 

if op == "Create a new table":
    name = st.text_input("Name for the Table")
    no_of_columns = st.number_input("Number of Columns",step = 1, min_value = 1)
    key_index  = st.number_input("Key Index",step = 1, min_value = 0)
    if st.button("Enter"):
        grades_table = db.create_table(name,no_of_columns,key_index)
        if grades_table:
            st.write("Created new table with the name: {}".format(name)) 
            query = Query(grades_table)
            st.session_state["q"] = query
            st.session_state["table"] = name
elif op == "Get an already exisitng table":
    name = st.text_input("Name for the Table")

    if st.button("Enter"):
        grades_table = db.get_table(name)
        if grades_table is not None: 
            st.write("Retreived table with name: {}".format(name)) 
            query = Query(grades_table)
            st.session_state["q"] = query
            st.session_state["table"] = name
db.close()