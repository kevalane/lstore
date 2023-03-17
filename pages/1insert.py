import streamlit as st
import pandas as pd 
from lstore.db import Database
from lstore.query import Query
from random import randint

st.set_page_config(
    page_title="Insert"
)
st.title("DBMS Demo - YW.py")
st.subheader("Insert")
db = Database()
# db.open('./ECS165')
# name = st.session_state("table")
# grades_table = db.get_table(name)
# query = Query(grades_table)
query = st.session_state["q"]
key, temp_vals = 0,0
# if st.button("Generate Random Data"):
#     key = 92106429 + randint(0,1000)
#     temp_vals = (randint(0,20)),str(randint(0,20)),str(randint(0,20)),str(randint(0,20))
key = st.number_input("Enter key:",key)
val = st.text_input("Enter values seperated by commas: ",temp_vals)
vals = val.split(',')
valsint = [eval(i) for i in vals]
record = [key] + valsint
records ={}
records[key] = record
count = 0
if st.button("Execute"):
    # print(records[key])
    temp = query.insert(*records[key])
    # temp = query.insert(55, 2, 3, 4, 5)
    if temp:
        st.write("Record with key {} inserted successfully.".format(key)) 
        d = {'Key/Col 0':[record[0]],'Col 1':[record[1]],"Col 2":[record[2]],"Col 3":[record[3]],"Col 4":[record[4]]}
        df = pd.DataFrame(d)
        df.to_excel("insert.xlsx",index=False)
        st.table(df)
    else: 
        st.write("Record not inserted")
db.close()