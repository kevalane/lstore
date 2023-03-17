import streamlit as st
import pandas as pd
from lstore.db import Database
from lstore.query import Query
 
st.set_page_config(
    page_title="Update"
)

db = Database()
db.open('./ECS165')
# grades_table = db.get_table('Grades')
# query = Query(grades_table)
query = st.session_state["q"]
st.title("DBMS Demo - YW.py")
st.subheader("Update")

key, val = 0,0
# if st.button("Generate Random Data"):
#     key = 92106429 + randint(0,1000)
#     val1, val2, val3, val4 = randint(0,20), randint(0,20), randint(0,20), randint(0,20)
key = st.number_input("Enter key:",key)
val = st.text_input("Enter values seperated by commas: ",val)
vals = val.split(',')
valsint = [eval(i) for i in vals]
record = [key] + valsint
records ={}
records[key] = record
if st.button("Execute"):
    temp = query.update(key,*records[key])
    if temp:
        d = {'Key/Col 0':[record[0]],'Col 1':[record[1]],"Col 2":[record[2]],"Col 3":[record[3]],"Col 4":[record[4]]}
        df = pd.DataFrame(d)
        st.write("Record with key {} updated successfully.".format(key))
        df.to_excel("update.xlsx",index=False)
        st.table(df)
    else:
        st.write("Record not updated succesfully")
db.close()