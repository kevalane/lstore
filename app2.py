from calendar import c
from random import randint

import streamlit as st
from lstore.db import Database
from lstore.query import Query
from array import *


# def display_table(data):
#     st.write("| key | val1 | val2 | val3 | val4 |")
#     st.write("|:---:|:----:|:----:|:----:|:----:|")
#     for key in data:
#         st.write("| {} | {} | {} | {} | {} |".format(key, data[key]['val1'], data[key]['val2'], data[key]['val3'], data[key]['val4']))

db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
def main():
    st.title("DBMS Demo - You're Welcome.py")
    st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")
    
    operation = st.selectbox("Select Operation", ["Select", "Insert", "Update", "Delete", "Sum"])
    
    if operation == "Insert" or operation == "Update":
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
            
    elif operation == "Delete":
        key = 0
        # if st.button("Generate Random Data"):
        #     key = 92106429 + randint(0,1000)
        key = st.number_input("Enter key:",key) 
        

    elif operation == "Sum":
        start_range,end_range,col_index = 0 , 0 , 0
        # if st.button("Generate Random Data"):
        #     start_range= 92106429 + randint(0,1000) 
        #     end_range = start_range + randint(0,100)
        #     col_index = randint(0,20)
        start_range = st.number_input("Enter Start Key",start_range,step = 1)
        end_range = st.number_input("Enter End Key",end_range,step = 1)
        col_index = st.number_input("Column Index to be summed up",step = 1)
        

    elif operation == "Select":
        search_key = search_key_index =  0
        projected_columns_index = [1,1,1,1]
        # if st.button("Generate Random Data"):
        #     search_key = 92106429 + randint(0,1000)
        #     search_key_index = 92106429 + randint(0,1000) 
        #     projected_columns_index = 92106429 + randint(0,1000)
        search_key = st.number_input("Enter Search Key",search_key)
        search_key_index = st.number_input("Enter Search Key Index",search_key_index)
        # projected_columns_index = st.text_input("Enter Projected Column Index",projected_columns_index)

    if st.button("Execute"):
        if operation == "Insert":
            # print(records[key])
            temp = query.insert(*records[key])
            # temp = query.insert(55, 2, 3, 4, 5)
            if temp:
                st.write("Record with key {} inserted successfully.".format(key))
            else: 
                st.write("Record not inserted")

        elif operation == "Update":
            temp = query.update(key,*records[key])
            if temp:
                st.write("Record with key {} updated successfully.".format(key))
            else:
                st.write("Record not updated succesfully")

        elif operation == "Delete":
            temp = query.delete(key)
            if temp:
                st.write("Record with key {} deleted successfully.".format(key))
            else:
                st.write("Record with key {} wasn't deleted.".format(key))

        elif operation == "Sum":
            sum = query.sum(start_range,end_range,col_index)
            st.write("The requested sum ={}".format(sum))
        
        elif operation == "Select":
            select = query.select(search_key,search_key_index,projected_columns_index)
            st.write("The requested record {}".format(select))
        db.close() 
        
main()