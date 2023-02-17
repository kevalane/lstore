from random import randint
import streamlit as st
from lstore.db import Database
from lstore.query import Query

def insert_record(query,record,key):
    query.insert(record[key])
    st.write("Record with key {} inserted successfully.".format(key))
    

def update_record(query,key, val1, val2, val3, val4):
    query.update(key,[val1, val2, val3, val4])
    st.write("Record with key {} updated successfully.".format(key))
    

def delete_record(query,key):
    st.write("Record with key {} deleted successfully.".format(key))
    query.delete(key)
    
def sum_data(query,start_key, end_key,col_index):
    sum = query.sum(start_key,end_key,col_index)
    st.write("The requested sum ={}".format(sum))

def select_data(query,search_key, search_key_index, projected_columns_index):
    query.select(search_key,search_key_index,projected_columns_index)


# def display_table(data):
#     st.write("| key | val1 | val2 | val3 | val4 |")
#     st.write("|:---:|:----:|:----:|:----:|:----:|")
#     for key in data:
#         st.write("| {} | {} | {} | {} | {} |".format(key, data[key]['val1'], data[key]['val2'], data[key]['val3'], data[key]['val4']))

def main(db):
    # st.title("DBMS Demo - Team sorry.py")
    # st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")
    query = Query(db)
    operation = st.selectbox("Select Operation", ["Select", "Insert", "Update", "Delete", "Sum"])
    
    if operation == "Insert" or operation == "Update":
        key, val1, val2, val3, val4 = 0,0,0,0,0
        if st.button("Generate Random Data"):
            key = 92106429 + randint(0,1000)
            val1, val2, val3, val4 = randint(0,20), randint(0,20), randint(0,20), randint(0,20)
        key = st.text_input("Enter key:",key)
        val1 = st.text_input("Enter value 1:",val1)
        val2 = st.text_input("Enter value 2:",val2)
        val3 = st.text_input("Enter value 3:",val3)
        val4 = st.text_input("Enter value 4:",val4)
            
    elif operation == "Delete":
        key = 0
        if st.button("Generate Random Data"):
            key = 92106429 + randint(0,1000)
        key = st.text_input("Enter key:",key) 
        

    elif operation == "Sum":
        start_range,end_range,col_index = 0 , 0
        if st.button("Generate Random Data"):
            start_range= 92106429 + randint(0,1000) 
            end_range = start_range + randint(0,100)
            col_index = randint(0,20)
        start_range = st.text_input("Enter Start Key",start_range)
        end_range = st.text_input("Enter End Key",end_range)
        col_index = st.number_input("Column Index to be summed up",step = 1, min_value = 0)
        

    elif operation == "Select":
        search_key = search_key_index = projected_columns_index = 0
        if st.button("Generate Random Data"):
            search_key = 92106429 + randint(0,1000)
            search_key_index = 92106429 + randint(0,1000) 
            projected_columns_index = 92106429 + randint(0,1000)
        search_key = st.text_input("Enter Search Key",search_key)
        search_key_index = st.text_input("Enter Search Key Index",search_key_index)
        projected_columns_index = st.text_input("Enter Projected Column Index",projected_columns_index)
         

    if st.button("Execute"):
        if operation == "Insert":
            insert_record(query,key, val1, val2, val3, val4)
            
        elif operation == "Update":
            update_record(query,key, val1, val2, val3, val4)
            
        elif operation == "Delete":
            delete_record(query,key)
            
        elif operation == "Sum":
            sum_data(query,start_range,end_range,col_index)
        
        elif operation == "Select":
            select_data(query,search_key, search_key_index, projected_columns_index)

def intro_page(): 
    st.title("DBMS Demo - Team sorry.py")
    st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")
    op = st.sidebar.selectbox("Select Operation",["Create a new table","Get an already exisitng table"]) 
    if op == "Create a new table":
        new_table()
    elif op == "Get an already exisitng table":
        get_table()

def new_table():  
    db = Database()
    name = st.text_input("Name for the Table")
    no_of_columns = st.number_input("Number of Columns",step = 1, min_value = 1)
    key_index  = st.number_input("Key Index",step = 1, min_value = 0)
    if st.button("Enter"):
        new = db.create_table(name,no_of_columns,key_index)
        if new:
            st.write("Created new table with the name: {}".format(name))
            main(new)

def get_table():
    db = Database()
    name = st.text_input("Name for the Table")
    if st.button("Enter"):
        table = db.get_table(name)
        if table: 
            st.write("Retreived table with name: {}".format(name)) 
            main(table)

intro_page()
