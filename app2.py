from random import randint
<<<<<<< Updated upstream
from turtle import onclick
=======
from site import execsitecustomize

>>>>>>> Stashed changes
import streamlit as st
from lstore.db import Database
from lstore.query import Query


# def display_table(data):
#     st.write("| key | val1 | val2 | val3 | val4 |")
#     st.write("|:---:|:----:|:----:|:----:|:----:|")
#     for key in data:
#         st.write("| {} | {} | {} | {} | {} |".format(key, data[key]['val1'], data[key]['val2'], data[key]['val3'], data[key]['val4']))

<<<<<<< Updated upstream
def main():
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)
    st.title("DBMS Demo - Team sorry.py")
    st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")
    query = Query(grades_table)
    operation = st.selectbox("Select Operation", ["Select", "Insert", "Update", "Delete", "Sum"])
=======

def page(query,operation):
>>>>>>> Stashed changes
    
    if operation == "Insert" or operation == "Update":
        key, val = 0,0
        # if st.button("Generate Random Data"):
        #     key = 92106429 + randint(0,1000)
        #     val1, val2, val3, val4 = randint(0,20), randint(0,20), randint(0,20), randint(0,20)
        key = st.text_input("Enter key:",key)
        val = st.text_input("Enter values seperated by commas: ",val)
        vals = val.split(',')
        record = [key] + vals
        records ={}
        records[key] = [record]
            
    elif operation == "Delete":
        key = 0
        # if st.button("Generate Random Data"):
        #     key = 92106429 + randint(0,1000)
        key = st.text_input("Enter key:",key) 
        

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
        search_key = search_key_index = projected_columns_index = 0
        # if st.button("Generate Random Data"):
        #     search_key = 92106429 + randint(0,1000)
        #     search_key_index = 92106429 + randint(0,1000) 
        #     projected_columns_index = 92106429 + randint(0,1000)
        search_key = st.text_input("Enter Search Key",search_key)
        search_key_index = st.text_input("Enter Search Key Index",search_key_index)
        # projected_columns_index = st.text_input("Enter Projected Column Index",projected_columns_index)
    
    if st.button("Execute"):
        if operation == "Insert":
            # print(len(*records[key]))
            temp = query.insert(*records[key])
            if temp:
                st.write("Record with key {} inserted successfully.".format(key))
            else: 
                st.write("Record not inserted")

        elif operation == "Update":
            temp = query.update(*records[key])
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
            select = query.select(search_key,search_key_index,[projected_columns_index])
            st.write("The requested record {}".format(select))


<<<<<<< Updated upstream
main()
=======
def main():
    db = Database()
    db.open('./ECS165')
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    st.title("DBMS Demo - You're Welcome.py")
    st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")
    operation = st.sidebar.selectbox("Select Operation", ["Select", "Insert", "Update", "Delete", "Sum"])
    page(query,operation)
    db.close() 
main()
>>>>>>> Stashed changes
