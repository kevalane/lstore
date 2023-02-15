from random import randint
import streamlit as st

# from query import Query

def insert_record(key, val1, val2, val3, val4):
    # Query.insert(key, val1, val2, val3, val4)
    st.write("Record with key {} inserted successfully.".format(key))
    pass

def update_record(key, val1, val2, val3, val4):
    # Query.update(key,val1, val2, val3, val4)
    st.write("Record with key {} updated successfully.".format(key))
    pass

def delete_record(key):
    st.write("Record with key {} deleted successfully.".format(key))
    # Query.delete(key)
    pass
def sum_data(start_key, end_key):
    # Query.sum(start_key,end_key)
    pass
def select_data(search_key, search_key_index, projected_columns_index):
    # Query.select(search_key,search_key_index,projected_columns_index)
    pass

# def display_table(data):
#     st.write("| key | val1 | val2 | val3 | val4 |")
#     st.write("|:---:|:----:|:----:|:----:|:----:|")
#     for key in data:
#         st.write("| {} | {} | {} | {} | {} |".format(key, data[key]['val1'], data[key]['val2'], data[key]['val3'], data[key]['val4']))

def main():
    st.title("DBMS Demo")
    
    operation = st.selectbox("Select Operation", ["Select", "Insert", "Update", "Delete", "Sum"])
    
    if operation == "Insert" or operation == "Update":
        key_random, val1_random , val2_random, val3_random, val4_random = 0,0,0,0,0
        if st.button("Generate Random Data"):
            key_random = 92106429 + randint(0,1000)
            val1_random , val2_random, val3_random, val4_random = randint(0,20), randint(0,20), randint(0,20), randint(0,20)
        key = st.text_input("Enter key:",key_random)
        val1 = st.text_input("Enter value 1:",val1_random)
        val2 = st.text_input("Enter value 2:",val2_random)
        val3 = st.text_input("Enter value 3:",val3_random)
        val4 = st.text_input("Enter value 4:",val4_random)
        
            
    elif operation == "Delete":
        key_random = 0
        if st.button("Generate Random Data"):
            key_random = 92106429 + randint(0,1000)
        key = st.text_input("Enter key:",key_random) 
        

    elif operation == "Sum":
        start_range_random,end_range_random = 0 , 0
        if st.button("Generate Random Data"):
            start_range_random = 92106429 + randint(0,1000) 
            end_range_random = start_range_random + randint(0,100) 
        start_range = st.text_input("Enter Start Key",start_range_random)
        end_range = st.text_input("Enter End Key",end_range_random)
        

    elif operation == "Select":
        search_key_random = search_key_index_random = projected_columns_index_random = 0
        if st.button("Generate Random Data"):
            search_key_random = 92106429 + randint(0,1000)
            search_key_index_random = 92106429 + randint(0,1000) 
            projected_columns_index_random = 92106429 + randint(0,1000)
        search_key = st.text_input("Enter Search Key",search_key_random)
        search_key_index = st.text_input("Enter Search Key Index",search_key_index_random)
        projected_columns_index = st.text_input("Enter Projected Column Index",projected_columns_index_random)
         

    if st.button("Execute"):
        if operation == "Insert":
            insert_record(key, val1, val2, val3, val4)
            
        elif operation == "Update":
            update_record(key, val1, val2, val3, val4)
            
        elif operation == "Delete":
            delete_record(key)
            
        elif operation == "Sum":
            sum_data(start_range,end_range)
        
        elif operation == "Select":
            select_data(search_key, search_key_index, projected_columns_index)

if __name__ == "__main__":
    main()