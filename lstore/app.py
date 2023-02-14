from ast import Delete
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

# def display_table(data):
#     st.write("| key | val1 | val2 | val3 | val4 |")
#     st.write("|:---:|:----:|:----:|:----:|:----:|")
#     for key in data:
#         st.write("| {} | {} | {} | {} | {} |".format(key, data[key]['val1'], data[key]['val2'], data[key]['val3'], data[key]['val4']))

def main():
    st.title("DBMS Demo")
    
    operation = st.selectbox("Select Operation", ["Select", "Insert", "Update", "Delete", "Sum"])
    
    if operation == "Insert" or operation == "Update":
        key = st.text_input("Enter key:")
        val1 = st.text_input("Enter value 1:")
        val2 = st.text_input("Enter value 2:")
        val3 = st.text_input("Enter value 3:")
        val4 = st.text_input("Enter value 4:")

    elif operation == "Delete":
       key = st.text_input("Enter key:") 

    elif operation == "Sum":
        start_range = st.text_input("Enter Start Key")
        end_range = st.text_input("Enter End Key")
             
    if st.button("Execute"):
        if operation == "Insert":
            insert_record(key, val1, val2, val3, val4)
            
        elif operation == "Update":
            update_record(key, val1, val2, val3, val4)
            
        elif operation == "Delete":
            delete_record(key)
            
        elif operation == "Sum":
            sum_data(start_range,end_range)

if __name__ == "__main__":
    main()