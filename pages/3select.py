import streamlit as st

from lstore.db import Database
from lstore.query import Query
 
st.set_page_config(
    page_title="Select"
)
st.title("DBMS Demo - You're Welcome.py")
st.subheader("Select")
db = Database()
db.open('./ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)
search_key = search_key_index =  0
projected_columns_index = [1,1,1,1,1]
# if st.button("Generate Random Data"):
#     search_key = 92106429 + randint(0,1000)
#     search_key_index = 92106429 + randint(0,1000) 
#     projected_columns_index = 92106429 + randint(0,1000)
search_key = st.number_input("Enter Search Key",search_key)
search_key_index = st.number_input("Enter Search Key Index",search_key_index)
# projected_columns_index = st.text_input("Enter Projected Column Index",projected_columns_index)


if st.button("Execute"):
    select = query.select(search_key,search_key_index,projected_columns_index)[0]
    st.write("The requested record {}".format(select))
