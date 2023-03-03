import streamlit as st

from lstore.db import Database
from lstore.query import Query

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)
# st.sidebar("Select an operation")
st.title("DBMS Demo - You're Welcome.py")
st.subheader("Roshni Prasad, Rishabh Jain, Ashton Coates, Kevin Rasmusson, Catherine Yaroslavtseva")


db = Database()
db.open('./ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
db.close()