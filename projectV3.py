
import pandas as pd
import psycopg2
import streamlit as st
import numpy as np
import datetime
from configparser import ConfigParser
from streamlit.ScriptRunner import RerunException


'# Application: Market Database'

def get_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    return {k: v for k, v in parser.items(section)}


def query_db(sql: str):
    # print(f'Running query_db(): {sql}')
    db_info = get_config()
    # Connect to an existing database
    conn = psycopg2.connect(**db_info)
    # Open a cursor to perform database operations
    cur = conn.cursor()
    # Execute a command: this creates a new table
    cur.execute(sql)
    # Obtain data
    data = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    # Make the changes to the database persistent
    conn.commit()
    # Close communication with the database
    cur.close()
    conn.close()
    df = pd.DataFrame(data=data, columns=column_names)
    return df
    
def insert_producer(sql,p_name, p_phone, p_email, p_bank):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(p_name, p_phone, p_email, p_bank))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def insert_product(sql,p_id, p_name, p_unit, p_code, p_producer, p_cost, p_category):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(p_id, p_name, p_unit, p_code, p_producer, p_cost, p_category))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def insert_shelf(sql,s_id, s_location):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(s_id, s_location))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def insert_locates(sql,p_id,s_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(p_id,s_id))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def insert_inventory(sql,pur_id, p_id, i_quantity, e_date):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(pur_id, p_id, i_quantity, e_date))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def insert_membership(sql,m_id, m_name, m_dob, m_balance):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(m_id, m_name, m_dob, m_balance))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def delete_producer(sql,p_name):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(p_name,))
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def delete_product(sql,p_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(p_id,))
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def delete_shelf(sql,s_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(s_id,))
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def delete_locates(sql,p_id,s_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(p_id, s_id,))
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def delete_inventory(sql,pur_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(pur_id,))
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
           
def delete_membership(sql,m_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(m_id,))
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def update_inventory(sql,i_quantity, pur_id):
    # print(f'Running query_db(): {sql}')
    try:
        db_info = get_config()
        # Connect to an existing database
        conn = psycopg2.connect(**db_info)
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: this creates a new table
        cur.execute(sql,(i_quantity, pur_id))
        updated_rows = cur.rowcount
        conn.commit()
        # Close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def superoperations():
    option=st.sidebar.selectbox('Choose your opeartion', ('View data','Insert date', 'Delete date', 'Update data'), key="selectboxo")
    if option== 'View data':
        view()
    elif option=='Insert date':
        insertdata()
    elif option=='Delete date':
        deletedata()
    elif option=='Update data':
        updatedata()

def insertdata():
    '## Read tables'
    sql_all_table_names = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
    all_table_names = query_db(sql_all_table_names)['relname'].tolist()
    table_name = st.selectbox('Choose a table', all_table_names)
    if table_name:
        f'Display the table'
        sql_table = f'select * from {table_name};'
        df = query_db(sql_table)
        st.dataframe(df)

    if table_name=='producer':
        updated_rows=0
        p_name=st.text_input("Enter the producer name",'')
        p_phone=st.text_input("Enter the phone number",'')
        p_email=st.text_input("Enter the email",'')
        p_bank=st.text_input("Enter the bank account",'')
        insertsql="insert into producer(producer_name, phone_num, email_address, bank_account) VALUES (%s,%s,%s,%s);"
        if st.button('insert'):
            insert_producer(insertsql,p_name, p_phone, p_email, p_bank)
            st.write('yes')

    if table_name=='product_producedby':
        updated_rows=0
        p_id=st.text_input("Enter the product id",'')
        p_name=st.text_input("Enter the product name",'')
        p_unit=st.text_input("Enter the product unit",'')
        p_code=st.text_input("Enter the product code",'')
        p_producer=st.text_input("Enter the product producer",'')
        p_cost=st.number_input("Enter the product cost")
        p_category=st.text_input("Enter the product category",'')
        insertsql="insert into product_producedby(pid, product_name, unit, bar_code, producer, cost, category) VALUES (%s,%s,%s,%s,%s,%s,%s);"
        if st.button('insert'):
            insert_product(insertsql,p_id, p_name, p_unit, p_code, p_producer, p_cost, p_category)
            st.write('yes')
    
    if table_name=='shelf':
        updated_rows=0
        s_id=st.text_input("Enter the shelf id",'')
        s_location=st.text_input("Enter the shelf's location",'')
        insertsql="insert into shelf(sid, s_location) VALUES (%s,%s);"
        if st.button('insert'):
            insert_shelf(insertsql,s_id, s_location)
            st.write('yes')
            
    if table_name=='locates':
        updated_rows=0
        p_id=st.text_input("Enter the product id",'')
        s_id=st.text_input("Enter the shelf id",'')
        insertsql="insert into locates(pid, sid) VALUES (%s,%s);"
        if st.button('insert'):
            insert_locates(insertsql,p_id, s_id)
            st.write('yes')
            
    if table_name=='inventory_of':
        updated_rows=0
        pur_id=st.text_input("Enter the purchase id",'')
        p_id=st.text_input("Enter the product id",'')
        i_quantity=st.text_input("Enter the quantity",'')
        e_date=st.text_input("Enter the expire date",'')
        insertsql="insert into inventory_of(pur_id, pid, in_quantity, expire_date) VALUES (%s,%s,%s,%s);"
        if st.button('insert'):
            insert_inventory(insertsql, pur_id, p_id, int(i_quantity), e_date)
            st.write('yes')
            
    if table_name=='membership':
        updated_rows=0
        m_id=st.text_input("Enter the membership id",'')
        m_name=st.text_input("Enter the name",'')
        m_dob=st.text_input("Enter the date of birth",'')
        m_balance=st.number_input("Enter the balance")
        insertsql="insert into membership(mid, m_name, dob, balance) VALUES (%s,%s,%s,%s);"
        if st.button('insert'):
            insert_membership(insertsql, m_id, m_name, m_dob, m_balance)
            st.write('yes')
            


def deletedata():
    '## Read tables'
    sql_all_table_names = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
    all_table_names = query_db(sql_all_table_names)['relname'].tolist()
    table_name = st.selectbox('Choose a table', all_table_names)
    if table_name:
        f'Display the table'
        sql_table = f'select * from {table_name};'
        df = query_db(sql_table)
        st.dataframe(df)

    if table_name=='producer':
        updated_rows=0
        p_name=st.text_input("Enter the producer name you want to delete",'')
        deletesql="DELETE FROM producer where producer_name = %s ;"
        if st.button('delete'):
            delete_producer(deletesql,p_name)
            st.write('yes')

    if table_name=='product_producedby':
        updated_rows=0
        p_id=st.text_input("Enter the product id you want to delete",'')
        deletesql="DELETE FROM product_producedby where pid = %s ;"
        if st.button('delete'):
            delete_product(deletesql,p_id)
            st.write('yes')
    
    if table_name=='shelf':
        s_id=st.text_input("Enter the shelf id you want to delete",'')
        deletesql="DELETE FROM shelf where sid = %s;"
        if st.button('delete'):
            delete_shelf(deletesql,s_id)
            st.write('yes')
            
    if table_name=='locates':
        updated_rows=0
        p_id=st.text_input("Enter the product id you want to delete",'')
        s_id=st.text_input("Enter the shelf id you want to delete",'')
        deletesql="DELETE FROM locates where pid = %s and sid = %s;"
        if st.button('delete'):
            delete_locates(deletesql,p_id, s_id)
            st.write('yes')
            
    if table_name=='inventory_of':
        updated_rows=0
        pur_id=st.text_input("Enter the purchase id you want to delete",'')
        deletesql="DELETE FROM inventory_of where pur_id = %s;"
        if st.button('delete'):
            delete_inventory(deletesql, pur_id)
            st.write('yes')
            
    if table_name=='membership':
        updated_rows=0
        m_id=st.text_input("Enter the membership id you want to delete",'')
        deletesql="DELETE FROM membership where mid = %s;"
        if st.button('delete'):
            delete_membership(deletesql, m_id)
            st.write('yes')


def updatedata():
    '## Read tables'
    sql_all_table_names = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
    all_table_names = query_db(sql_all_table_names)['relname'].tolist()
    table_name = st.selectbox('Choose a table', all_table_names)
    if table_name:
        f'Display the table'
        sql_table = f'select * from {table_name};'
        df = query_db(sql_table)
        st.dataframe(df)

    if table_name=='producer':
        p_name=st.text_input("Enter the producer name you want to update",' ')
        p_phone=st.text_input("Enter the phone number",'')
        p_email=st.text_input("Enter the email",'')
        p_bank=st.text_input("Enter the bank account",'')
        updatesql="UPDATE producer SET phone_num= %s, email_address = %s, bank_account = %s where producer_name= %s;"
        if st.button('update'):
            insert_producer(updatesql, p_phone, p_email, p_bank, p_name)
            st.write('yes')

    if table_name=='product_producedby':
        updated_rows=0
        p_id=st.text_input("Enter the product id you want to update",' ')
        p_name=st.text_input("Enter the product name",' ')
        p_unit=st.text_input("Enter the product unit",' ')
        p_code=st.text_input("Enter the product code",' ')
        p_producer=st.text_input("Enter the product producer",' ')
        p_cost=st.number_input("Enter the product cost")
        p_category=st.text_input("Enter the product category",' ')
        updatesql="UPDATE product_producedby SET product_name = %s, unit=%s, bar_code=%s, producer = %s, cost=%s, category=%s WHERE pid=%s;"
        if st.button('update'):
            insert_product(updatesql, p_name, p_unit, p_code, p_producer, p_cost, p_category, p_id)
            st.write('yes')
            
    if table_name=='shelf':
        updated_rows=0
        s_id=st.text_input("Enter the shelf id you want to update",' ')
        s_location=st.text_input("Enter the shelf's location",' ')
        updatesql="UPDATE shelf SET s_location=%s WHERE sid=%s ;"
        if st.button('update'):
            insert_shelf(updatesql, s_location,s_id)
            st.write('yes')
            
    if table_name=='inventory_of':
        updated_rows=0
        pur_id=st.text_input("Enter the purchase id you want to update",' ')
        i_quantity=st.text_input("Enter the quantity",' ')
        updatesql="UPDATE inventory_of SET in_quantity = %s WHERE pur_id=%s;"
        if st.button('update'):
            update_inventory(updatesql, int(i_quantity), pur_id)
            st.write('yes')
            
    if table_name=='membership':
        updated_rows=0
        m_id=st.text_input("Enter the membership id you want to update",' ')
        m_name=st.text_input("Enter the name",' ')
        m_dob=st.text_input("Enter the date of birth",' ')
        m_balance=st.number_input("Enter the balance")
        updatesql="UPDATE membership SET m_name=%s, dob=%s, balance=%s WHERE mid=%s;"
        if st.button('update'):
            insert_membership(updatesql, m_name, m_dob, m_balance, m_id)
            st.write('yes')


def view():
    '## Read tables'
    
    sql_all_table_names = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
    all_table_names = query_db(sql_all_table_names)['relname'].tolist()
    table_name = st.selectbox('Choose a table', all_table_names)
    if table_name:
        f'Display the table'
    
        sql_table = f'select * from {table_name};'
        df = query_db(sql_table)
        st.dataframe(df)
    

    '## Search for products approaching best used before date'

    today = datetime.date.today()
    start_date = st.date_input('Start date', today)
    if start_date:
	f'Display the table'
	
    
    '## Query top N rating products of the grocery store (Group by, Join)'
    
    top_nratings = st.selectbox('Select N', ('5', '10', '15'), key="selectbox1")
    if top_nratings:
        f'Display the table'
        sql_ratings = f"select producer.producer_name, cast(avg(R.numStar) as Decimal (10,2)) as ratings from rating_rate as R, producer, product_producedBy as P where P.producer=producer.producer_name  and R.pid=P.pid group by producer_name order by ratings desc limit {top_nratings};"
        df = query_db(sql_ratings)
        st.dataframe(df)
        
    
    '## Query top N popular products of the grocery store (Group by, Join)'
    
    top_npopular = st.selectbox('Select N', ('5', '10', '15'), key="selectbox2")
    if top_npopular:
        f'Display the table'
        sql_npopular = f"select p.product_name, count(o.o_quantity) as num from order_history o, product_producedby p where o.pid = p.pid group by p.product_name order by num desc limit {top_npopular} ;"
        df = query_db(sql_npopular)
        st.dataframe(df)
    
    '## Query sales amount of products(Group by, Join)'
    st.write('<style>div.Widget.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)
    sales_amount = st.radio('Query sales amount', ('Query all','Vegetable', 'Fruit', 'Grocery', 'Kitchen', 'Drink', 'Electronic Devices', 'Phone', 'Food'), index=0, key="sales_amount")
    if sales_amount=='Query all':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Vegetable':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Vegetable' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Fruit':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Fruit' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Grocery':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Grocery' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Kitchen':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Kitchen' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Drink':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Drink' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Electronic Product':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Electronic Product' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'Phone':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as total_value from order_history o, product_producedby p where o.pid = p.pid and p.category = 'Phone' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)
    elif sales_amount == 'food':
       f'Display the table'
       sql_sales = f"select p.product_name, sum(o.o_quantity * o.o_price) as sales from order_history o, product_producedby p where o.pid = p.pid and p.category = 'food' group by p.product_name;"
       df = query_db(sql_sales)
       st.dataframe(df)

    '## Show total inventory value of each producer(Group by, Join)'
    sql_in_value=f"select P.producer, sum(I.in_quantity*P.cost) as Total_inventory_value from product_producedBy as P, inventory_of as I WHERE I.pid=P.pid group by P.producer order by Total_inventory_value desc"
    df = query_db(sql_in_value)
    st.dataframe(df)
    
    '## Check the profit of each product(Group by, Join)'
    sql_profit=f"select P.product_name, P.producer, sum(O.o_quantity*(O.o_price-P.cost)) as total_profit from Order_history as O, product_producedBy as P WHERE O.pid=P.pid group by P.pid order by total_profit desc"
    df = query_db(sql_profit)
    st.dataframe(df)
    
def main():
    st.sidebar.title("Welcome to Market Datebase")
    password = st.sidebar.text_input("Enter a password", type="password")
    
    if password == "super":
        superoperations()
    elif password == "123":
        view()
    else:
        st.error("please enter your password")
        
        
main()
