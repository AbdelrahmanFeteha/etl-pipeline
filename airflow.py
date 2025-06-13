from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd 
import json

# Database connection details
DATABASE_TYPE = 'mssql+pyodbc'
USERNAME = 'username'
PASSWORD = 'password'
SERVER = 'x.y.z.w'
DATABASE_STAGING = 'TEAM2_STAGING'
DATABASE_FINAL = 'TEAM2_FINAL'
DRIVER = 'ODBC+Driver+17+for+SQL+Server'

# Create the database engines
engine_staging = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE_STAGING}?driver={DRIVER}', fast_executemany=True)
engine_final = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE_FINAL}?driver={DRIVER}', fast_executemany=True)

def extract():
    DATABASE_TYPE = 'mssql+pyodbc'
    USERNAME = 'username'
    PASSWORD = 'password'
    SERVER = 'x.y.z.w'
    DATABASE = 'Source'
    DRIVER = 'ODBC+Driver+17+for+SQL+Server'
    engine_source  = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
    DATABASE = 'TEAM2_STAGING'
    engine_staging = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

    CUSTOMERS_df=pd.read_csv('/root/airflow/dags/CUSTOMERS.csv')
    GEO_LOCATION_df=pd.read_csv('/root/airflow/dags/GEO_LOCATION.csv')
    ORDER_ITEMS_df=pd.read_csv('/root/airflow/dags/ORDER_ITEMS.csv')
    ORDER_PAYMENTS_df=pd.read_csv('/root/airflow/dags/ORDER_PAYMENTS.csv')
    ORDER_REVIEW_RATINGS_df=pd.read_csv('/root/airflow/dags/ORDER_REVIEW_RATINGS.csv')
    ORDERS_df=pd.read_csv('/root/airflow/dags/ORDERS.csv')
    with open ('/root/airflow/dags/PRODUCTS.json','r') as file:
        data=json.load(file)
        PRODUCTS_df = pd.DataFrame(data)
    sellers_df = pd.read_sql_table('sellers',con = engine_source)


    
    #load all dataframes to Staging Database
    PRODUCTS_df = PRODUCTS_df.to_sql('products', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("products_df has been loaded")
    CUSTOMERS_df = CUSTOMERS_df.to_sql('customers', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("customer_df has been loaded")
    ORDER_ITEMS_df = ORDER_ITEMS_df.to_sql('orderItems', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("order_items have been loaded")
    ORDER_PAYMENTS_df = ORDER_PAYMENTS_df.to_sql('orderPayments', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("order_payments has been loaded")
    ORDER_REVIEW_RATINGS_df = ORDER_REVIEW_RATINGS_df.to_sql('orderReviewRatings', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("review ratings has been loaded")
    ORDERS_df = ORDERS_df.to_sql('orders', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("orders has been loaded")
    sellers_df = sellers_df.to_sql('sellers', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("sellers has been loaded")
    GEO_LOCATION_df = GEO_LOCATION_df.to_sql('geoLocation', con = engine_staging,  if_exists = 'replace', index = False, chunksize= 1000)
    print("geo location has been loaded")

def transform_data (engine_staging):
    # Load the tables into DataFrames
    products_df = pd.read_sql_table('products', con=engine_staging)
    geoLocation_df = pd.read_sql_table('geoLocation', con=engine_staging)
    customers_df = pd.read_sql_table('customers', con=engine_staging)
    orderItems_df = pd.read_sql_table('orderItems', con=engine_staging)
    orderPayments_df = pd.read_sql_table('orderPayments', con=engine_staging)
    orderReviewRatings_df = pd.read_sql_table('orderReviewRatings', con=engine_staging)
    orders_df = pd.read_sql_table('orders', con=engine_staging)
    sellers_df = pd.read_sql_table('sellers', con=engine_staging)

    # # Print the head of each DataFrame
    # print(products_df.head())
    # print(customers_df.head())
    # print(orderItems_df.head())
    # print(orderPayments_df.head())
    # print(orderReviewRatings_df.head())
    # print(orders_df.head())
    # print(sellers_df.head())

    # Drop duplicate rows
    products_df = products_df.drop_duplicates(subset=['product_id'], keep='first')
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')
    orderItems_df = orderItems_df.drop_duplicates(subset=['order_id','order_item_id'], keep='first')
    orderPayments_df = orderPayments_df.drop_duplicates()
    orderReviewRatings_df = orderReviewRatings_df.drop_duplicates(subset=['review_id'], keep='first')
    orders_df = orders_df.drop_duplicates(subset=['order_id','customer_id'], keep='first')
    sellers_df = sellers_df.drop_duplicates(subset=['seller_id'], keep='first')
    geoLocation_df = geoLocation_df.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')

    
    def get_columns_with_null(df):
        return df.columns[df.isnull().any()].tolist()

    ### Check for null values in each DataFrame
    # print ("Looking for any null values!")
    # print(get_columns_with_null(products_df))
    # print(get_columns_with_null(customers_df))
    # print(get_columns_with_null(orderItems_df))
    # print(get_columns_with_null(orderPayments_df))
    # print(get_columns_with_null(orderReviewRatings_df))
    # print(get_columns_with_null(orders_df))
    # print(get_columns_with_null(sellers_df))

    ### Null values exist in products_df and sellers_df
    products_df['product_category_name'] = products_df['product_category_name'].fillna('N/A')
    products_df['product_weight_g'] = products_df['product_weight_g'].fillna(0)

    ## Null values for seller_state --> seller city (string) and seller state (string)
    sellers_df['seller_city'] = sellers_df['seller_city'].fillna('N/A')
    sellers_df['seller_state'] = sellers_df['seller_state'].fillna('N/A')

    ### Check for null values in each DataFrame, result: NO NULL VALUES
    # print("Looking for any null values!")
    # print("Products:", get_columns_with_null(products_df))
    # print("Customers:", get_columns_with_null(customers_df))
    # print("Order Items:", get_columns_with_null(orderItems_df))
    # print("Order Payments:", get_columns_with_null(orderPayments_df))
    # print("Order Review Ratings:", get_columns_with_null(orderReviewRatings_df))
    # print("Orders:", get_columns_with_null(orders_df))
    # print("Sellers:", get_columns_with_null(sellers_df))

        # Function to map hashed IDs to integers
    def create_id_mapping(df: pd.DataFrame, column_name: str) -> dict:
        unique_ids = df[column_name].unique()
        id_mapping = {original_id: new_id for new_id, original_id in enumerate(unique_ids)}
        return id_mapping

    # Function to replace hashed IDs with integer IDs
    def replace_hashed_ids(df: pd.DataFrame, column_name: str, id_mapping: dict) -> pd.DataFrame:
        df[column_name] = df[column_name].map(id_mapping)
        return df

    # Create mappings for each entity's ID
    customer_id_mapping = create_id_mapping(customers_df, 'customer_id')
    order_id_mapping = create_id_mapping(orders_df, 'order_id')
    product_id_mapping = create_id_mapping(products_df, 'product_id')
    seller_id_mapping = create_id_mapping(sellers_df, 'seller_id')
    orderReviewRatings_mapping = create_id_mapping(orderReviewRatings_df, 'review_id')

    # Replace hashed IDs with integer IDs in all relevant dataframes
    customers_df = replace_hashed_ids(customers_df, 'customer_id', customer_id_mapping)
    orders_df = replace_hashed_ids(orders_df, 'customer_id', customer_id_mapping)
    orders_df = replace_hashed_ids(orders_df, 'order_id', order_id_mapping)
    orderItems_df = replace_hashed_ids(orderItems_df, 'order_id', order_id_mapping)
    orderItems_df = replace_hashed_ids(orderItems_df, 'product_id', product_id_mapping)
    orderItems_df = replace_hashed_ids(orderItems_df, 'seller_id', seller_id_mapping)
    orderPayments_df = replace_hashed_ids(orderPayments_df, 'order_id', order_id_mapping)
    orderReviewRatings_df = replace_hashed_ids(orderReviewRatings_df, 'order_id', order_id_mapping)
    products_df =  replace_hashed_ids(products_df, 'product_id', product_id_mapping)
    sellers_df = replace_hashed_ids(sellers_df, 'seller_id', seller_id_mapping)
    orderReviewRatings_df = replace_hashed_ids(orderReviewRatings_df, 'review_id', orderReviewRatings_mapping)

    
    ###Ensure datatypes
    def get_unique_values_in_order(df, column_name):
        unique_values = df[column_name].unique()
        unique_values_list = unique_values.tolist()
        unique_values_list.sort()
        return unique_values_list

    def clean_column(column):
        if column.dtype =='object':
            # First, attempt to convert the column to datetime
            # Define the expected date format
            date_format = '%m/%d/%Y %H:%M'
            # Attempt to convert to datetime with the specified format
            converted_column = pd.to_datetime(column, format=date_format, errors='coerce')
            
            # Check if conversion resulted in a significant number of NaT values
            if converted_column.isna().sum() > len(column) / 2:
                # If the column is not likely a date, perform string cleaning
                column = column.str.lower()
                # Remove leading, trailing, and double spaces, then convert text to title case
                column = column.apply(lambda x: ' '.join(x.split()).title() if isinstance(x, str) else x)
            else:
                # If the column is likely a date, use the converted datetime column
                column = converted_column
        return column

    def clean_data(df):
        # Apply cleaning to each column in the DataFrame
        return df.apply(clean_column, axis=0)   #pandas series of columns

    customers_df = clean_data(customers_df)
    geoLocation_df = clean_data(geoLocation_df)
    orderItems_df = clean_data(orderItems_df)
    orderPayments_df = clean_data(orderPayments_df)
    orderReviewRatings_df = clean_data(orderReviewRatings_df)
    orders_df = clean_data(orders_df)
    products_df= clean_data(products_df)
    sellers_df=clean_data(sellers_df)
    
    customers_df=customers_df.dropna()
    geoLocation_df=geoLocation_df.dropna()
    orderItems_df=orderItems_df.dropna()
    orderPayments_df=orderPayments_df.dropna()
    orderReviewRatings_df=orderReviewRatings_df.dropna()
    orders_df=orders_df.dropna()
    products_df=products_df.dropna()
    sellers_df=sellers_df.dropna()

    # Drop duplicate rows
    products_df = products_df.drop_duplicates(subset=['product_id'], keep='first')
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')
    orderItems_df = orderItems_df.drop_duplicates(subset=['order_id','order_item_id'], keep='first')
    orderPayments_df = orderPayments_df.drop_duplicates()
    orderReviewRatings_df = orderReviewRatings_df.drop_duplicates(subset=['review_id'], keep='first')
    orders_df = orders_df.drop_duplicates(subset=['order_id','customer_id'], keep='first')
    sellers_df = sellers_df.drop_duplicates(subset=['seller_id'], keep='first')
    geoLocation_df = geoLocation_df.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')


    # print(customers_df)
    # print(geoLocation_df)
    # print(orderItems_df)
    # print(orderPayments_df)
    # print(orderReviewRatings_df)
    # print(orders_df)
    # print(products_df)
    # print(sellers_df)


        # Store transformed DataFrames in a dictionary
    transformed_data = {
        'customers_df': customers_df,
        'geoLocation_df': geoLocation_df,
        'orderItems_df': orderItems_df,
        'orderPayments_df': orderPayments_df,
        'orderReviewRatings_df': orderReviewRatings_df,
        'orders_df': orders_df,
        'products_df': products_df,
        'sellers_df': sellers_df
    }

    return transformed_data






    #return customers_df, geoLocation_df, orderItems_df, orderPayments_df, orderReviewRatings_df, orders_df, products_df,sellers_df


    
    # print("\nGeoLocation DataFrame:")
    # print(geoLocation_df.head())
    # print("Data types for order reviews:")
    # print(orderReviewRatings_df.dtypes)


def get_insertion_updates(staging_df, final_df, key_columns):
    # Merge staging and final dataframes to find new and updated records
    merged_df = staging_df.merge(final_df, on=key_columns, how='left', suffixes=('', '_final'), indicator=True)
    # Select new and updated records
    insertions = merged_df[merged_df['_merge'] == 'left_only']
    updates = merged_df[merged_df['_merge'] == 'both']
    # Drop auxiliary columns
    insertions = insertions.drop(columns=[col for col in insertions.columns if col.endswith('_final') or col == '_merge'])
    updates = updates.drop(columns=[col for col in updates.columns if not col.endswith('_final') and col != '_merge'])
    return insertions, updates

# Delete updated records from final database
def delete_updated_records(engine, updates_df, table_name, key_columns):
    keys = updates_df[key_columns].to_dict(orient='records')
    conn = engine.connect()
    for key in keys:
        delete_query = f"DELETE FROM {table_name} WHERE " + ' AND '.join([f"{col} = '{key[col]}'" for col in key_columns])
        conn.execute(delete_query)
    conn.close()




# Insert new and updated records into final database
def upsert_data(df, table_name):
    df.to_sql(table_name, con=engine_final, if_exists='append', index=False)
    print("Finished:", table_name)

# Assuming 'order_index' is the identity column
def upsert_data2(df, table_name, identity_column=None):
    if identity_column:
        df = df.drop(columns=[identity_column])
    df.to_sql(table_name, con=engine_final, if_exists='append', index=False)
    print("Finished:", table_name)

def load(engine_final,transformed_data):
    customers_df = transformed_data['customers_df']
    geoLocation_df = transformed_data['geoLocation_df']
    orderItems_df = transformed_data['orderItems_df']
    orderPayments_df = transformed_data['orderPayments_df']
    orderReviewRatings_df = transformed_data['orderReviewRatings_df']
    orders_df = transformed_data['orders_df']
    products_df = transformed_data['products_df']
    sellers_df = transformed_data['sellers_df']
    # Create a session
    Session = sessionmaker(bind=engine_final)
    session = Session()

    # # Load new or updated records into DataFrames
    # products_df = pd.read_sql_table('products', con=engine_staging)
    # geoLocation_df = pd.read_sql_table('geoLocation', con=engine_staging)
    # customers_df = pd.read_sql_table('customers', con=engine_staging)
    # orderItems_df = pd.read_sql_table('orderItems', con=engine_staging)
    # orderPayments_df = pd.read_sql_table('orderPayments', con=engine_staging)
    # orderReviewRatings_df = pd.read_sql_table('orderReviewRatings', con=engine_staging)
    # orders_df = pd.read_sql_table('orders', con=engine_staging)
    # sellers_df = pd.read_sql_table('sellers', con=engine_staging)

    
    customers_df = customers_df.drop(columns=['customer_unique_id'])



    # Load final database tables into DataFrames
    final_products_df = pd.read_sql_table('products', con=engine_final)
    final_geoLocation_df = pd.read_sql_table('geoLocations', con=engine_final)
    final_customers_df = pd.read_sql_table('customers', con=engine_final)
    final_orderItems_df = pd.read_sql_table('orderItems', con=engine_final)
    final_orderPayments_df = pd.read_sql_table('orderPayments', con=engine_final)
    final_orderReviewRatings_df = pd.read_sql_table('orderReviews', con=engine_final)
    final_orders_df = pd.read_sql_table('orders', con=engine_final)
    final_sellers_df = pd.read_sql_table('sellers', con=engine_final)

    # Get new and updated records
    insertions_customers, updates_customers = get_insertion_updates(customers_df, final_customers_df, key_columns=['customer_id'])
    insertions_geoLocation, updates_geoLocation = get_insertion_updates(geoLocation_df, final_geoLocation_df, key_columns=['geolocation_zip_code_prefix'])
    insertions_products, updates_products = get_insertion_updates(products_df, final_products_df, key_columns=['product_id'])
    insertions_orderItems, updates_orderItems = get_insertion_updates(orderItems_df, final_orderItems_df, key_columns=['order_id', 'order_item_id'])
    insertions_orderPayments, updates_orderPayments = get_insertion_updates(orderPayments_df, final_orderPayments_df, key_columns=['order_id'])
    insertions_orderReviewRatings, updates_orderReviewRatings = get_insertion_updates(orderReviewRatings_df, final_orderReviewRatings_df, key_columns=['review_id'])
    insertions_orders, updates_orders = get_insertion_updates(orders_df, final_orders_df, key_columns=['order_id'])
    insertions_sellers, updates_sellers = get_insertion_updates(sellers_df, final_sellers_df, key_columns=['seller_id'])

    


    upsert_data(insertions_products, 'products')
    upsert_data(insertions_customers, 'customers')
    upsert_data(insertions_orders, 'orders')
    upsert_data(insertions_sellers, 'sellers')
    upsert_data2(insertions_orderItems, 'orderItems', identity_column='order_index')
    upsert_data2(insertions_orderPayments, 'orderPayments', identity_column='payment_id')
    upsert_data(insertions_orderReviewRatings, 'orderReviews')
    upsert_data(insertions_geoLocation, 'geoLocations')

    # Commit the session if any changes are made to the database
    session.commit()

    print("Incremental load completed successfully.")


default_args = {
    'owner': "TheGroup",
    'start_date': datetime(2024, 8, 7),
    'retries': 1,
}
dag = DAG(
    dag_id='Etl_Pipline_dag',
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False
)   


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag
)

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd 
import json

# Database connection details
DATABASE_TYPE = 'mssql+pyodbc'
USERNAME = 'Team2'
PASSWORD = 'NOSAA'
SERVER = '192.168.22.40'
DATABASE_STAGING = 'TEAM2_STAGING'
DATABASE_FINAL = 'TEAM2_FINAL'
DRIVER = 'ODBC+Driver+17+for+SQL+Server'

# Create the database engines
engine_staging = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE_STAGING}?driver={DRIVER}', fast_executemany=True)
engine_final = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE_FINAL}?driver={DRIVER}', fast_executemany=True)

def extract():
    DATABASE_TYPE = 'mssql+pyodbc'
    USERNAME = 'Team2'
    PASSWORD = 'NOSAA'
    SERVER = '192.168.22.40'
    DATABASE = 'Source'
    DRIVER = 'ODBC+Driver+17+for+SQL+Server'
    engine_source  = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}',fast_executemany=True)
    DATABASE = 'TEAM2_STAGING'
    engine_staging = create_engine(f'{DATABASE_TYPE}://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}',fast_executemany=True)

    CUSTOMERS_df=pd.read_csv('/root/airflow/dags/CUSTOMERS.csv')
    GEO_LOCATION_df=pd.read_csv('/root/airflow/dags/GEO_LOCATION.csv')
    ORDER_ITEMS_df=pd.read_csv('/root/airflow/dags/ORDER_ITEMS.csv')
    ORDER_PAYMENTS_df=pd.read_csv('/root/airflow/dags/ORDER_PAYMENTS.csv')
    ORDER_REVIEW_RATINGS_df=pd.read_csv('/root/airflow/dags/ORDER_REVIEW_RATINGS.csv')
    ORDERS_df=pd.read_csv('/root/airflow/dags/ORDERS.csv')
    with open ('/root/airflow/dags/PRODUCTS.json','r') as file:
        data=json.load(file)
        PRODUCTS_df = pd.DataFrame(data)
    sellers_df = pd.read_sql_table('sellers',con = engine_source)


    
    #load all dataframes to Staging Database
    PRODUCTS_df = PRODUCTS_df.to_sql('products', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("products_df has been loaded")
    CUSTOMERS_df = CUSTOMERS_df.to_sql('customers', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("customer_df has been loaded")
    ORDER_ITEMS_df = ORDER_ITEMS_df.to_sql('orderItems', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("order_items have been loaded")
    ORDER_PAYMENTS_df = ORDER_PAYMENTS_df.to_sql('orderPayments', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("order_payments has been loaded")
    ORDER_REVIEW_RATINGS_df = ORDER_REVIEW_RATINGS_df.to_sql('orderReviewRatings', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("review ratings has been loaded")
    ORDERS_df = ORDERS_df.to_sql('orders', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("orders has been loaded")
    sellers_df = sellers_df.to_sql('sellers', con = engine_staging, if_exists = 'replace', index = False, chunksize= 1000)
    print("sellers has been loaded")
    GEO_LOCATION_df = GEO_LOCATION_df.to_sql('geoLocation', con = engine_staging,  if_exists = 'replace', index = False, chunksize= 1000)
    print("geo location has been loaded")

def transform_data (engine_staging):
    # Load the tables into DataFrames
    products_df = pd.read_sql_table('products', con=engine_staging)
    geoLocation_df = pd.read_sql_table('geoLocation', con=engine_staging)
    customers_df = pd.read_sql_table('customers', con=engine_staging)
    orderItems_df = pd.read_sql_table('orderItems', con=engine_staging)
    orderPayments_df = pd.read_sql_table('orderPayments', con=engine_staging)
    orderReviewRatings_df = pd.read_sql_table('orderReviewRatings', con=engine_staging)
    orders_df = pd.read_sql_table('orders', con=engine_staging)
    sellers_df = pd.read_sql_table('sellers', con=engine_staging)

    # # Print the head of each DataFrame
    # print(products_df.head())
    # print(customers_df.head())
    # print(orderItems_df.head())
    # print(orderPayments_df.head())
    # print(orderReviewRatings_df.head())
    # print(orders_df.head())
    # print(sellers_df.head())

    # Drop duplicate rows
    products_df = products_df.drop_duplicates(subset=['product_id'], keep='first')
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')
    orderItems_df = orderItems_df.drop_duplicates(subset=['order_id','order_item_id'], keep='first')
    orderPayments_df = orderPayments_df.drop_duplicates()
    orderReviewRatings_df = orderReviewRatings_df.drop_duplicates(subset=['review_id'], keep='first')
    orders_df = orders_df.drop_duplicates(subset=['order_id','customer_id'], keep='first')
    sellers_df = sellers_df.drop_duplicates(subset=['seller_id'], keep='first')
    geoLocation_df = geoLocation_df.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')

    
    def get_columns_with_null(df):
        return df.columns[df.isnull().any()].tolist()

    ### Check for null values in each DataFrame
    # print ("Looking for any null values!")
    # print(get_columns_with_null(products_df))
    # print(get_columns_with_null(customers_df))
    # print(get_columns_with_null(orderItems_df))
    # print(get_columns_with_null(orderPayments_df))
    # print(get_columns_with_null(orderReviewRatings_df))
    # print(get_columns_with_null(orders_df))
    # print(get_columns_with_null(sellers_df))

    ### Null values exist in products_df and sellers_df
    products_df['product_category_name'] = products_df['product_category_name'].fillna('N/A')
    products_df['product_weight_g'] = products_df['product_weight_g'].fillna(0)

    ## Null values for seller_state --> seller city (string) and seller state (string)
    sellers_df['seller_city'] = sellers_df['seller_city'].fillna('N/A')
    sellers_df['seller_state'] = sellers_df['seller_state'].fillna('N/A')

    ### Check for null values in each DataFrame, result: NO NULL VALUES
    # print("Looking for any null values!")
    # print("Products:", get_columns_with_null(products_df))
    # print("Customers:", get_columns_with_null(customers_df))
    # print("Order Items:", get_columns_with_null(orderItems_df))
    # print("Order Payments:", get_columns_with_null(orderPayments_df))
    # print("Order Review Ratings:", get_columns_with_null(orderReviewRatings_df))
    # print("Orders:", get_columns_with_null(orders_df))
    # print("Sellers:", get_columns_with_null(sellers_df))

        # Function to map hashed IDs to integers
    def create_id_mapping(df: pd.DataFrame, column_name: str) -> dict:
        unique_ids = df[column_name].unique()
        id_mapping = {original_id: new_id for new_id, original_id in enumerate(unique_ids)}
        return id_mapping

    # Function to replace hashed IDs with integer IDs
    def replace_hashed_ids(df: pd.DataFrame, column_name: str, id_mapping: dict) -> pd.DataFrame:
        df[column_name] = df[column_name].map(id_mapping)
        return df

    # Create mappings for each entity's ID
    customer_id_mapping = create_id_mapping(customers_df, 'customer_id')
    order_id_mapping = create_id_mapping(orders_df, 'order_id')
    product_id_mapping = create_id_mapping(products_df, 'product_id')
    seller_id_mapping = create_id_mapping(sellers_df, 'seller_id')
    orderReviewRatings_mapping = create_id_mapping(orderReviewRatings_df, 'review_id')

    # Replace hashed IDs with integer IDs in all relevant dataframes
    customers_df = replace_hashed_ids(customers_df, 'customer_id', customer_id_mapping)
    orders_df = replace_hashed_ids(orders_df, 'customer_id', customer_id_mapping)
    orders_df = replace_hashed_ids(orders_df, 'order_id', order_id_mapping)
    orderItems_df = replace_hashed_ids(orderItems_df, 'order_id', order_id_mapping)
    orderItems_df = replace_hashed_ids(orderItems_df, 'product_id', product_id_mapping)
    orderItems_df = replace_hashed_ids(orderItems_df, 'seller_id', seller_id_mapping)
    orderPayments_df = replace_hashed_ids(orderPayments_df, 'order_id', order_id_mapping)
    orderReviewRatings_df = replace_hashed_ids(orderReviewRatings_df, 'order_id', order_id_mapping)
    products_df =  replace_hashed_ids(products_df, 'product_id', product_id_mapping)
    sellers_df = replace_hashed_ids(sellers_df, 'seller_id', seller_id_mapping)
    orderReviewRatings_df = replace_hashed_ids(orderReviewRatings_df, 'review_id', orderReviewRatings_mapping)

    
    ###Ensure datatypes
    def get_unique_values_in_order(df, column_name):
        unique_values = df[column_name].unique()
        unique_values_list = unique_values.tolist()
        unique_values_list.sort()
        return unique_values_list

    def clean_column(column):
        if column.dtype =='object':
            # First, attempt to convert the column to datetime
            # Define the expected date format
            date_format = '%m/%d/%Y %H:%M'
            # Attempt to convert to datetime with the specified format
            converted_column = pd.to_datetime(column, format=date_format, errors='coerce')
            
            # Check if conversion resulted in a significant number of NaT values
            if converted_column.isna().sum() > len(column) / 2:
                # If the column is not likely a date, perform string cleaning
                column = column.str.lower()
                # Remove leading, trailing, and double spaces, then convert text to title case
                column = column.apply(lambda x: ' '.join(x.split()).title() if isinstance(x, str) else x)
            else:
                # If the column is likely a date, use the converted datetime column
                column = converted_column
        return column

    def clean_data(df):
        # Apply cleaning to each column in the DataFrame
        return df.apply(clean_column, axis=0)   #pandas series of columns

    customers_df = clean_data(customers_df)
    geoLocation_df = clean_data(geoLocation_df)
    orderItems_df = clean_data(orderItems_df)
    orderPayments_df = clean_data(orderPayments_df)
    orderReviewRatings_df = clean_data(orderReviewRatings_df)
    orders_df = clean_data(orders_df)
    products_df= clean_data(products_df)
    sellers_df=clean_data(sellers_df)
    
    customers_df=customers_df.dropna()
    geoLocation_df=geoLocation_df.dropna()
    orderItems_df=orderItems_df.dropna()
    orderPayments_df=orderPayments_df.dropna()
    orderReviewRatings_df=orderReviewRatings_df.dropna()
    orders_df=orders_df.dropna()
    products_df=products_df.dropna()
    sellers_df=sellers_df.dropna()

    # Drop duplicate rows
    products_df = products_df.drop_duplicates(subset=['product_id'], keep='first')
    customers_df = customers_df.drop_duplicates(subset=['customer_id'], keep='first')
    orderItems_df = orderItems_df.drop_duplicates(subset=['order_id','order_item_id'], keep='first')
    orderPayments_df = orderPayments_df.drop_duplicates()
    orderReviewRatings_df = orderReviewRatings_df.drop_duplicates(subset=['review_id'], keep='first')
    orders_df = orders_df.drop_duplicates(subset=['order_id','customer_id'], keep='first')
    sellers_df = sellers_df.drop_duplicates(subset=['seller_id'], keep='first')
    geoLocation_df = geoLocation_df.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')


    # print(customers_df)
    # print(geoLocation_df)
    # print(orderItems_df)
    # print(orderPayments_df)
    # print(orderReviewRatings_df)
    # print(orders_df)
    # print(products_df)
    # print(sellers_df)


        # Store transformed DataFrames in a dictionary
    transformed_data = {
        'customers_df': customers_df,
        'geoLocation_df': geoLocation_df,
        'orderItems_df': orderItems_df,
        'orderPayments_df': orderPayments_df,
        'orderReviewRatings_df': orderReviewRatings_df,
        'orders_df': orders_df,
        'products_df': products_df,
        'sellers_df': sellers_df
    }

    return transformed_data






    #return customers_df, geoLocation_df, orderItems_df, orderPayments_df, orderReviewRatings_df, orders_df, products_df,sellers_df


    
    # print("\nGeoLocation DataFrame:")
    # print(geoLocation_df.head())
    # print("Data types for order reviews:")
    # print(orderReviewRatings_df.dtypes)


def get_insertion_updates(staging_df, final_df, key_columns):
    # Merge staging and final dataframes to find new and updated records
    merged_df = staging_df.merge(final_df, on=key_columns, how='left', suffixes=('', '_final'), indicator=True)
    # Select new and updated records
    insertions = merged_df[merged_df['_merge'] == 'left_only']
    updates = merged_df[merged_df['_merge'] == 'both']
    # Drop auxiliary columns
    insertions = insertions.drop(columns=[col for col in insertions.columns if col.endswith('_final') or col == '_merge'])
    updates = updates.drop(columns=[col for col in updates.columns if not col.endswith('_final') and col != '_merge'])
    return insertions, updates

# Delete updated records from final database
def delete_updated_records(engine, updates_df, table_name, key_columns):
    keys = updates_df[key_columns].to_dict(orient='records')
    conn = engine.connect()
    for key in keys:
        delete_query = f"DELETE FROM {table_name} WHERE " + ' AND '.join([f"{col} = '{key[col]}'" for col in key_columns])
        conn.execute(delete_query)
    conn.close()




# Insert new and updated records into final database
def upsert_data(df, table_name):
    df.to_sql(table_name, con=engine_final, if_exists='append', index=False)
    print("Finished:", table_name)

# Assuming 'order_index' is the identity column
def upsert_data2(df, table_name, identity_column=None):
    if identity_column:
        df = df.drop(columns=[identity_column])
    df.to_sql(table_name, con=engine_final, if_exists='append', index=False)
    print("Finished:", table_name)

def load(engine_final,transformed_data):
    customers_df = transformed_data['customers_df']
    geoLocation_df = transformed_data['geoLocation_df']
    orderItems_df = transformed_data['orderItems_df']
    orderPayments_df = transformed_data['orderPayments_df']
    orderReviewRatings_df = transformed_data['orderReviewRatings_df']
    orders_df = transformed_data['orders_df']
    products_df = transformed_data['products_df']
    sellers_df = transformed_data['sellers_df']
    # Create a session
    Session = sessionmaker(bind=engine_final)
    session = Session()

    # # Load new or updated records into DataFrames
    # products_df = pd.read_sql_table('products', con=engine_staging)
    # geoLocation_df = pd.read_sql_table('geoLocation', con=engine_staging)
    # customers_df = pd.read_sql_table('customers', con=engine_staging)
    # orderItems_df = pd.read_sql_table('orderItems', con=engine_staging)
    # orderPayments_df = pd.read_sql_table('orderPayments', con=engine_staging)
    # orderReviewRatings_df = pd.read_sql_table('orderReviewRatings', con=engine_staging)
    # orders_df = pd.read_sql_table('orders', con=engine_staging)
    # sellers_df = pd.read_sql_table('sellers', con=engine_staging)

    
    customers_df = customers_df.drop(columns=['customer_unique_id'])



    # Load final database tables into DataFrames
    final_products_df = pd.read_sql_table('products', con=engine_final)
    final_geoLocation_df = pd.read_sql_table('geoLocations', con=engine_final)
    final_customers_df = pd.read_sql_table('customers', con=engine_final)
    final_orderItems_df = pd.read_sql_table('orderItems', con=engine_final)
    final_orderPayments_df = pd.read_sql_table('orderPayments', con=engine_final)
    final_orderReviewRatings_df = pd.read_sql_table('orderReviews', con=engine_final)
    final_orders_df = pd.read_sql_table('orders', con=engine_final)
    final_sellers_df = pd.read_sql_table('sellers', con=engine_final)

    # Get new and updated records
    insertions_customers, updates_customers = get_insertion_updates(customers_df, final_customers_df, key_columns=['customer_id'])
    insertions_geoLocation, updates_geoLocation = get_insertion_updates(geoLocation_df, final_geoLocation_df, key_columns=['geolocation_zip_code_prefix'])
    insertions_products, updates_products = get_insertion_updates(products_df, final_products_df, key_columns=['product_id'])
    insertions_orderItems, updates_orderItems = get_insertion_updates(orderItems_df, final_orderItems_df, key_columns=['order_id', 'order_item_id'])
    insertions_orderPayments, updates_orderPayments = get_insertion_updates(orderPayments_df, final_orderPayments_df, key_columns=['order_id'])
    insertions_orderReviewRatings, updates_orderReviewRatings = get_insertion_updates(orderReviewRatings_df, final_orderReviewRatings_df, key_columns=['review_id'])
    insertions_orders, updates_orders = get_insertion_updates(orders_df, final_orders_df, key_columns=['order_id'])
    insertions_sellers, updates_sellers = get_insertion_updates(sellers_df, final_sellers_df, key_columns=['seller_id'])

    


    upsert_data(insertions_products, 'products')
    upsert_data(insertions_customers, 'customers')
    upsert_data(insertions_orders, 'orders')
    upsert_data(insertions_sellers, 'sellers')
    upsert_data2(insertions_orderItems, 'orderItems', identity_column='order_index')
    upsert_data2(insertions_orderPayments, 'orderPayments', identity_column='payment_id')
    upsert_data(insertions_orderReviewRatings, 'orderReviews')
    upsert_data(insertions_geoLocation, 'geoLocations')

    # Commit the session if any changes are made to the database
    session.commit()

    print("Incremental load completed successfully.")


default_args = {
    'owner': "TheGroup",
    'start_date': datetime(2024, 8, 7),
    'retries': 1,
}
dag = DAG(
    dag_id='Etl_Pipline_dag',
    default_args=default_args, 
    schedule_interval='@daily',
    catchup=False
)   


extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    op_args=[engine_staging],
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    op_args=[engine_final, 'transformed_data' ], 
    dag=dag
)

extract_task >> transform_task >> load_task










# transform_task = PythonOperator(
#     task_id='transform_task',
#     python_callable=transform_data,
#     op_args=[engine_staging],
#     dag=dag
# )

# load_task = PythonOperator(
#     task_id='load_task',
#     python_callable=load,
#     op_args=[engine_final, 'transformed_data' ], 
#     dag=dag
# )

extract_task >> transform_task >> load_task
