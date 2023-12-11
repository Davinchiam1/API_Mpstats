import datetime
from datetime import datetime
import sys
from API_Mpstats import requ_Mpstats
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, insert, select, exists, inspect,delete
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DATETIME, Date, MetaData, Table, text, DDL, BigInteger
import pandas as pd
import sqlalchemy
import numpy as np

# creating a database connection from a file
with open('database.txt', 'r') as file:
    conn_string = file.readline().strip()

engine = create_engine(conn_string)
Session = sessionmaker(bind=engine)
metadata = MetaData()


# session = Session()


def create_table(table_name, frame):
    """
    Function for creating a Table from dataframe or establishment of dependency with existing table from db.

    Args:
        table_name (string): name of table.
        frame (pd.Dataframe): dataframe sample for table.

    Returns:
        Table: Table object with name table_name.
    """
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        table = Table(table_name, metadata, autoload_with=engine)
        db_columns = table.columns.keys()
        frame_columns = frame.columns
        new_columns = set(frame_columns) - set(db_columns)
    else:
        table = Table(table_name, metadata)
        columns = create_colums(frame)
        for name, data_type in columns.items():
            if name == 'id':
                table.append_column(Column(name, data_type, primary_key=True,autoincrement=True))
            else:
                table.append_column(Column(name, data_type))
        metadata.create_all(engine)
    return table


def create_colums(frame=pd.DataFrame()):
    """
    Function for transformations dataframe columns into dict with column_names: data types structure.

    Args:
        frame (pd.Dataframe): dataframe sample for table.

    Returns:
        columns_dict (dict): dict of column_names and data types.
    """
    columns_dict = {'id': BigInteger}
    for col, dtype in frame.dtypes.items():
        if dtype == 'int64':
            columns_dict[col] = Integer
        elif dtype == 'float64':
            columns_dict[col] = Float
        elif str(dtype).startswith('object'):
            columns_dict[col] = String
        else:
            columns_dict[col] = String
    return columns_dict


def update_brands(table_name='brand_name',brand_string='', startdate='2021-03-01', enddate='2021-03-01',wb_oz_var=None):
    """
    Function for updating a table of the "brand_name" type with data from the api service from start to end date.

    Args:
        table_name (string): name of table in db.
        brand_string (string): link for api request.
        startdate (string): start date.
        enddate (string): end date.

    Returns:
        None

    """

    if wb_oz_var is None:
        wb_oz_var='wb'
    requ = requ_Mpstats(request=wb_oz_var)

    # transform strings into datetime
    date1 = datetime.strptime(startdate, "%Y-%m-%d")
    date2 = datetime.strptime(enddate, "%Y-%m-%d")

    # calculate differense between dates
    date_difference = date2 - date1
    if date_difference==6:
        data = requ.brand_request(d1=startdate, d2=enddate, brand_string=brand_string,save=False, db_conn=True)
    else:
        data = requ.get_brand_by_dates(start_date=startdate, end_date=enddate, brand_string=brand_string, db_connect=True)
    brands = create_table(table_name, data)
    Base = sqlalchemy.orm.declarative_base()
    errors_list = []

    class Brands(Base):
        # base class for sqlalchemy to secure transactions
        __tablename__ = table_name
        __table__ = brands

    session = Session()

    with engine.connect() as connection:
        # prepare data for uploading
        # data=data.drop('items.serial_num',axis=1)
        data1 = data.to_dict('records')

        # update or upload data in tabl.,e
        for row in data1:
            try:
                brands = Brands(**row)
                session.merge(brands)
            except Exception as e:
                errors_list.append(row['id'])
                print(e)
                continue
    session.commit()
    session.close()



# update_brands(table_name='brand_name',brand_string='WiMi', startdate='2023-08-01', enddate='2023-08-20')
