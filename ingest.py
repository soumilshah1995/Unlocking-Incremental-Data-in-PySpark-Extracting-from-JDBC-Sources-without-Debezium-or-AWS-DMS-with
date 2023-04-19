try:
    import os
    import logging

    from functools import wraps
    from abc import ABC, abstractmethod
    from enum import Enum
    from logging import StreamHandler

    import uuid
    from datetime import datetime, timezone
    from random import randint
    import datetime

    import sqlalchemy as db
    from faker import Faker
    import random
    import psycopg2
    import psycopg2.extras as extras
    from dotenv import load_dotenv

    load_dotenv(".env")
except Exception as e:
    raise Exception("Error: {} ".format(e))


def error_handling_with_logging(argument=None):
    def real_decorator(function):
        @wraps(function)
        def wrapper(self, *args, **kwargs):
            function_name = function.__name__
            response = None
            try:
                if kwargs == {}:
                    response = function(self)
                else:
                    response = function(self, **kwargs)
            except Exception as e:
                response = {
                    "status": -1,
                    "error": {"message": str(e), "function_name": function.__name__},
                }
                print(response)
            return response

        return wrapper

    return real_decorator


class DatabaseInterface(ABC):
    @abstractmethod
    def get_data(self, query):
        """
        For given query fetch the data
        :param query: Str
        :return: Dict
        """

    def execute_non_query(self, query):
        """
        Inserts data into SQL Server
        :param query:  Str
        :return: Dict
        """

    def insert_many(self, query, data):
        """
        Insert Many items into database
        :param query: str
        :param data: tuple
        :return: Dict
        """

    def get_data_batch(self, batch_size=10, query=""):
        """
        Gets data into batches
        :param batch_size: INT
        :param query: STR
        :return: DICT
        """

    def get_table(self, table_name=""):
        """
        Gets the table from database
        :param table_name: STR
        :return: OBJECT
        """


class Settings(object):
    """settings class"""

    def __init__(
            self,
            port="",
            server="",
            username="",
            password="",
            timeout=100,
            database_name="",
            connection_string="",
            collection_name="",
            **kwargs,
    ):
        self.port = port
        self.server = server
        self.username = username
        self.password = password
        self.timeout = timeout
        self.database_name = database_name
        self.connection_string = connection_string


class DatabaseAurora(DatabaseInterface):
    """Aurora database class"""

    def __init__(self, data_base_settings):
        self.data_base_settings = data_base_settings
        self.client = db.create_engine(
            "postgresql://{username}:{password}@{server}:{port}/{database}".format(
                username=self.data_base_settings.username,
                password=self.data_base_settings.password,
                server=self.data_base_settings.server,
                port=self.data_base_settings.port,
                database=self.data_base_settings.database_name
            )
        )
        self.metadata = db.MetaData()


    @error_handling_with_logging()
    def get_data(self, query):
        self.query = query
        cursor = self.client.connect()
        response = cursor.execute(self.query)
        result = response.fetchall()
        columns = response.keys()._keys
        data = [dict(zip(columns, item)) for item in result]
        cursor.close()
        return {"statusCode": 200, "data": data}

    @error_handling_with_logging()
    def execute_non_query(self, query):
        self.query = query
        cursor = self.client.connect()
        cursor.execute(self.query)
        cursor.close()
        return {"statusCode": 200, "data": True}

    @error_handling_with_logging()
    def insert_many(self, query, data):
        self.query = query
        print(data)
        cursor = self.client.connect()
        cursor.execute(self.query, data)
        cursor.close()
        return {"statusCode": 200, "data": True}

    @error_handling_with_logging()
    def get_data_batch(self, batch_size=10, query=""):
        self.query = query
        cursor = self.client.connect()
        response = cursor.execute(self.query)
        columns = response.keys()._keys
        while True:
            result = response.fetchmany(batch_size)
            if not result:
                break
            else:
                items = [dict(zip(columns, data)) for data in result]
                yield items

    @error_handling_with_logging()
    def get_table(self, table_name=""):
        table = db.Table(table_name, self.metadata,
                         autoload=True,
                         autoload_with=self.client)

        return {"statusCode": 200, "table": table}


class Connector(Enum):
    ON_AURORA_PYCOPG = DatabaseAurora(
        data_base_settings=Settings(
            port="5432",
            server="localhost",
            username="postgres",
            password="postgres",
            database_name="postgres",
        )
    )


def main():
    helper = Connector.ON_AURORA_PYCOPG.value
    import time

    states = ("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
              "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
              "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
              "WA", "WV", "WI", "WY")
    shipping_types = ("Free", "3-Day", "2-Day")

    product_categories = ("Garden", "Kitchen", "Office", "Household")
    referrals = ("Other", "Friend/Colleague", "Repeat Customer", "Online Ad")

    try:
        query = """

CREATE TABLE IF NOT EXISTS public.sales
(
    salesid SERIAL PRIMARY KEY,
    invoiceid integer,
    itemid integer,
    category text COLLATE pg_catalog."default",
    price numeric(10,2),
    quantity integer,
    orderdate date,
    destinationstate text COLLATE pg_catalog."default",
    shippingtype text COLLATE pg_catalog."default",
    referral text COLLATE pg_catalog."default",
    updated_at TIMESTAMP DEFAULT NOW()
);
        """
        helper.execute_non_query(query=query)
        time.sleep(2)
    except Exception as e:
        print("Error", e)
    try:
        query = """
CREATE OR REPLACE FUNCTION update_sales_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_sales_updated_at_trigger
BEFORE UPDATE ON public.sales
FOR EACH ROW
EXECUTE FUNCTION update_sales_updated_at();
        """
        helper.execute_non_query(query=query)
        time.sleep(2)
    except Exception as e:
        print("Error", e)

    for i in range(0, 100):
        item_id = random.randint(1, 100)
        state = states[random.randint(0, len(states) - 1)]
        shipping_type = shipping_types[random.randint(0, len(shipping_types) - 1)]
        product_category = product_categories[random.randint(0, len(product_categories) - 1)]
        quantity = random.randint(1, 4)
        referral = referrals[random.randint(0, len(referrals) - 1)]
        price = random.randint(1, 100)
        order_date = datetime.date(2016, random.randint(1, 12), random.randint(1, 28)).isoformat()
        invoiceid = random.randint(1, 20000)

        data_order = (invoiceid, item_id, product_category, price, quantity, order_date, state, shipping_type, referral)

        query = """INSERT INTO public.sales
                                            (
                                            invoiceid, itemid, category, price, quantity, orderdate, destinationstate,shippingtype, referral
                                            )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        helper.insert_many(query=query, data=data_order)


main()
