from models import Market, Transactions, PIB, models_connection, models_keyspace
from cassandra.cqlengine import connection,management
from cassandra.cluster import Cluster
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from os import listdir
from os.path import isfile, join

import datetime
import traceback
import pandas as pd

PATH = "./data/"

"""
Spark app manager
I'm using spark because this is supposed to run in AWS Glue environment
"""
class SparkApp():
    """
    Path to the raw data folder, in can be a bucket as the name suggest. But you need to make the configurations for it.
    """
    BUCKET_PATH = PATH
    def __init__(self, app_name:str) -> None:
        # Configuration for the spark app.
        conf = SparkConf().setAppName(app_name)

        # Apply the configuration to a spark context
        self.sc = SparkContext(conf=conf)

        #Creates a session from the context
        self.session = SparkSession(self.sc) \
            .builder \
            .appName("reading from local") \
            .config("spark.pyspark.python", "python") \
            .getOrCreate()
        
    """
    Releases resources, it may not be necessary but is always good to free them.
    """
    def stop(self):
        self.session.stop()
        self.sc.stop()

    """
    This function approximates the transactions in the economy by checking the delta in the quantities between lookups.
    """
    def approximate_transactions(self, lookup_pair):
        # The date is extracted from the name of the json. This works because the name of the json is the timestamp in nanoseconds of the date of the lookup.
        # The date can be from any of the pairs, but in this case is the first one because of a design decision.
        date=datetime.datetime.fromtimestamp(int(lookup_pair[0].split(".")[0])/1000000000)
        #Obtain and read files from S3

        #Read the lookups, they must be ordered in this way (oldest, newest)
        df1 = self.session.read.json(self.BUCKET_PATH + lookup_pair[0])
        df2 = self.session.read.json(self.BUCKET_PATH + lookup_pair[1])

        #Explode nested values
        df1 = df1.withColumn("buys_quantity", col("buys").getField("quantity"))\
            .withColumn("buys_unit_price", col("buys").getField("unit_price"))\
            .withColumn("sells_quantity", col("sells").getField("quantity"))\
            .withColumn("sells_unit_price", col("sells").getField("unit_price"))\
            .drop("whitelisted").drop("buys").drop("sells")
        
        df2 = df2.withColumn("buys_quantity_newer", col("buys").getField("quantity"))\
            .withColumn("buys_unit_price_newer", col("buys").getField("unit_price"))\
            .withColumn("sells_quantity_newer", col("sells").getField("quantity"))\
            .withColumn("sells_unit_price_newer", col("sells").getField("unit_price"))\
            .drop("whitelisted").drop("buys").drop("sells")
        
        # Join datasets se we can take the differences on their fields
        intersected_lookup = df1.join(other=df2, on="id", how='inner')
        
        # Differentiate the fields and add those values as new columns
        intersected_lookup = intersected_lookup.withColumn('buys_delta', intersected_lookup['buys_quantity']-intersected_lookup['buys_quantity_newer']).withColumn('sells_delta', intersected_lookup['sells_quantity']-intersected_lookup['sells_quantity_newer'])

        # Ignore the no transactions rows and select just the needed columns
        transactions = intersected_lookup.filter((intersected_lookup.buys_delta>0) | (intersected_lookup.sells_delta>0)).select(['id', 'buys_unit_price', 'sells_unit_price', 'buys_delta', 'sells_delta'])

        # Collect the transactions and return them with the lookup date object
        approximated_transactions = []
        for row in transactions.collect():
            if row['buys_delta'] > 0:
                approximated_transactions.append({"item_id": row["id"], "price": row["buys_unit_price"], "trans_type": "buys", "quantity":row['buys_delta']})
            if row['sells_delta'] > 0:
                approximated_transactions.append({"item_id": row["id"], "price": row["sells_unit_price"], "trans_type": "sells", "quantity":row['sells_delta']})
        return (approximated_transactions, date)

"""
Scylla Manager
This class does the setup to use scylla. You must give the models that you want to be sync.
"""
class ScyllaManager():
    # Host to connect
    host_list = ['127.0.0.1']
    def __init__(self, models) -> None:
        # Do connection
        self.session = Cluster(self.host_list).connect()

        # Register connection
        self.register_session(models_connection)

        # It creates the specified keyspace, it can be anything, but my models export their respective keyspace and connection name
        self.create_keyspace(models_keyspace,[models_connection])
        
        #Sync models
        for Model in models:
            management.sync_table(Model)

    # Specifications for the context logic
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, tb):
        connection.unregister_connection(models_connection)
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
        return True

    # Handler for registering the connection
    def register_session(self, connection_name):
        connection.register_connection(connection_name, session=self.session)
    
    # Handler for creating keyspace. It always create a SimpleStrategy keyspace with one node. Change it at will and your own risk.
    def create_keyspace(self, name, connections):
        management.create_keyspace_simple(name,1,True,connections)

"""
Returns a sorted list pair of the lookups in the data folder. [(l1,l2),(l2,l3),(l3,l4),...]
"""
def get_data():
    data = [f for f in listdir(PATH) if isfile(join(PATH, f)) and f.endswith(".json")]
    data.sort()
    return [(previous, actual) for previous, actual in zip(data, data[1:])]

"""
Do the approximation of the transactions and store them in ScyllaDB
"""
def approximate_and_store_transactions_local():
    lookups=get_data()
    total = len(lookups)
    app = SparkApp("local_processing")
    for i, lookup in enumerate(lookups):
        transactions = app.approximate_transactions(lookup)
        for transaction in transactions[0]:
            # Insert the transaction in the cassandra model
            Transactions.create(item_id=transaction["item_id"], trans_type=transaction["trans_type"], quantity=transaction["quantity"], price=transaction["price"], date=transactions[1])
        print(f'{i+1}/{total} Done processing')
        if i+1 == total:
            pass
    app.stop()

def approximate_and_make_pibs_local(session):
    lookups=get_data()
    total = len(lookups)
    min_date = datetime.datetime.fromtimestamp(int(lookups[0][0].split(".")[0])/1000000000)
    max_date = datetime.datetime.fromtimestamp(int(lookups[-1][0].split(".")[0])/1000000000)
    dates = [single_date for single_date in (datetime.datetime(min_date.year, min_date.month, min_date.day, 0, 0, 0, 0) + datetime.timedelta(n) for n in range((max_date-min_date).days))]
    date_ranges = [(previous, actual) for previous, actual in zip(dates, dates[1:])]
    
    
    app = SparkApp("local_processing")
    for i, lookup in enumerate(lookups):
        transactions = app.approximate_transactions(lookup)
        for transaction in transactions[0]:
            Transactions.create(item_id=transaction["item_id"], trans_type=transaction["trans_type"], quantity=transaction["quantity"], price=transaction["price"], date=transactions[1])
        print(f'{i+1}/{total} Lookup processing')
    

    transactions_lookup = session.prepare("select sum(price),sum(quantity) from gmw_dw.transactions where date>=? and date<? allow filtering")
    total = len(date_ranges)
    for i, day_range in enumerate(date_ranges):
        query_set = scylla_manager.session.execute(transactions_lookup, [day_range[0], day_range[1]])
        PIB.create(year=day_range[0].year, month=day_range[0].month, day=day_range[0].day, value=(query_set.one()['system.sum(price)']*query_set.one()['system.sum(quantity)']))
        print(f'{i+1}/{total} PIB processing')
    app.stop()

with ScyllaManager(models=[Market, Transactions, PIB]) as scylla_manager:
    approximate_and_make_pibs_local(scylla_manager.session)
    #approximate_and_store_transactions_local()