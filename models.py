from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

# This truncate the data in datetimes to appear as they will do in the database. So this "syncs" the precision of the DateTime field
columns.DateTime.truncate_microseconds = True

# Name of my connection and keyspace
models_connection='single_node'
models_keyspace='gmw_dw'

"""
It is important to note that the order in which the primary keys are created is the order they will retain in the cql table partition.
"""
class Market(Model):
    __keyspace__ = models_keyspace
    __table_name__ = 'market'
    __connection__ = models_connection
    item_id=columns.Integer(primary_key=True)
    buys_unit_price=columns.Integer()
    sells_unit_price=columns.Integer()

class Transactions(Model):
    __keyspace__ = models_keyspace
    __table_name__ = 'transactions'
    __connection__ = models_connection
    item_id=columns.Integer(primary_key=True)
    date = columns.DateTime(primary_key=True)
    trans_type=columns.Text(primary_key=True)
    price = columns.BigInt()
    quantity = columns.BigInt()

class PIB(Model):
    __keyspace__ = models_keyspace
    __table_name__ = 'pib'
    __connection__ = models_connection
    year = columns.SmallInt(primary_key=True)
    month = columns.SmallInt(primary_key=True)
    day = columns.SmallInt(primary_key=True)
    value = columns.BigInt()