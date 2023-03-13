import pyspark.sql.types as T

INPUT_DATA_PATH_FHV = 'fhv_tripdata_2019-01.csv'
INPUT_DATA_PATH_GREEN = 'green_tripdata_2019-01.csv'

BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT_ALL = 'rides_all '

PRODUCE_TOPIC_RIDES_CSV_GREEN = CONSUME_TOPIC_RIDES_CSV_GREEN = 'rides_green'
PRODUCE_TOPIC_RIDES_CSV_FHV = CONSUME_TOPIC_RIDES_CSV_FHV = 'rides_fhv'

RIDE_SCHEMA_GREEN = T.StructType(
    [T.StructField("VendorID", T.IntegerType()),
    T.StructField('lpep_pickup_datetime', T.TimestampType()),
    T.StructField('lpep_dropoff_datetime', T.TimestampType()),  
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("passenger_count", T.IntegerType()),
    T.StructField("trip_distance", T.FloatType()),   
    T.StructField("total_amount", T.FloatType()),
    T.StructField("payment_type", T.IntegerType())
     ])

RIDE_SCHEMA_FHV = T.StructType(
    [T.StructField("dispatching_base_num", T.StringType()),
    T.StructField('pickup_datetime', T.TimestampType()),
    T.StructField('dropOff_datetime', T.TimestampType()),
    T.StructField("PUlocationID", T.IntegerType()),
    T.StructField("DOlocationID", T.IntegerType())
     ])