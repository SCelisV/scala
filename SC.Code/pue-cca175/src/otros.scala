spark-shell
/*
 add todo el fichero al final de scelisexercises.txt
TODAS LAS SALIDAS LAS HE DEJADO EN EL MISMO SITIO: "/devsh_loudacre/OUT/"
*/
//--------------------------------------Scenario 1--------------------------------------
//*
Data description
All the customer records are stored in the HDFS directory dataset/retail_db/customers-tab-delimited
■ Data is in text format
■ Data is tab delimited
■ Schema is: 
◆ customer_id int,
◆ customer_fname string,
◆ customer_lname string,
◆ customer_email string,
◆ customer_password string,
◆ customer_street string,
◆ customer_city string,
◆ customer_state string,
◆ customer_zipcode string
Requerimientos de salida
■ Output all the customers who live in California
■ Use text format for the output files
■ Place de result data in dataset/results/scenario1/solution
■ Result should only contain records that have state value as "CA"
■ Output should only contain customer's full name. Example: Robert Hudson
*/
/*
Planteamientos:
Schema start in: "_c0"
crear un DF
leer fichero de texto de HDFS, campos separados por '\t' - no header
seleccionar customer_fname, customer_lname, customer_zipcode 
customer_state string == "CA"
concatenar customer_fname , customer_lname string,
write 
*/
/*
ls -l /tmp/dataset/retail_db/customers-tab-delimited
cat /retail_db/customers-tab-delimited/part-m-00000 | more
hdfs dfs -put /tmp/dataset/retail_db/customers-tab-delimited /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/customers-tab-delimited
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

import org.apache.spark.sql.types._

val filenameIn = "/devsh_loudacre/customers-tab-delimited"
val filenameOut = "/devsh_loudacre/OUT/customers-tab-delimited"

val dfColumns = List(
StructField ("devnum",LongType),
StructField ("customer_id", LongType),
StructField ("customer_fname", StringType),
StructField ("customer_lname", StringType),
StructField ("customer_email", StringType),
StructField ("customer_password", StringType),
StructField ("customer_street", StringType),
StructField ("customer_city", StringType),
StructField ("customer_state", StringType),
StructField ("customer_zipcode", StringType)
)
 
val dfSchema = StructType( dfColumns )

/*
val DFIn = ( spark. read. schema( dfSchema ). option( "sep", "\t" ). csv( filenameIn ) )
val DFIn = ( spark. read. option( "inferSchema", "true" ). csv( filenameIn ) )
*/

val DFIn = ( spark. read. option ( "sep", "\t" ). csv( filenameIn ) )
val DFOut = ( DFIn. 
select( $"customer_fname", $"customer_lname", $"customer_zipcode" ).
filter( $"customer_state"  == "CA" ).
select( concat_ws ( " ", $"customer_fname", $"customer_lname" ) ) )

val DFOut. show

val DFOut. write. mode( "overwrite", True). text( filenameOut )

//------------------

spark.read.
option( "sep", "\t" ).
csv( filenameIn )
select( "_c1", "_c2", "_c7").
filter( $"_c7" === "CA" ).
select( concat_ws( " ", $"_c1", $"_c2") ).
write.
mode( "overwrite" ).
text( filenameOut )

//------------------

spark.read. option( "inferSchema", true). option( "delimiter", "\t"). csv( filenameIn ). toDF(
"customer_id",
"customer_fname",
"customer_lname",
"customer_email",
"customer_password",
"customer_street",
"customer_city",
"customer_state",
"customer_zipcode").
createOrReplaceTempView( "customer_view")
spark.sql (
"""
SELECT concat_ws( " ", customer_fname, customer_lname)
FROM customer_view
WHERE customer_state = 'CA'
""").
write.
mode( "overwrite" ).
text( filenameOut )

//------------------ Comprobación

val filenameIn = filenameOut

( spark. read. option ( "sep", "\t" ). text( filenameIn ). 
select( split( $"value", " ")(0) as "first_name", split( $"value", " ")(1) as "last_name" ).
show( 5 )

//--------------------------------------EndScenario 1--------------------------------------

//--------------------------------------StartScenario 2--------------------------------------
/*
Data description
■ All the order records are stored in HDFS directory dataset/retail_db/orders_parquet.
■ Data is in parquet format.
Output requirements
■ Output all the completed orders (where order_status is 'COMPLETE')
■ Use json format for the output files
■ Place the result data in HDFS directory dataset/result/scenario2/solution.
■ order_date should be in format yyyy-MM-dd
https://sparkbyexamples.com/spark/spark-sql-unix-timestamp/
■ Compress the output using gzip compression
■ Output should only contain order_id, order_date, order_status.
*/
/*
Planteamientos:
formato del fichero parquet
Schema start in: "_c0"
select order_status
filtrar por where order_status is 'COMPLETE'
revisar y modificar el formato de order_date a yyyy-MM-dd
select order_date|order_id|order_status|
write json(path, mode=None, compression=None, dateFormat=None, timestampFormat=None, lineSep=None, encoding=None)
compression – compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate).
using gzip

ls -l /tmp/dataset/retail_db/orders_parquet
cat /dataset/retail_db/741ca897-c70e-4633-b352-5dc3414c5680.parquet | more
hdfs dfs -put /tmp/dataset/retail_db/orders_parquet/devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/orders_parquet
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/orders_parquet/"
val filenameOut = "/devsh_loudacre/OUT/orders_parquet/"

val DFIn = ( spark. read. format( "json" ). load( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn
.select ( $"order_status", $"order_date" ).
.filter ( $"order_status" === 'COMPLETE' ).
.drop ( $"order_date" ).
.withColumn ( $"order_date", from_unixtime( $"order_date", "yyyy-MM-dd" ) )
.select ( $"order_date", $"order_id", $"order_status" )
.write 
.option( "compression", "gzip" )
.mode( "overwrite", "true" )
.json( filenameOut )
)

DFOut. show()

DFOut. write. mode( "overwrite", True). text( filenameOut )

//------------------

spark.read.
load( filenameIn ).
filter( "order_status = 'COMPLETE'" ).
withColumn( "order_date", from_unixtime( $"order_date"/1000, "yyyy-MM-dd" ) ).
drop( "order_customer_id" ).
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
json( filenameIn )
//------------------ Comprobación

val filenameIn = filenameOut
( spark. read. load( filenameIn ). orderBy( "order_id" ). show( 10, false ) )

//--------------------------------------EndScenario 2--------------------------------------

//--------------------------------------StartScenario 3--------------------------------------
/*
Data description
*/
/*
Planteamientos:
Schema start in: "_c0"
Schema have the names indicated above.
read text, tab, sin head
filter customer_city string, "Caguas"
"customer_customer_id", "customer_fname", "customer_lname", "customer_street"
orc format y snappy compression
https://sparkbyexamples.com/spark/spark-read-orc-file-into-dataframe/

ls -l /tmp/dataset/retail_db/
cat /dataset/retail_db/ | more
hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/customers-tab-delimited"
val filenameOut = "/devsh_loudacre/OUT/customers-tab-delimited"

val dfColumns = Seq( ( "customer_customer_id" ), ( "customer_fname" ), ( "customer_lname" ), ( "customer_email" ), ( "customer_password" ), ( "customer_street" ), ( "customer_city" ), ( "customer_state" ), ( "customer_zipcode" )

val DFIn = ( spark. read. option ( "sep", "\t" ). csv ( filenameIn ). toDF( dfColums:_* ) ) 
DFIn.printSchema

val DFOut = (
DFIn .filter ( $"customer_city" === "Caguas" )
.withColumn ( $"customer_customer_id", DFIn.customer_customer_id. cast( "int" ) )
.write .option( "compression", "snappy" ) .mode( "overwrite", "true" ) .orc( filenameOut )
)

DFOut. show()

//------------------

val filenameIn = "/devsh_loudacre/customers-tab-delimited"
val filenameOut = "/devsh_loudacre/OUT/customers-tab-delimited"

val columnNames = Seq(
"customer_customer_id",
"customer_fname",
"customer_lname",
"customer_email",
"customer_password",
"customer_street",
"customer_city",
"customer_state",
"customer_zipcode" )
spark.read.
option( "sep", "\t" ).
csv( filenameIn ).
toDF( columnNames: _* ).
where( $"customer_city" === "Caguas" ).
withColumn( "customer_customer_id", $"customer_customer_id" cast "int" ).
write.
mode( "overwrite" ).
option( "compression", "snappy" ).
orc( filenameOut )

//------------------ Comprobación

val filenameIn = filenameOut

spark.read.
orc( filenameIn ).
select( "customer_customer_id", "customer_fname", "customer_lname", "customer_street" ).
orderBy( "customer_customer_id" ).
show( 10, false )

//--------------------------------------EndScenario 3--------------------------------------

//--------------------------------------StartScenario 4--------------------------------------
/*
Data description
■ All the categories records are stored at dataset/retail_db/categories.
■ Data is in text format
■ Data is comma separated
■ Schema is:
◆ category_id int,
◆ category_department_id int,
◆ category_name string
Output requirements
■ Convert data into tab delimited file
■ Use text format for the output files
■ Place de result data in HDFS directory dataset/result/scenario4/solution
■ Compress the output using lz4 compression
*/
/*
Planteamientos:
Schema start in: "_c0"
read text comma separated
write tab delimited - Compress lz4
*/
/*
ls -l /tmp/dataset/retail_db/categories
cat /dataset/retail_db/categories/part-m-00000 | more
hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/categories/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/categories"
val filenameOut =  "/devsh_loudacre/OUT/categories"

val DFIn = ( spark. read. csv ( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn
.drop ( $"_c0" ).withColumn( $"_c0", $"_c0". cast( "int" ) )
.drop ( $"_c1" ).withColumn( $"_c1", $"_c1". cast( "int" ) )
.drop ( $"_c2" ).withColumn( $"value", concat_ws( "\t ", $"_c0", $"_c1", $"_c2" ) )
.write()
.mode( "overwrite" )
.option( "compress", "lz4" )
.csv( filenameOut )
)

DFOut. show()

//------------------

val filenameIn = "/devsh_loudacre/categories/"
val filenameOut =  "/devsh_loudacre/OUT/categories/"
spark.read.
csv( filenameIn ).
selectExpr( "concat_ws('\t', * ) value" ).
write.
mode( "overwrite" ).
option( "compression", "lz4" ).
text( filenameOut )

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  text( filenameIn ).  orderBy( "value" ).  show( 10 , false )

//--------------------------------------EndScenario 4--------------------------------------

//--------------------------------------StartScenario 5--------------------------------------
/*
Data description
■ All the products records are stored at dataset/retail_db/products_avro
■ Data is in avro fromat
■ Data is compressed with snappy compression
Output requirements
■ Outuput should only contain the products with price greater than 1000.0
■ Use parquet format for the outuput files
■ Place the result in HDFS directory dataset/result/scenario5/solution
■ Compress the output using snappy compression
■ No se puede leer directamente con el método avro; no existe. Hay que usar for- mat("avro") y luego load.
■ save escribe directamente en formato parquet con compresión snappy.
*/
/*
Planteamientos:
Schema start in: "_c0"
avro snappy compression
filter products price gt 1000.0
save write parquet snappy compression
"product_id", "product_name", "product_price" desc 
*/
/*
ls -l /tmp/dataset/retail_db/products_avro
cat /dataset/retail_db/products_avro/part-m-00000.avro | more
hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/products_avro
hdfs dfs -ls /devsh_loudacre/products_avro
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/products_avro/" 
val filenameOut = "/devsh_loudacre/OUT/products_avro/"

val DFIn = ( spark. read. format( 'avro' ). option( "compression", "snappy" ). load( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn
.filter ( $"product_price" > 1000.0 )
.select ( $"product_id", "product_name", "product_price" ) 
.orderBy( $"product_id" && DFIn.product_price.desc() )
write. 
mode( "overwrite" ).
save( filenameOut )
)

DFOut. show()
//------------------
spark.read.
format( "avro" ).
load( filenameIn ).
filter( "product_price > 1000.0" ).
write.
mode( "overwrite" ).
save( filenameOut )


//------------------ Comprobación

val filenameIn = filenameOut

spark.read.
load( filenameIn ).
select( "product_id", "product_name", "product_price" ).
orderBy( "product_id" ).
show( 10, false )
//--------------------------------------EndScenario 5--------------------------------------

//--------------------------------------StartScenario 6--------------------------------------
/*
Data description
Data description
■ Get data from metastore table named orders.
■ Table is present in the database default.
Output requirements
■ Fetch orders from Jan-2013 to Dec-2013.
■ Use parquet format for the output files.
■ Place the result data in HDFS directory dataset/result/scenario6/solution.
*/
/*
Planteamientos:
Schema start in: "_c0"
read hive default/orders
filter orders from Jan-2013 to Dec-2013.
write order_id| order_date|order_customer_id| order_status
write parquet compress gzip

hdfs dfs -ls /user/hive/warehouse/default
hdfs dfs -ls /
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "orders"
val filenameOut = "/devsh_loudacre/OUT/orders"

val DFIn = ( spark. read. table ( "filenameIn" ) ) 
DFIn.printSchema

val DFOut = ( 
DFIn.
filter( "year(order_date ) = 2013" ).
select ( $"order_id", $"order_date", $"order_customer_id", $"order_status" )
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
save( filenameOut )
)

//------------------

spark.read.
table( filenameIn ).
filter( "year(order_date) = 2013" ).
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
save( filenameIn )

//------------------

spark.sql(
"""
SELECT * FROM orders
WHERE year( order_date ) = '2013'
""").
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
save( filenameIn )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut

spark.read.
load( filenameIn ).
orderBy( "order_id" ).
show( 10 )

//--------------------------------------EndScenario 6--------------------------------------

//--------------------------------------StartScenario 7--------------------------------------
/*
Data description
■ All the categories records are stored at dataset/retail_db/categories.
■ Data is in text format
■ Data is comma separated
■ Schema is:
◆ category_id int,
◆ category_department_id int,
◆ category_name string
Output requirements
■ Save all categories in metastore table categories_replica in scenario7_db database
■ Use no compression
■ Si se pone format( "hive" ), la tabla se crea con formato texto. Si no se pone format( "hive" ) se crea en formato parquet.
*/
/*
Planteamientos:
Schema start in: "_c0"
read text csv no header
write newdb/categories_replica

ls -l /tmp/dataset/retail_db/categories
cat /dataset/retail_db/ | more
hdfs dfs -put 
hdfs dfs -ls /user/hive/warehouse/default
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/categories"
val filenameOut = "newdb.categories_replica"

val DFIn = ( spark.  read.  table ( filenameIn )) 
DFIn.printSchema

val DFOut = ( 
DFIn.
write
.withRenameColumn( $"_c0", $"category_id". cast( "int" ) ).
.withColumn( $"_c0", $"category_id". cast( "int" ) ).
.withColumn( $"_c0", $"category_name" ).
mode( "overwrite" ).
format ( "hive" ).
option( "compression", none ).
option( "path", "/devsh_loudacre/" ).
saveAsTable( filenameOut )
)

//------------------

spark.sql( "CREATE database if not exists scenario7_db" )
spark.read.
option( "inferSchema", true).
csv( filenameIn ).
toDF( "category_id", "category_department_id", "category_name" ).
write.
mode( "overwrite" ).
option( "compression", "uncompressed" ).
saveAsTable( filenameOut)

//------------------ Comprobación

val filenameIn = filenameOut
val df = spark.read.
table( out_table )
df.printSchema
df.
orderBy( "category_id" ).
show( 10 )

//--------------------------------------EndScenario 7--------------------------------------

//--------------------------------------StartScenario 8--------------------------------------
/*
Data description
■ All the categories records are stored at dataset/retail_db/categories.
■ Data is in text format
■ Data is comma separated
■ Schema is:
◆ category_id int,
◆ category_department_id int,
◆ category_name string
Output requirements
■ Create a metastore table named categories_parquet.
■ Table should only contain category_id, category_name.
■ Use parquet format for the output files.
*/
/*
Planteamientos:
Schema start in: "_c0"
read csv
select category_id int, category_name string
write parquet table categories_parquet.
Si no se pone format( "hive" ) se crea en formato parquet.
save table escribe directamente en formato parquet con compresión snappy.

ls -l /tmp/dataset/retail_db/categories
cat /dataset/retail_db/categories/part-m-00000 | more
hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/categories"
val filenameOut = ( "categories_parquet" )

val DFIn = ( spark. read. option( "inferSchema", "true" ). csv( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
select( $"category_id", $"category_name" ) 
write.
mode( "overwrite" ).
saveAsTable( filenameOut )
)

//------------------

spark.read.
option( "inferSchema", true).
13csv( in_dir ).
drop( "_c1" ).
toDF( "category_id", "category_name" ).
write.
mode( "overwrite" ).
saveAsTable( filenameOut )

//------------------ Comprobación

val filenameIn = filenameOut

spark. read. table (filenameIn). orderBy( "category_id" ).  show( 10 )

//--------------------------------------EndScenario 8--------------------------------------

//--------------------------------------StartScenario 9--------------------------------------
/*
Data description
■ All the products records are stored at dataset/retail_db/products_avro
■ Data is in avro format
■ Data is compressed with snappy compression
Output requirements
■ Output should contain columns product_id, product_price.
■ Save output as json file
■ Place the result data in HDFS directory dataset/result/scenario9/solution
■ Use no compression
json no se comprime por omisión. Por lo tanto no hace falta poner nada. 
Para indicar que no hay compresión, hay dos posibilidades:
option( "compression", "uncompressed")
option( "compression", "none")
*/
/*
Planteamientos:
Schema start in: "_c0"
read avro, snappy Hay que usar format("avro") y luego load.
orderBy( "product_id" )
select product_id, product_price
write json 
option( "compression", "none")

ls -l /tmp/dataset/retail_db/products_avro
cat /tmp/dataset/retail_db/products_avro/part-m-00000.avro | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/products_avro"
val filenameOut = "/devsh_loudacre/OUT/products_avro"

val DFIn = ( spark.  read.  format( "avro" ).  load( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
select ( $"product_id", $"product_price" ).
write.
mode( "overwrite" ).
json( filenameOut )
option( "compression", "none").
)

//------------------

val in_dir = "dataset/retail_db/products_avro/"
val out_dir = "dataset/result/scenario9/solution/"
spark.read.
format( "avro" ).
load( in_dir ).
select( "product_id", "product_price" ).
write.
mode( "overwrite" ).
option( "compression", "uncompressed" ).
json( out_dir )

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  json( out_dir ).  orderBy( "product_id" ).  show( 10 )

//--------------------------------------EndScenario 9--------------------------------------

//--------------------------------------StartScenario 10--------------------------------------
/*
Data description
■ All the categories records are stored at dataset/retail_db/categories.
■ Data is in text format
■ Data is comma separated
■ Schema is:
◆ category_id int,
◆ category_department_id int,
◆ category_name string
Output requirements
■ Create a metastore table named categories_partitioned. The format of the data must be parquet.
■ Table must be partitioned by category_department_id.
■ Save all categories in metastore table categories_partitioned.
*/
/*
Planteamientos:
Schema start in: "_c0"

read csv inferSchema sin header
category_id int,
category_department_id int,
category_name string
partitioned by category_department_id
write parquet saveAsTable categories_partitioned 
save table escribe directamente en formato parquet con compresión snappy.
Si no se pone format( "hive" ) se crea en formato parquet.

ls -l /tmp/dataset/retail_db/categories
cat /dataset/retail_db/categories/part-m-00000 | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/
//--------------------------------------scala

val filenameIn = "/devsh_loudacre/categories"
val filenameOut = "/devsh_loudacre/OUT/categories_partitioned"

val DFIn = ( spark.  read.  option. ( "inferSchema", "true" ). csv. ( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
toDF( $"category_id", $"category_department_id", $"category_name" ).
"""
esto es para probrar en lugar del toDF
select ( $"category_id", $"category_department_id", $"category_name" ).
"""
write.
partitionBy( "category_department_id" ).
mode( "overwrite" ).
saveAsTable( filenameOut )
)

//------------------

val in_dir = "dataset/retail_db/categories"
val out_table = "default.categories_partitioned"
spark.read.
csv( in_dir ).
select(
16$"_c0" as "category_id" cast "int",
$"_c1" as "category_department_id" cast "int",
$"_c2" as "category_name").
write.
mode( "overwrite" ).
partitionBy( "category_department_id" ).
saveAsTable( out_table )

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  table( out_table ).  orderBy( "category_id" ).  show( 10 )


//--------------------------------------EndScenario 10--------------------------------------

//--------------------------------------StartScenario 11--------------------------------------
/*
Data description
*/
/*
Planteamientos:
Schema start in: "_c0"
*/
/*
ls -l /tmp/dataset/retail_db/
cat /dataset/retail_db/ | more
hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "/devsh_loudacre/"
val filenameOut = "/devsh_loudacre/OUT/"

val DFIn = ( 

)
DFIn.printSchema

val DFOut = ( 
DFIn.

)

DFOut. show()

//------------------

//------------------ Comprobación

val filenameIn = filenameOut

//--------------------------------------EndScenario 11--------------------------------------

//--------------------------------------StartScenario .--------------------------------------
/*
Data description
*/
/*
Planteamientos:
Schema start in: "_c0"
*/
/*
ls -l /tmp/dataset/retail_db/
cat /dataset/retail_db/ | more
hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "/devsh_loudacre/"
val filenameOut = "/devsh_loudacre/OUT/"

val DFIn = ( 

)
DFIn.printSchema

val DFOut = ( 
DFIn.

)

DFOut. show()

//------------------

//------------------ Comprobación

val filenameIn = filenameOut

//--------------------------------------EndScenario .--------------------------------------
