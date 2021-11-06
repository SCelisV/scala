spark-shell
/*
 add todo el fichero al final de scelisexercises.txt
TODAS LAS SALIDAS LAS HE DEJADO EN EL MISMO SITIO: "/devsh_loudacre/OUT/"
*/
//--------------------------------------Scenario 1.--------------------------------------
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

val DFIn = ( spark. read. schema( dfSchema ). option( "sep", "\t" ). csv( filenameIn ) )

val DFIn = ( spark. read. option ( "sep", "\t" ). csv( filenameIn ) )

val DFOut = ( DFIn. 
select( $"customer_fname", $"customer_lname", $"customer_zipcode" ).
filter( $"customer_state"  == "CA" ).
select( concat_ws ( " ", $"customer_fname", $"customer_lname" ) )

val DFOut. show

val DFOut. write. mode( "overwrite" ). text( filenameOut )

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

//--------------------------------------EndScenario 1.--------------------------------------

//--------------------------------------StartScenario 2.--------------------------------------
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

//--------------------------------------EndScenario 2.--------------------------------------

//--------------------------------------StartScenario 3.--------------------------------------
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

//--------------------------------------EndScenario 3.--------------------------------------

//--------------------------------------StartScenario 4.--------------------------------------
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
.write
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

//--------------------------------------EndScenario 4.--------------------------------------

//--------------------------------------StartScenario 5.--------------------------------------
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
//--------------------------------------EndScenario 5.--------------------------------------

//--------------------------------------StartScenario 6.--------------------------------------
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

//--------------------------------------EndScenario 6.--------------------------------------

//--------------------------------------StartScenario 7.--------------------------------------
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

//--------------------------------------EndScenario 7.--------------------------------------

//--------------------------------------StartScenario 8.--------------------------------------
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

//--------------------------------------EndScenario 8.--------------------------------------

//--------------------------------------StartScenario 9.--------------------------------------
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

//--------------------------------------EndScenario 9.--------------------------------------

//--------------------------------------StartScenario 10.--------------------------------------
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


//--------------------------------------EndScenario 10.--------------------------------------

//--------------------------------------StartScenario 11.--------------------------------------
/*
Data description
■ All the customer records are stored at dataset/retail_db/customers-avro
■ Data is in avro format
Output requirements
■ Convert all data into tab delimited file
■ Use text format for the outupt files
■ Use bzip2 compression
■ Place the result data at dataset/result/scenario11/solution
■ Ouput should only contain customer_id, customer_name
■ customer_name is first caracter of first name and complete last name separated by space.
A la hora de leer, no es necesario indicar que los datos están comprimidos. Spark se da cuenta y los descomprime. 
Es más, si se indica un formato incorrecto, por ejemplo, gzip en vez de bzip2, Spark lee correctamente.
*/
/*
Planteamientos:
Schema start in: "_c0"
read Hay que usar format("avro") y luego load.
select _c0| _c1
write
_c0 = customer_id, 
_c1 = customer_name = customer_name = first (first name) + " " + (last name)
write text '\t' 
bzip2 compression
*/
/*
ls -l /tmp/dataset/retail_db/customers-avro
cat /dataset/retail_db/customers-avro/part-m-00000.avro | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "/devsh_loudacre/customers-avro"
val filenameOut = "/devsh_loudacre/OUT/customers-avro"

val DFIn = ( spark.  read.  format( "avro" ).  load( filenameIn ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
withColumn( $"customer_fname", substring( $"customer_fname" ), 1, 1 ).
withColumn( $"customer_name", concat_ws( " ", $"customer_fname", $"customer_lname" ) ).
select( $"customer_id", $"customer_name" ).
write.
mode( "overwrite" ).
option( "compression", "bzip2" ).
option( "sep", "\t" ).
text( filenameOut )
)

DFOut. show()

//------------------

spark.read.
format( "avro" ).
load( in_dir ).
select( "customer_id", "customer_fname", "customer_lname" ).
withColumn( "customer_fname", substring( $"customer_fname", 1, 1 ) ).
withColumn( "customer_name",
concat_ws( " ", $"customer_fname", $"customer_lname" ) ).
select( "customer_id", "customer_name" ).
select( concat_ws( "\t", $"customer_id", $"customer_name" ) ).
write.
mode( "overwrite" ).
option( "compression", "bzip2" ).
text( out_dir )

//------------------
//------------------

val in_dir = "dataset/retail_db/customers-avro/"
val out_dir = "dataset/result/scenario11/solution/"
spark.read.
format( "avro" ).
load( in_dir ).
select( "customer_id", "customer_fname", "customer_lname").
createOrReplaceTempView( "customers" )
spark.sql(
"""
SELECT customer_id,
concat_ws( ' ',
substring( customer_fname, 1, 1 ), customer_lname) customer_name
FROM customers
""").
map( _.mkString( "\t" ) ).
write.
mode( "overwrite" ).
option( "compression", "bzip2" ).
text( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  option( "sep", "\t" ).  csv( out_dir ).  show( 10 )

//--------------------------------------EndScenario 11.--------------------------------------

//--------------------------------------StartScenario 12.--------------------------------------
/*
Data description
■ All the order records are stored in HDFS directory dataset/retail_db/orders_parquet.
■ Data is in parquet format.
Output requirements
■ Output all the PENDING orders in July 2013
■ Use json format for the output files
■ Place the result data in HDFS directory dataset/result/scenario12/solution,
■ order_date should be in format yyyy-MM-dd.
■ Compress the output using snappy compression and output should only contain order_date, order_status.
*/
/*
Planteamientos:
Schema start in: "_c0"
read parquet format
order_date, order_status
filter PENDING 2013 and 07
order_date 'yyyy-MM-dd'
write json snappy
json no se comprime por omisión. Por lo tanto no hace falta poner nada. 
*/
/*
ls -l /tmp/dataset/retail_db/orders_parquet
cat /dataset/retail_db/orders_parquet/741ca897-c70e-4633-b352-5dc3414c5680.parquet  | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "/devsh_loudacre/orders_parquet"
val filenameOut = "/devsh_loudacre/OUT/orders_parquet"

val DFIn = ( spark.  read.  parquet. ( filenameIn) )
DFIn.printSchema

val DFOut = ( 
DFIn.
filter ( $"order_status" == 'PENDING' || year( $"order_date" ) == 2013 && month( $"order_date" ) == 07 ).
withColumn ( $"order_date", from_unixtime( $"order_date", "yyyy-MM-dd" ) ).
select ( $"order_date", $"order_status" ).
write.
mode( "overwrite" )
option( "compression", "snappy").
json( filenameOut )
)

DFOut. show()

//------------------

val in_dir = "dataset/retail_db/orders_parquet/"
val out_dir = "dataset/result/scenario12/solution/"
spark.read.
load( in_dir ).
filter( "order_status = 'PENDING'" ).
withColumn(
"order_date",
from_unixtime( $"order_date"/1000, "yyyy-MM-dd" ) ).
select( "order_date", "order_status" ).
filter( "substring( order_date, 1, 7 ) = '2013-07'").
write.
mode( "overwrite" ).
option( "compression", "snappy" ).
json( out_dir )

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  json( out_dir ).  orderBy( "order_date" ).  show( 10 )

//--------------------------------------EndScenario 12.--------------------------------------

//--------------------------------------StartScenario 13.--------------------------------------
/*
Data description
■ Get data from metastore table named customers.
■ Table is present in the database default.
Output requirements
■ Get records from the metastore table named customers whose customer_fname is like Rich.
■ Use parquet file for the outuput files.
■ Place the result data in HDFS directory dataset/result/scenario13/solution.
■ Compress the output using snappy compression.
■ Con poner save es suficiente. Spark usa format parquet y snappy compression por omisión.
*/
/*
Planteamientos:
Schema start in: "_c0"
read. table "default. customers"
"customers"
filter customer_fname like Rich.
customer_id, customer_fname, customer_lname
save
*/
/*
ls -l /tmp/dataset/retail_db/
cat /dataset/retail_db/ | more

hdfs dfs -put 
hdfs dfs -ls /user/hive/warehouse/default
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "default.customers"
val filenameOut = "/devsh_loudacre/OUT/customers"

val DFIn = ( spark. read. table ( filenameIn ) ) 
DFIn.printSchema

val DFOut = ( 
DFIn.
select ( $"customer_fname" ).
filter ( $"customer_fname" like "%Rich" ).
select ( $"customer_id", $"customer_fname", $"customer_lname" ).
write.
mode( "overwrite" ).
save( filenameOut )
)

DFOut. show()

//------------------

val in_table = "default.customers"
val out_dir = "dataset/result/scenario13/solution/"
spark.sql(
"""
SELECT *
FROM customers
WHERE customer_fname LIKE 'Rich%'
""").
write.
mode( "overwrite" ).
save( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  load( out_dir ).  orderBy( "customer_id" ).  show( 10, false )

//--------------------------------------EndScenario 13.--------------------------------------

//--------------------------------------StartScenario 14.--------------------------------------
/*
Data description
■ All the customer records are stored at dataset/retail_db/customers-tab-delimited.
■ Data is in text format
■ Data is tab delimited
■ Schema is:
◆ customer_customer_id int,
◆ customer_fname string,
◆ customer_lname string,
◆ customer_email string,
◆ customer_password string,
◆ customer_street string,
◆ customer_city string,
◆ customer_state string,
◆ customer_zipcode string
Output requirements
■ Get total number of customers in each state whose first name starts with M.
■ Use parquet format for the ouput files
■ Place the result in HDFS directory dataset/result/scenario14/solution
■ Use gzip compression
■ Output should only contain the columns customer_state, count.
*/
/*
Planteamientos:
Schema start in: "_c0"
read text format tab delimited
filter substrign first name 1,1 == "M"
groupby for state and count total customers in each 
customer_state, count
write parquet gzip
save write parquet snappy compression
*/
/*
ls -l /tmp/dataset/retail_db/customers-tab-delimited
cat /dataset/retail_db/customers-tab-delimited | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "/devsh_loudacre/customers-tab-delimited"
val filenameOut = "/devsh_loudacre/OUT/customers-tab-delimited"

val dfColumns = Seq( "customer_customer_id", "customer_fname", "customer_lname", "customer_email", "customer_password", "customer_street", "customer_city", "customer_state", "customer_zipcode" )

val DFIn = ( spark. read. option ( "sep", "\t" ). csv ( filenameIn ). toDF( dfColums ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
select( "customer_fname" ).
withColumn( "customer_fname" , substring( "customer_fname", 1, 1 ) ).
filter( "customer_fname" == "M" ).
groupBy( "customer_state" ).count().
select( "customer_state", "count" ).
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
save( filenameOut )
)

DFOut. show()

//------------------

val in_dir = "dataset/retail_db/customers-tab-delimited/"
val out_dir = "dataset/result/scenario14/solution/"
spark.read.
option( "sep", "\t" ).
csv( in_dir ).
filter( "_c1 LIKE 'M%'" ).
selectExpr( "_c7 customer_state").
groupBy( "customer_state" ).
count.
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
save( out_dir )

//------------------
//------------------

spark.read.
option("sep", "\t").
csv( in_dir ).
select( "_c1", "_c7").
toDF( "customer_name", "customer_state" ).
createOrReplaceTempView( "customers" )
spark.sql(
"""
SELECT customer_state, COUNT(*) count
FROM customers
WHERE customer_name LIKE 'M%'
GROUP BY customer_state
""").
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
parquet( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut

spark.read.  load( out_dir ).  orderBy( "customer_state" ).  show( 10 )

//--------------------------------------EndScenario 14.--------------------------------------

//--------------------------------------StartScenario 15.--------------------------------------
/*
Data description
■ All the customer records are stored at dataset/retail_db/customers.
■ Format is csv with no header.
■ The schema of the customers record is:
◆ customer_customer_id int,
◆ customer_fname string,
◆ customer_lname string,
◆ customer_email string,
◆ customer_password string,
◆ customer_street string,
◆ customer_city string,
◆ customer_state string,
◆ customer_zipcode string
■ All the order records are stored at dataset/retail_db/orders. Format is csv.
■ Format is csv with no header.
■ The schema of the orders records is:
◆ order_id int,
◆ order_date string,
◆ order_customer_id int,
◆ order_status string
Output requirements
■ Find out customers who have made more than 5 orders.
■ Customer name must start with M.
■ Use text format for the output files.
■ Use " | " as field separator.
■ Use gzip compression.
■ Place the result data in HDFS directory dataset/result/scenario15/solution
■ There should be only 1 file in the output
■ Output should only contain customer_fname, customer_lname, count
■ Output should be sorted by count in descending order.
"""
*/
/*
Planteamientos:
Schema start in: "_c0"
read csv customers, orders
join files ◆ customer_customer_id int, ◆ order_customer_id int,
filter ◆ customer_fname string like M%
groupBy ( customer, orders ). count
having ( customer, orders ) having > 5
select customer_fname, customer_lname, count
orderby  count desc
write text "|" pipe delimited gzip
*/
/*
ls -l /tmp/dataset/retail_db/customers
ls -l /tmp/dataset/retail_db/orders
cat /dataset/retail_db/customers/part-m-00000 | more
cat /dataset/retail_db/orders/part-m-00000 | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val fileInOne = "/devsh_loudacre/customers"
val fileInTwo = "/devsh_loudacre/orders"

val filenameOut = "/devsh_loudacre/OUT/"

val DFCusIn = ( spark.  read.  option( "inferSchema", "true" ). csv. ( fileInOne ) )
val DFCusIn.printSchema()
val DFOrdIn = ( spark.  read.  option( "inferSchema", "true" ). csv. ( fileInTwo ) )
val DFOrdIn.printSchema()

val DFIn.printSchema

val DFOut = ( 
DFIn.
select ( "customer_fname", "customer", "orders", "customer_fname", "customer_lname" ).
filter ( "customer_fname" like M% ).
groupBy ( "customer", "orders" ). count(). as ( "count" ).
filter ( "count" ) > 5 ).
sort( "count" ).desc() )
coalesce( 1 ).
selectExpr( " concat_ws( '|', "customer_fname", "customer_lname", "count" " ).
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
text( "filenameOut" )
)

DFOut. show()

//------------------

val in_customers = "dataset/retail_db/customers/"
val in_orders = "dataset/retail_db/orders/"
val out_dir = "dataset/result/scenario15/solution/"
val customers_view = "customers_view"
val orders_view = "orders_view"
spark.read.
csv( in_customers ).
selectExpr( "_c0 id", "_c1 customer_fname", "_c2 customer_lname" ).
createOrReplaceTempView( customers_view )
spark.read.
csv( in_orders ).
selectExpr( "_c2 id" ).
createOrReplaceTempView( orders_view )
spark.sql(
"""
SELECT customer_fname, customer_lname, COUNT(1) count
FROM customers_view
NATURAL JOIN orders_view
WHERE customer_fname LIKE 'M%'
GROUP BY id, customer_fname, customer_lname
HAVING COUNT(1) > 5
ORDER BY count DESC
""").
coalesce( 1 ).
selectExpr( "concat_ws('|', *) value" ).
write.
mode( "overwrite" ).
option( "compression", "gzip" ).
text( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  text( out_dir ).  orderBy( "value" ).  show( 20, false )

//--------------------------------------EndScenario 15.--------------------------------------

//--------------------------------------StartScenario 16.--------------------------------------
/*
Data description
■ All the order records are stored at dataset/retail_db/orders.
■ Format of files is csv with no header
■ The schema of the orders records is:
◆ order_id int,
◆ order_date string,
◆ order_customer_id int,
◆ order_status string
Output requirements
■ Find all fraud transactions per month
■ Order status must be equal to SUSPECTED_FRAUD
■ Use parquet format for the ouput files
■ Use snappy compression
■ Place the result data in HDFS directory dataset/result/scenario16/solution/
■ Output should only contain order_date, count
■ Use yyyy-MM format for order_date
■ Output should be sorted by order_date in descending order.
*/
/*
Planteamientos:
Schema start in: "_c0"
read csv, no header, 
filter order_status = "SUSPECTED_FRAUD"
order_date = yyyy-MM
groupBy year, month, count()
orderBy order_date desc
order_date, count
write parquet snappy
save escribe directamente en formato parquet con compresión snappy.
*/
/*
ls -l /tmp/dataset/retail_db/orders
cat /dataset/retail_db/orders/part-m-00000 | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val filenameIn = "/devsh_loudacre/orders"
val filenameOut = "/devsh_loudacre/OUT/orders"

val DFIn = ( spark.  read.  option ( "inferSchema", "true" ).  csv ( filenameIn) )
DFIn.printSchema

val DFOut = ( 
DFIn.
select( $"order_id", $"order_date", $"order_customer_id", $"order_status" ).
filter(  $"order_status" == "SUSPECTED_FRAUD" ).
drop( $"order_date" ).
withColumn ( $"order_date", from_unixtime( $"order_date", "yyyy-MM" ) ).
groupBy( $"order_date" ) .count(). as( "count" ).
sort( $"order_date" ).desc().
select( $"order_date", $"count" )
write.
mode( "overwrite")
save ( filenameOut )
)
DFOut. show()

//------------------

//------------------

val DFIn = ( spark.  read.  option ( "inferSchema", "true" ).  csv ( filenameIn) )
DFIn.printSchema

val DFOut = ( 
DFIn.

select( $"order_id", $"order_date", $"order_customer_id", $"order_status" ).
filter(  $"order_status" == "SUSPECTED_FRAUD" ).
drop( $"order_date" ).
withColumn ( $"order_date", from_unixtime( $"order_date", "yyyy-MM" ) ).
toDF( $"order_id", $"order_date", $"order_customer_id", $"order_status" ).
createOrReplaceTempView( "orders_view" ) )
spark.sql(
"""
SELECT order_date, COUNT(*) count
FROM orders_view
GROUP BY order_date
ORDER BY order_date DESC
""").
write.
mode( "overwrite").
save ( filenameOut ) )

DFOut. show()
//------------------

//------------------

val in_dir = "dataset/retail_db/orders/"
val out_dir = "dataset/result/scenario16/solution/"
spark.read.
csv( in_dir ).
filter( "_c3 = 'SUSPECTED_FRAUD'" ).
selectExpr( "substring(_c1, 1, 7) order_date" ).
groupBy( "order_date" ).
count.
orderBy( $"order_date".desc ).
write.
mode( "overwrite" ).
save( out_dir )

//------------------
//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  load( out_dir ).  show

//--------------------------------------EndScenario 16.--------------------------------------

//--------------------------------------StartScenario 17.--------------------------------------
/*
Data description
■ All the product records are stored at dataset/retail_db/products
■ All the category records are stored at dataset/retail_db/categories
■ Data is in text format
■ The schema of the products records is:
◆ product_id int,
◆ product_category_id int,
◆ product_name, string,
◆ product_description string,
◆ product_price double,
◆ product_image string
The schema of the categories records is:
◆ category_id int,
◆ category_department_id int,
◆ category_name string
Output requirements
■ Get maximum product_price in each product_category
■ Get minimum product_price in each product_category
■ Get average product_price in each product_category
■ Round values to 2 decimal places
■ Place the result data in HDFS directory dataset/result/scenario17/solution
■ Save the output as JSON file
■ Compress the output using deflate compression
■ Output data should contain columns category_name, max_price, min_price, avg_price.
■ A la hora de leer products se puede poner option( "inferSchema", true ); 
de este modo no es necesario hacer el cast de las columnas max_price, min_price, y avg_price a double. 
Con lo cual, se pondría select( $"category_name", format_number( $"max_price", 2 ) alias "max_price", format_number( $"min_price", 2 ) alias "min_price", format_number( $"avg_price", 2 ) alias "avg_price")
■ La función format_number se puede evitar usando el ROUND en la sentencia SQL:
*/
/*
Planteamientos:
Schema start in: "_c0"
read Data text format no header, ',' delimited option( "inferSchema", true ); 
product_id int,
product_category_id int,
product_name, string,
product_description string,
product_price double,
product_image string
category_id int,
category_department_id int,
category_name string
join product_category_id int == category_id int,
select category_id, category_name, product_price double,
groupBy product_category_id int,
format_number( $"max_price", 2 ) alias "max_price", 
format_number( $"min_price", 2 ) alias "min_price", 
format_number( $"avg_price", 2 ) alias "avg_price"
format_number se puede evitar con ROUND 
ROUND( MAX( price ), 2) max_price,
ROUND( MIN( price ), 2) min_price,
ROUND( AVG( price ) ,2) avg_price
select category_name, max_price, min_price, avg_price.
write JSON deflate compression
*/
/*
ls -l /tmp/dataset/retail_db/products
cat /tmp/dataset/retail_db/products/part-m-00000 | more
ls -l /tmp/dataset/retail_db/categories
cat /tmp/dataset/retail_db/categories/part-m-00000 |more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val fileInOne = "/devsh_loudacre/products"
val fileInTwo = "/devsh_loudacre/categories"

val DFPrdIn = ( spark.  read.  option( "inferSchema", "true" ).  text( fileInOne ) )
DFPrdIn.printSchema
val DFCatIn = ( spark.  read.  option( "inferSchema", "true" ).  text( fileInTwo ) )
DFCatIn.printSchema

val filenameOut = "/devsh_loudacre/OUT/catpro"

val DFIn = ( DFCatIn. join( DFPrdIn, DFPrdIn.product_category_id == DFCatIn.category_id ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
select ( $"product_category_id", $"product_price", $"category_id", $"category_name" ).
groupBy ( "product_category_id" ).
max ( $"product_price" ).format_number( $"max_price", 2 ) alias "max_price", 
min( $"product_price" ).format_number( $"min_price", 2 ) alias "min_price", 
avg( $"product_price" ).format_number( $"avg_price", 2 ) alias "avg_price"
select( $"category_name", $"max_price", $"min_price", $"avg_price" ).
write.
mode( "overwrite" ).
option( "compression", "deflate" ).
json.( filenameOut )
)

DFOut. show()

//------------------

val DFIn = ( DFCatIn. join( DFPrdIn, DFPrdIn.product_category_id == DFCatIn.category_id ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
select ( $"product_category_id", $"product_price", $"category_id", $"category_name" ).
toDF( $"product_category_id", $"product_price", $"category_id", $"category_name" ).
createOrReplaceTempView( "prdcat_view" ) )

(spark.sql(
"""
SELECT product_category_id, category_name, round(max(product_price), 2) as max_price, round(min(product_price), 2) as min_price, round(avg(product_price), 2) as avg_price
FROM prdcat_view
GROUP BY product_category_id, category_name
ORDER BY product_category_id
""").
write.
mode( "overwrite" ).
option( "compression", "deflate" ).
json.( filenameOut )

//------------------

//------------------

val products_dir = "dataset/retail_db/products/"
val categories_dir = "dataset/retail_db/categories/"
val product_view = "product_view"
val categories_view = "categories_view"
val out_dir = "dataset/result/scenario17/solution/"
spark.read.
option( "inferSchema", true).
csv( products_dir ).
select(
$"_c1" as "id",
$"_c4" as "price" cast "double" ).
createOrReplaceTempView( product_view )
spark.read.
csv( categories_dir ).
select(
$"_c0" as "id",
$"_c2" as "category_name" ).
createOrReplaceTempView( categories_view )
spark.sql(
"""
SELECT
category_name,
round( max( price ), 2) max_price,
round( min( price ), 2) min_price,
round( avg( price ), 2) avg_price
FROM product_view
NATURAL JOIN categories_view
GROUP BY id, category_name
""").
write.
mode( "overwrite" ).
option( "compression", "deflate" ).
json( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  json( out_dir ).  orderBy( "avg_price" ).  show( 10 )

//--------------------------------------EndScenario 17.--------------------------------------

//--------------------------------------StartScenario 18.--------------------------------------
/*
Data description
■ All the customer records are stored at dataset/retail_db/customers.
■ Format is csv with no header.
■ The schema of the customers record is:
◆ customer_customer_id int,
◆ customer_fname string,
◆ customer_lname string,
◆ customer_email string,
◆ customer_password string,
◆ customer_street string,
◆ customer_city string,
◆ customer_state string,
◆ customer_zipcode string
■ All the order records are stored at dataset/retail_db/orders. Format is csv.
■ Format is csv with no header.
■ The schema of the orders records is:
◆ order_id int,
◆ order_date string,
◆ order_customer_id int,
◆ order_status string
Output requirements
■ Find out total number of orders placed by each customer in the year 2014
■ Order status should be COMPLETE
■ There must be only one output file
■ Output file must not be compressed and the format should be orc.
■ Place the result data in HDFS directory dataset/result/scenario18/solution
■ Output should only contain customer_fname, customer_lname, orders_count
■ Output should be sorted by orders_count in descending order.
■ Aunque se ordene el DataFrame antes de escribirlo a HDFS, a la hora de leer puede que el resultado no esté en orden. 
Cuando se escribe el DataFrame los ficheros resultantes están ordenados de acuerdo al nombre. 
Sin embargo, cuando se leen los ficheros para meterlos en el DataFrame, no se puede predecir, qué ficheros se leerán primero.
*/
/*
Planteamientos:
Schema start in: "_c0"
read customers csv inferSchema with no header.
read orders csv inferSchema with no header.
join customers vs orders customer_customer_id == order_customer_id
select customer_customer_id customer_fname customer_lname order_id order_date order_status
filter year order_date == 2014 and order_status == "COMPLETE"
groupBy customer_customer_id, count(*) alias orders_count
sorted by orders_count desc()
select customer_fname, customer_lname, orders_count
write orc not compress
one output file coalesce( 1 )
■ Aunque se ordene el DataFrame antes de escribirlo a HDFS, a la hora de leer puede que el resultado no esté en orden. 
Cuando se escribe el DataFrame los ficheros resultantes están ordenados de acuerdo al nombre. 
Sin embargo, cuando se leen los ficheros para meterlos en el DataFrame, no se puede predecir, qué ficheros se leerán primero.
*/
/*
ls -l /tmp/dataset/retail_db/customers
cat /dataset/retail_db/customers/part-m-00000 | more
ls -l /tmp/dataset/retail_db/orders
cat /dataset/retail_db/orders/part-m-00000 | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val fileInOne = "/devsh_loudacre/customers"
val fileInTwo = "/devsh_loudacre/orders"
val filenameOut = "/devsh_loudacre/OUT/ordcus"

val DFCusIn = ( spark. read.  option( "inferSchema", "true" ). csv( fileInOne ) ) 
DFCusIn.printSchema

val DFOrdIn = ( spark. read.  option( "inferSchema", "true" ). csv( fileInTwo ) )
DFOrdIn.printSchema

val DFIn = ( DFCusIn. join( DFOrdIn, DFOrdIn.order_customer_id == DFCusIn.customer_customer_id ) )
DFIn.printSchema

val DFOut = ( 
DFIn.
filter ( year( $"order_date" ) === 2014 && $"order_status" === "COMPLETE" ).
groupBy( $"customer_customer_id" ). count(). alias( "orders_count" ). 
orderBy( desc( $"orders_count" ) ).
select( $"customer_fname", $"customer_lname", $"orders_count" ).
write.
coalesce( 1 ).
mode( "overwrite", "true" ).
option( "compression". None ).
orc( filenameOut )
)

DFOut. show()

//------------------

//------------------

( spark.read.option( "inferSchema", "true" ).  csv( fileInOne ). 
select( $"customer_customer_id" , $"customer_fname" , $"customer_lname" , $"customer_email" , $"customer_password" , $"customer_street" , $"customer_city" , $"customer_state" , $"customer_zipcode" ). 
createOrReplaceTempView( "customers_view" ) )

( spark.read.option( "inferSchema", "true" ).  csv( fileInTwo ). 
select( $"order_id", $"order_date", $"order_customer_id", $"order_status").
createOrReplaceTempView( "orders_view" ) )

(spark.sql(
"""
SELECT c.customer_customer_id, c.customer_fname, c.customer_lname, count() as orders_count
FROM customers_view c
JOIN orders_view o
ON c.customer_customer_id = o.order_customer_id
WHERE( year(col( "order_date" )) == 2014 & col( "order_status" ) == "COMPLETE" ).
GROUP BY c.customer_customer_id, c.customer_fname, c.customer_lname
ORDER BY orders_count DESC
""").
write.
coalesce( 1 ).
mode( "overwrite", "true" ).
option( "compression". None ).
orc( filenameOut ) )

//------------------

//------------------

val customers_dir = "dataset/retail_db/customers/"
val orders_dir = "dataset/retail_db/orders/"
val out_dir = "dataset/result/scenario18/solution/"
val customers_view = "customers_view"
val orders_view = "orders_view"
spark.read.
csv( customers_dir ).
selectExpr(
"_c0 id", "_c1 customer_fname", "_c2 customer_lname" ).
createOrReplaceTempView( customers_view )
spark.read.
csv( orders_dir ).
filter( "year(_c1) = 2014" ).
filter( "_c3 = 'COMPLETE'" ).
select( $"_c2" as "id" ).
createOrReplaceTempView( orders_view )
spark.sql(
"""
SELECT customer_fname, customer_lname, COUNT(1) orders_count
FROM customers_view
NATURAL JOIN orders_view
GROUP BY id, customer_fname, customer_lname
ORDER BY orders_count DESC
""").
coalesce( 1 ).
write.
mode( "overwrite" ).
option( "compression", "uncompressed" ).
orc( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  orc( out_dir ).  show( 6 )

//--------------------------------------EndScenario 18.--------------------------------------

//--------------------------------------StartScenario 19.--------------------------------------
/*
Data description
■ All the product records are stored at dataset/retail_db/products
■ All the category records are stored at dataset/retail_db/categories
■ All the order items records are stored at dataset/retail_db/order_items
■ Data is in text format
■ The schema of the products records is:
◆ product_id int,
◆ product_category_id int,
◆ product_name string, 
◆ product_description string,
◆ product_price double,
◆ product_image string,
■ The schema of the categories records is:
◆ category_id int,
◆ category_department_id int,
◆ category_name string
■ The schema of the order_items records is:
◆ order_item_id int,
◆ order_item_order_id int,
◆ order_item_product_id int,
◆ order_item_quantity tinyint,
◆ order_item_subtotal double,
◆ order_item_product_price double
Output requirements
■ Get top five selling products in "Accessories" category
■ Place the result data in HDFS directory dataset/result/scenario19/solution
■ Save the output as text file
■ Use "|" as field separator
■ Ouput data should contain columns category_name, product_name, and product_revenue.
■ product_revenue should be rounded to 2 decimals.
*/
/*
Planteamientos:
Schema start in: "_c0"
read in text format no header
join products, categories, category_id == product_category_id
join products, order_items product_id == order_item_product_id
filter "Accessories" category 
order by desc category top five 
product_revenue rounded to 2 decimals.
select category_name, product_name, and product_revenue
write text file "|" separator
*/
/*
ls -l /tmp/dataset/retail_db/products
cat /tmp/dataset/retail_db/products/part-m-00000 | more
ls -l /tmp/dataset/retail_db/categories
cat /tmp/dataset/retail_db/categories/part-m-00000 | more
ls -l /tmp/dataset/retail_db/order_items
cat /tmp/dataset/retail_db/order_items/part-m-00000 | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val fileInOne = "/devsh_loudacre/order_items"
val fileInTwo = "/devsh_loudacre/products"
val fileInThree = "/devsh_loudacre/categories"

val filenameOut = "/devsh_loudacre/OUT/catprdord"

( spark. read. option( "inferSchema", "true" ).  csv( fileInOne ). select( $"order_item_id", $"order_item_order_id", $"order_item_product_id", $"order_item_quantity", $"order_item_subtotal", $"order_item_product_price" ).  createOrReplaceTempView( "order_items_view" ) )

( spark. read. option( "inferSchema", "true" ). csv( fileInTwo ). select( $"product_id", $"product_category_id", $"product_name", $"product_description", $"product_price", $"product_image", 
). createOrReplaceTempView( "products_view" ) )

( spark. read. option( "inferSchema", "true" ). csv( fileInThree ). select( $"category_id", $"category_department_id", $"category_name" ).  createOrReplaceTempView( "categories_view" ) )

( spark.sql (
"""
SELECT c.category_name, p.product_name, ROUND( ( o.order_item_subtotal - ( o.order_item_quantity * o.order_item_product_price ) ) , 2) as product_revenue
LIMIT (5) 
FROM categories_view c
JOIN products_view p ON c.category_id == p.product_category_id
JOIN order_items o ON product_id == order_item_product_id
WHERE c.category_name = "Accessories"
ORDER BY product_revenue DESC
"""
).
selectExpr( "concat_ws('|', * ) value" ).
write.
mode( "overwrite" ).
text( filenameOut )
)

//------------------

val fileInOne = "/devsh_loudacre/order_items"
val fileInTwo = "/devsh_loudacre/products"
val fileInThree = "/devsh_loudacre/categories"

val filenameOut = "/devsh_loudacre/OUT/catprdord"

"""
 toDF(*cols) Returns a new class:DataFrame that with new specified column names
"""
val DFOrdIn = ( spark. read. option( "inferSchema", "true" ).  csv( fileInOne ). toDF( $"order_item_id", $"order_item_order_id", $"order_item_product_id", $"order_item_quantity", $"order_item_subtotal", $"order_item_product_price" ) )
DFOrdIn.printSchema

val DFPrdIn = ( spark. read. option( "inferSchema", "true" ). csv( fileInTwo ).toDF( $"product_id", $"product_category_id", $"product_name", $"product_description", $"product_price", $"product_image", ) )
DFPrdIn.printSchema

val DFCatIn = ( spark. read. option( "inferSchema", "true" ). csv( fileInThree ). toDF ( $"category_id", $"category_department_id", $"category_name" ) )
DFCatIn.printSchema

val DFIn = ( DFCatIn.join ( DFPrdIn, DFPrdIn.product_category_id == DFCatIn.category_id ).join ( DFOrdIn, DFOrdIn.order_item_product_id == DFPrdIn.order_items product_id ) )
DFIn.printSchema

val DFOut = ( 
DFIn.

filter ( $"category_name" === "Accessories" ).
select( $"category_name", $"product_name", ( $"order_item_subtotal" - ( $"order_item_quantity" * $"order_item_product_price" ) ).  format_number( $"product_revenue", 2 ). alias( $"product_revenue" ) ).
orderBy( desc( "product_revenue" ) ).
selectExpr( "concat_ws('|', * ) value" ).
write.
mode( "overwrite" ).
text( filenameOut )
)

DFOut. show()

//------------------

//------------------

val products_dir = "dataset/retail_db/products/"
val categories_dir = "dataset/retail_db/categories/"
val order_items_dir = "dataset/retail_db/order_items/"
val out_dir = "dataset/result/scenario19/solution/"
val products_view = "products_view"
val categories_view = "categories_view"
val order_items_view = "order_items_view"
spark.read.
csv( products_dir ).
select(
$"_c0" as "product_id",
$"_c1" as "category_id",
$"_c2" as "product_name" ).
createOrReplaceTempView( products_view )
spark.read.
csv( categories_dir ).
select(
$"_c0" as "category_id",
$"_c2" as "category_name" ).
filter( "category_name = 'Accessories'" ).
createOrReplaceTempView( categories_view )
spark.read.
csv( order_items_dir ).
select(
$"_c2" as "product_id",
$"_c4" as "subtotal" ).
createOrReplaceTempView( order_items_view )
spark.sql(
"""
SELECT
category_name, product_name,
round( sum( subtotal ), 2) product_revenue
FROM products_view
NATURAL JOIN categories_view
NATURAL JOIN order_items_view
GROUP BY product_id, category_name, product_name
ORDER BY product_revenue DESC
LIMIT 5
""").
selectExpr( "concat_ws('|', *) value" ).
write.
mode( "overwrite" ).
text( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read. text( out_dir ).  orderBy( "value" ).  show( 10, false )

//--------------------------------------EndScenario 19.--------------------------------------

//--------------------------------------StartScenario 20.--------------------------------------
/*
Data description
■ All the customer records are stored at dataset/retail_db/customers.
■ Format is csv with no header.
■ The schema of the customers record is:
◆ customer_customer_id int,
◆ customer_fname string,
◆ customer_lname string,
◆ customer_email string,
◆ customer_password string,
◆ customer_street string,
◆ customer_city string,
◆ customer_state string,
◆ customer_zipcode string
■ All the order records are stored at dataset/retail_db/orders.
■ Format is csv with no header.
■ The schema of the orders records is:
◆ order_id int,
◆ order_date string,
◆ order_customer_id int,
◆ order_status string
■ All the order items records are stored at dataset/retail_db/order_items
■ Format is csv with no header.
■ The schema of the order_items records is:
◆ order_item_id int,
◆ order_item_order_id int,
◆ order_item_product_id int,
◆ order_item_quantity tinyint,
◆ order_item_subtotal double,
◆ order_item_product_price double
Output requirements
■ Get customers who have placed succesuful (COMPLETE) orders of revenue more than 500
■ Use parquet files for the output files
■ Use snappy compression
■ Place the result in HDFS directory dataset/result/scenario20/solution
■ Output should only contain customer_fname, customer_lname, order_revenue
*/
/*
Planteamientos:
Schema start in: "_c0"
read csv no header
join customers, order_items, orders
filter "COMPLETE" col( "order_status" ) and revenue > 500
save escribe directamente en formato parquet con compresión snappy.
customer_fname, customer_lname, order_revenue
*/
/*
ls -l /tmp/dataset/retail_db/customers
cat /tmp/dataset/retail_db/customers/part-m-00000 | more
ls -l /tmp/dataset/retail_db/orders
cat /tmp/dataset/retail_db/orders/part-m-00000 | more
ls -l /tmp/dataset/retail_db/order_items
cat /tmp/dataset/retail_db/order_items/part-m-00000 | more

hdfs dfs -put 
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -ls /devsh_loudacre/
hdfs dfs -rm -R /devsh_loudacre/OUT/
*/

//--------------------------------------scala

val fileInOne = "/devsh_loudacre/customers"
val fileInTwo = "/devsh_loudacre/orders"
val fileInThree = "/devsh_loudacre/order_items"

val filenameOut = "/devsh_loudacre/OUT/"

val DFCusIn = ( spark. read.  option( "inferShema", "true" ).  csv( fileInOne ).  select( col( "customer_customer_id" ), col( "customer_fname" ), col( "customer_lname" ), col( "customer_email" ), col( "customer_password" ), col( "customer_street" ), col( "customer_city" ), col( "customer_state" ), col( "customer_zipcode" ) ).  createOrReplaceTempView( "customers_view" ) )

val DFOrdIn = ( spark. read.  option( "inferShema", "true" ).  csv( fileInTwo ).  select( col( "order_id" ), col( "order_date" ), col( "order_customer_id" ), col( "order_status" ) ).  createOrReplaceTempView( "orders_view" ) )

val DFItemsIn = ( spark. read.  option( "inferShema", "true" ).  csv( fileInThree ).  select( col( "order_item_id" ), col( "order_item_order_id" ), col( "order_item_product_id" ), col( "order_item_quantity" ), col( "order_item_subtotal" ), col( "order_item_product_price" ) ).  createOrReplaceTempView( "order_items_view" ) )

val DFIn = ( 
DFCusIn. join(DFOrdIn, DFOrdIn.order_customer_id == DFCusIn.customer_customer_id )
DFOrdIn. join(DFItemsIn, DFItemsIn.order_item_order_id == DFOrd.order_id )
)
DFIn.printSchema

val DFOut = ( 
DFIn.
filter( col( "order_status" ) == "COMPLETE" & ( col( "order_revenue" ) > 500 ) ).
select( col ("customer_fname" ).  col ("customer_lname" ).  col ("order_revenue" ) ).
write.
mode( "overwrite", "true" ).
option( "inferSchema", "true" ).
save( filenameOut )
)

DFOut.show()

//------------------

//------------------

"""
 toDF(*cols) Returns a new class:DataFrame that with new specified column names
"""

( spark. read.  option( "inferShema", "true" ).  csv( fileInOne ).  toDF( col( "customer_customer_id" ), col( "customer_fname" ), col( "customer_lname" ), col( "customer_email" ), col( "customer_password" ), col( "customer_street" ), col( "customer_city" ), col( "customer_state" ), col( "customer_zipcode" ) ) )

( spark. read.  option( "inferShema", "true" ).  csv( fileInTwo ).  toDF( col( "order_id" ), col( "order_date" ), col( "order_customer_id" ), col( "order_status" ) ) )

( spark. read.  option( "inferShema", "true" ).  csv( fileInThree ).  toDF( col( "order_item_id" ), col( "order_item_order_id" ), col( "order_item_product_id" ), col( "order_item_quantity" ), col( "order_item_subtotal" ), col( "order_item_product_price" ) ) )

(spark.sql(
"""
SELECT "customer_fname", "customer_lname", "order_revenue"
FROM customers_view c
JOIN orders_view o ON o.order_customer_id == c.customer_customer_id
JOIN order_items_view oi ON oi.order_item_order_id == o.order_id
WHERE "order_status" == "COMPLETE" AND "order_revenue" > 500
"""
).
write.
mode( "overwrite", "true" ).
option( "inferSchema", "true" ).
save( filenameOut )
)

//------------------

val orders_dir = "dataset/retail_db/orders/"
val order_items_dir = "dataset/retail_db/order_items/"
val customers_dir = "dataset/retail_db/customers/"
val out_dir = "dataset/result/scenario20/solution/"
spark.read.
csv( orders_dir ).
filter( $"_c3" === "COMPLETE" ).
select(
$"_c0" alias "order_id",
$"_c2" alias "customer_id").
createOrReplaceTempView( "orders_view" )
spark.read.
option( "inferSchema", true ).
csv( order_items_dir ).
select(
$"_c1" alias "order_id",
$"_c4" alias "subtotal").
createOrReplaceTempView( "order_items" )
spark.read.
csv( customers_dir ).
select(
$"_c0" alias "customer_id",
$"_c1" alias "customer_fname",
$"_c2" alias "customer_lname" ).
createOrReplaceTempView( "customers" )
spark.sql(
"""
SELECT
customer_fname,
customer_lname,
ROUND( SUM(subtotal), 2 ) order_revenue
FROM orders_view o
NATURAL JOIN order_items
NATURAL JOIN customers
GROUP BY order_id, customer_id, customer_fname, customer_lname
HAVING SUM(subtotal) > 500
""").
write.
mode( "overwrite" ).
option( "compression", "snappy" ).
save( out_dir )

//------------------

//------------------ Comprobación

val filenameIn = filenameOut
spark.read.  load( out_dir ).  orderBy( "customer_lname", "customer_fname" ).  show( 10 )

//--------------------------------------EndScenario 20.--------------------------------------

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