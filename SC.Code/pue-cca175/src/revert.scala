import org.apache.spark.rdd.RDD
def printRDD[T]( rdd: RDD[T], message: String = "RDD:"): Unit = {
  println( message + " " + "*"*25)
  rdd.collect.foreach(println)
  println()
}

val orderfile = "orderskus.txt"
val ordersRDD = sc.textFile("orderskus.txt").
val ordersRDD = sc.textFile("/devsh_loudacre/orderskus.txt").
  map( _.split(' ') ).
  map( fields => (fields(0),fields(1)) ).
  flatMapValues( _.split(':') )
printRDD( ordersRDD, "ordersRDD" )


val idSkuRDD = ordersRDD.
  groupByKey.
  mapValues( _.mkString(":") ).
  sortByKey().
  map( t => t._1 + " " + t._2 )
printRDD( idSkuRDD, "idSkuRDD" )


val skuIdRDD = ordersRDD.
  map( _.swap).
  groupByKey.
  mapValues( _.mkString(":") ).
  sortByKey().
  map( t => t._1 + " " + t._2 )
printRDD( skuIdRDD, "skuIdRDD" )
