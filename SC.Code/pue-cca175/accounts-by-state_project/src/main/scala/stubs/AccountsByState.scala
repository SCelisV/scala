package stubs

import org.apache.spark.sql.SparkSession

object AccountsByState {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: stubs.AccountByState <state-code>")
      System.exit(1)
    }
 
    val stateCode = args(0)
  
    // Create a SparkSession object
    val spark = SparkSession.builder.getOrCreate()

    // Change the application log level from INFO (the default) to WARN
    spark.sparkContext.setLogLevel("WARN")
  
    // Leer la tabla Hive
    val accountsDF = spark.read.table("devsh.accounts")

    // seleccionar los registros que coincidan con el argumento pasado
    // save el resultado
    // hdfs dfs -rm -R /devsh_loudacre/accounts_by_stateCA/

    accountsDF.where(accountsDF("state") === stateCode).write.mode("overwrite").save("/devsh_loudacre/accounts_by_state" + stateCode)

    // Otra forma
    /*
    val stateAccountsDF = accountsDF.where(accountsDF("state") === stateCode)
    stateAccountsDF.write.mode("overwrite").save("/devsh_loudacre/accounts_by_state/" + stateCode)
    */

    //stop the Spark session:
    spark.stop()

  }
}

/*
Revisar los ficheros en:

#hdfs dfs -ls /devsh_loudacre/
#hdfs dfs -ls /devsh_loudacre/accounts_by_state + stateCode
#hdfs dfs -ls /devsh_loudacre/accounts_by_stateCA/
*/