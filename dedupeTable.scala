def dedupeTable(keyspace: String, table: String, columns: Array[String], filter: String): Long = {
  import com.datastax.spark.connector.cql.CassandraConnector
  val dseConnector = CassandraConnector(sc)
  val documentDF = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> table, "keyspace" -> keyspace))
    .load
//TODO this category filter can be done using SPARK SQL, and you should be able to just pass it a SQL "where clause"
  documentDF.createOrReplaceTempView("documents")
  val docDF = spark.sql("SELECT * FROM documents "+filter)
  val documentDF_temp = docDF.withColumn("comparable", concat_ws(",", (columns.map(c => col(c)): _*)))
  val countsDF = documentDF_temp.rdd.map(rec => (rec(9),1)).reduceByKey(_+_).map(rec => (rec._1.toString, rec._2)).toDF("Compared", "Count")
  val joinedDF = documentDF_temp.join(countsDF, documentDF_temp("comparable")===countsDF("Compared"),
    "left_outer").select(documentDF_temp("doc_id"), documentDF_temp("comparable"), countsDF("Count"))
  val result = joinedDF.where("Count > 1")
  val originals = result.groupBy("comparable").agg(first("doc_id").as("doc_id"), min("count").as("count"))
  val to_Delete = (result.select("doc_id")).except(originals.select("doc_id"))
  val deletedCount = result.count()
  to_Delete.foreachPartition(partition => {
    dseConnector.withSessionDo(session =>
      partition.foreach{ log =>
          val delete = s"DELETE FROM "+keyspace+"."+table+" where doc_id="+log.getString(0) +";"
          session.execute(delete)
      })
  })
  deletedCount
}