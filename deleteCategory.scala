def deleteCategory(keyspace: String, table: String, filter: String): Long = {
  import com.datastax.spark.connector.cql.CassandraConnector
  val dseConnector = CassandraConnector(sc)
  val documentDF = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> table, "keyspace" -> keyspace))
    .load
  documentDF.createOrReplaceTempView("documents")
  val deleteDocDF = spark.sql("SELECT * FROM documents "+filter)
  val deletedCount = deleteDocDF.count()
  deleteDocDF.foreachPartition(partition => {
    dseConnector.withSessionDo(session =>
      partition.foreach{ log =>
          val delete = s"DELETE FROM "+keyspace+"."+table+" where <primary key>="+log.getString(0) +";"
          session.execute(delete)
      })
  })
  deletedCount
}
