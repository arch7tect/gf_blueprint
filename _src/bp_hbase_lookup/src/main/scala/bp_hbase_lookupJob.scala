package ru.neoflex.meta.etl2.spark

import java.sql.{CallableStatement, Timestamp}
import java.util.Date

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.{JdbcRDD, RDD, EmptyRDD}
import ru.neoflex.meta.etl2.ETLJobConst._
import ru.neoflex.meta.etl2.{ETLJobBase, JdbcETLContext, OracleSequence}
import scala.collection.{JavaConversions, immutable}
import ru.neoflex.meta.etl.functions._
import org.apache.spark.sql.functions._



class bp_hbase_lookupJob extends ETLJobBase {

  override def getApplicationName: String = {
    "bp_hbase_lookup"
  }
  
  def run(spark: SparkSession): Any = {
	    spark.udf.register("LONG2BINARY", new Long2BinaryUdfClass)
	    spark.udf.register("BINARY2BOOLEAN", new Binary2BooleanUdfClass)
	    spark.udf.register("BINARY2LONG", new Binary2LongUdfClass)
	    spark.udf.register("BINARY2STRING", new Binary2StringUdfClass)
	    spark.udf.register("HBASE_LOOKUP", new HBaseLookupUdfClass )
    	val Expression_0 = getExpression_0(spark)


    	val Spark_SQL_1 = getSpark_SQL_1(spark, Expression_0)


    	Local_3(spark, Spark_SQL_1)
 
  }

  def getExpression_0(spark: SparkSession): Dataset[Expression_0Schema] = {
  	import spark.implicits._
    val expr = {
        (10L to 21L).map(id=>Map("id"->id.asInstanceOf[AnyRef]))
    }
    spark.createDataset(expr.map(m => Expression_0Schema(
        id = m.getOrElse("id", null).asInstanceOf[java.lang.Long]        
    )))
  }

  def getSpark_SQL_1(spark: SparkSession, Expression_0: Dataset[Expression_0Schema]) = {
    import spark.implicits._
    	
    		Expression_0.createOrReplaceTempView("seq")
    	
    			
    	val sqlText = s"""with seqlk as (
    	                    select 
    	                      s.id as id, 
    	                      HBASE_LOOKUP('address', '0:cust_id;0:address;0:main', LONG2BINARY(s.id)) as arr
    	                    from seq s
    	                  )
    	                  select 
    	                      lk.id,
    	                      BINARY2LONG(lk.arr[0]) as cust_id,
    	                      BINARY2STRING(lk.arr[1]) as address,
    	                      BINARY2BOOLEAN(lk.arr[2]) as main
    	                  from seqlk lk"""	
    	val queryResult = spark.sql(s"${sqlText}")
    	
    		
    	queryResult.as[Spark_SQL_1Schema]      
  }

  def Local_3(spark: SparkSession, ds: Dataset[Spark_SQL_1Schema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/blueprint/hbase_lookup_result"""
    logger.logInfo(s"LocalTarget Local_3 fileName: ${fileName}")   
    val dsOut = ds  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("json")
    .save(fileName)    
 
  }


    override def initBuilder(builder: SparkSession.Builder): SparkSession.Builder = {
        builder.enableHiveSupport()
    } 

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class Expression_0Schema(
    var id: java.lang.Long
) extends Serializable
case class Spark_SQL_1Schema(
) extends Serializable

}

class Long2BinaryUdfClass extends scala.Function1[Long, Array[Byte]] with Serializable {
  def apply(value: Long): Array[Byte] = {
    import org.apache.hadoop.hbase.util.Bytes
    Bytes.toBytes(value)
  }
}
class Binary2BooleanUdfClass extends scala.Function1[Array[Byte], Boolean] with Serializable {
  def apply(value: Array[Byte]): Boolean = {
    import org.apache.hadoop.hbase.util.Bytes
    Bytes.toBoolean(value)
  }
}
class Binary2LongUdfClass extends scala.Function1[Array[Byte], Long] with Serializable {
  def apply(value: Array[Byte]): Long = {
    import org.apache.hadoop.hbase.util.Bytes
    Bytes.toLong(value)
  }
}
class Binary2StringUdfClass extends scala.Function1[Array[Byte], String] with Serializable {
  def apply(value: Array[Byte]): String = {
    import org.apache.hadoop.hbase.util.Bytes
    Bytes.toString(value)
  }
}
class HBaseLookupUdfClass extends scala.Function3[String, String, Array[Byte], Seq[Array[Byte]]] with Serializable {

    import org.apache.hadoop.hbase.HBaseConfiguration
    import org.apache.hadoop.hbase.TableName
    import org.apache.hadoop.hbase.client.ConnectionFactory
    import org.apache.hadoop.hbase.client.Get
    import org.apache.hadoop.hbase.util.Bytes
    import org.apache.hadoop.hbase.client.Connection
    import org.apache.hadoop.hbase.client.Table
    import org.slf4j.LoggerFactory
    
  @transient var connection = null: Connection
  @transient var tables = null: scala.collection.mutable.HashMap[String, Table]
  @transient lazy val logger = {LoggerFactory.getLogger(getClass)}

  def apply(tableName: String, columnNames: String, key: Array[Byte]): Seq[Array[Byte]] = {
    val columns = columnNames.split("[,;]").map(_.split("[:.]").map(_.trim()))
    logger.info("key: " + key.mkString(""))
    logger.info("columns: [" + columns.map("(" + _.mkString(", ") + ")").mkString("; ") + "]")
    val table = getTable(tableName)
    val get = new Get(key)
    columns.foreach(c=>get.addColumn(Bytes.toBytes(c(0)), Bytes.toBytes(c(1))))
    val result = table.get(get)
    columns.map(c=>result.getValue(Bytes.toBytes(c(0)),Bytes.toBytes(c(1))))
  }
  
  private def getConnection(): Connection = {
      if (connection == null) {
        logger.info("new HBase connection")
        val config = HBaseConfiguration.create()
        connection = ConnectionFactory.createConnection(config)
      }
      connection
  }

  private def getTable(tableName: String): Table = {
      if (tables == null) {
        tables = new scala.collection.mutable.HashMap[String, Table]
      }
      tables.getOrElseUpdate(tableName, {
          val parts = tableName.split("[.]", 2)
          val (schema, table) = if (parts.size == 2) {(parts(0), parts(1))} else {("default", parts(0))}
          logger.info("new Table: " + schema + "." + table)
          getConnection().getTable(TableName.valueOf(schema, table))
      })
  }

  sys.ShutdownHookThread {
    logger.info("exiting")
    if (tables != null) {
        for ((tableName, table)<-tables) {
            table.close()
        }
        tables = null
    }
    if (connection != null) {
        connection.close()
        connection = null
    }
  }
}

object bp_hbase_lookupJob {
   def main(args: Array[String]): Unit = {
     new bp_hbase_lookupJob().sparkMain(args)
  }
}

