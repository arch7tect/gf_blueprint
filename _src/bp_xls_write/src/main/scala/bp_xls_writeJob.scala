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



class bp_xls_writeJob extends ETLJobBase {

  override def getApplicationName: String = {
    "bp_xls_write"
  }
  
  def run(spark: SparkSession): Any = {
    	val Expression_0 = getExpression_0(spark)


    	xls_target(spark, Expression_0)
 
  }

  def getExpression_0(spark: SparkSession): Dataset[Expression_0Schema] = {
  	import spark.implicits._
    val expr = {
        Array(
            Map("id" -> new java.math.BigDecimal(1), "name" -> "1"),
            Map("id" -> new java.math.BigDecimal(2), "name" -> "2") 
        )
    }
    spark.createDataset(expr.map(m => Expression_0Schema(
        id = m.getOrElse("id", null).asInstanceOf[java.math.BigDecimal],        
        name = m.getOrElse("name", null).asInstanceOf[java.lang.String]        
    )))
  }

  def xls_target(spark: SparkSession, ds: Dataset[Expression_0Schema]): Unit = {
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file://"
      s"${fs}/temp/blueprint/xls_test.xls"
    }
    logger.logInfo(s"CSVTarget xls_target path: ${path}")
    
    
    ds.write
      .format("com.crealytics.spark.excel")       .option("useHeader", """true""")
      .mode("overwrite")
      .option("timestampFormat", """yyyy-MM-dd hh:mm:ss""")
      .option("dateFormat", """yyyy-MM-dd""")
      .save(path)
  }

 

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class Expression_0Schema(
    var id: java.math.BigDecimal,
    var name: java.lang.String
) extends Serializable

}


object bp_xls_writeJob {
   def main(args: Array[String]): Unit = {
     new bp_xls_writeJob().sparkMain(args)
  }
}

