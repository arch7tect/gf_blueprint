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



class bp_init_hbase_addressJob extends ETLJobBase {

  override def getApplicationName: String = {
    "bp_init_hbase_address"
  }
  
  def run(spark: SparkSession): Any = {
    	val Expression_0 = getExpression_0(spark)


    	HBase_1(spark, Expression_0)
 
  }

  def getExpression_0(spark: SparkSession): Dataset[Expression_0Schema] = {
  	import spark.implicits._
    val expr = {
        (0L to 20000L).map { i =>
            Map(
                "id" -> i.asInstanceOf[AnyRef], 
                "cust_id" -> i.asInstanceOf[AnyRef], 
                "address" -> s"""Address(${i}) of ${i%10000}""".asInstanceOf[AnyRef],
                "main" -> (i%2 == 0).asInstanceOf[AnyRef]
            )
        }
    }
    spark.createDataset(expr.map(m => Expression_0Schema(
        id = m.getOrElse("id", null).asInstanceOf[java.lang.Long],        
        cust_id = m.getOrElse("cust_id", null).asInstanceOf[java.lang.Long],        
        address = m.getOrElse("address", null).asInstanceOf[java.lang.String],        
        main = m.getOrElse("main", null).asInstanceOf[java.lang.Boolean]        
    )))
  }

def HBase_1(spark: SparkSession, ds: Dataset[Expression_0Schema]): Unit = {

	def catalog = s"""{
	                  |"table":{"namespace":"default", "name":"address"},
	                  |"rowkey":"id",
	                  |"columns":{
	                      |"id":{"cf":"rowkey", "col":"id", "type":"${ds.schema("id").dataType.typeName}"},        
	                      |"cust_id":{"cf":"0", "col":"cust_id", "type":"${ds.schema("cust_id").dataType.typeName}"},        
	                      |"address":{"cf":"0", "col":"address", "type":"${ds.schema("address").dataType.typeName}"},        
	                      |"main":{"cf":"0", "col":"main", "type":"${ds.schema("main").dataType.typeName}"}        
	                  |}
	              |}""".stripMargin
	ds
		.write
		.options(
			Map("catalog" -> catalog, "newtable" -> "5")
		)
		.format("org.apache.spark.sql.execution.datasources.hbase")
  		.save()
  }

 

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class Expression_0Schema(
    var id: java.lang.Long,
    var cust_id: java.lang.Long,
    var address: java.lang.String,
    var main: java.lang.Boolean
) extends Serializable

}


object bp_init_hbase_addressJob {
   def main(args: Array[String]): Unit = {
     new bp_init_hbase_addressJob().sparkMain(args)
  }
}

