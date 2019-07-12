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

import org.drools.compiler.compiler.io.memory.MemoryFileSystem
import org.kie.api.KieBase
import org.kie.api.KieServices
import org.kie.api.builder.Message.Level
import org.kie.api.io.ResourceConfiguration
import org.kie.api.io.ResourceType
import org.kie.internal.builder.DecisionTableConfiguration
import org.kie.internal.builder.DecisionTableInputType
import org.kie.internal.builder.KnowledgeBuilderFactory

object GlobalDrools_1 {
    var kbase: KieBase = null
}

class bp_drools_1Job extends ETLJobBase {

  override def getApplicationName: String = {
    "bp_drools_1"
  }
  
  def run(spark: SparkSession): Any = {
    	val Expression_0 = getExpression_0(spark)


    	val Drools_1 = getDrools_1(spark, Expression_0)


    	Local_2(spark, Drools_1)
 
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

  def getDrools_1(spark: SparkSession, Expression_0: Dataset[Expression_0Schema]) = {
    import spark.implicits._
    Expression_0.mapPartitions(partition => {
        if(GlobalDrools_1.kbase == null) {
            val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())
            val kieServices = KieServices.Factory.get()
            val kfs = kieServices.newKieFileSystem()
            val kieRepository = kieServices.getRepository()
        
            {
                val fileUrl = s"""${_defaultFS}/tmp/blueprint/droolsTypes.drl"""      
                val ins = fs.open(new Path(fileUrl))
                val res = kieServices.getResources().newInputStreamResource(ins)
                res.setResourceType(ResourceType.DRL)
                res.setSourcePath(s"""/tmp/blueprint/droolsTypes.drl""")
                kfs.write(res)
                ins.close()
            }
            {
                val fileUrl = s"""${_defaultFS}/tmp/blueprint/droolsRules.xls"""      
                val ins = fs.open(new Path(fileUrl))
                val res = kieServices.getResources().newInputStreamResource(ins)
                res.setResourceType(ResourceType.DTABLE)
                ;{
                    val resourceConfiguration = KnowledgeBuilderFactory.newDecisionTableConfiguration()
                    resourceConfiguration.setInputType(DecisionTableInputType.XLS)
                    res.setConfiguration(resourceConfiguration)
                }
                res.setSourcePath(s"""/tmp/blueprint/droolsRules.xls""")
                kfs.write(res)
                ins.close()
            }
            val kieBuilder = kieServices.newKieBuilder(kfs)
            kieBuilder.buildAll()
            if (kieBuilder.getResults().hasMessages(Level.ERROR)) {
                throw new RuntimeException("Build Errors:\n" + kieBuilder.getResults().toString())
            }
            val module = kieBuilder.getKieModule()
            val kieContainer = kieServices.newKieContainer(module.getReleaseId())
            val kieBaseName = kieContainer.getKieBaseNames().iterator().next()
            GlobalDrools_1.kbase = kieContainer.getKieBase(kieBaseName)
        }
        val results = new java.util.ArrayList[Drools_1Schema]()    
        partition.sliding(_slideSize, _slideSize).foreach(slide => {
            val session = GlobalDrools_1.kbase.newKieSession()
            if (_debug) {
                KieServices.Factory.get().getLoggers().newConsoleLogger(session)
            }
            val factType = GlobalDrools_1.kbase.getFactType("ru.neoflex.rules", "InputType");        
            for(row <- slide){
                val obj = factType.newInstance()
                factType.set(obj, "id", row.id)
                factType.set(obj, "name", row.name)
                session.insert(obj)
            }
            session.fireAllRules()  
            val resultFactType = GlobalDrools_1.kbase.getFactType("ru.neoflex.rules", "InputType")    
            val resultsQuery = session.getQueryResults("getRes")
            val resultIterator = resultsQuery.iterator()
            while(resultIterator.hasNext()){
                val r = resultIterator.next()
                val resultObject = r.get("res")
                results.add(Drools_1Schema(    
                    id = resultFactType.get(resultObject, "id").asInstanceOf[java.math.BigDecimal],    
                    name = resultFactType.get(resultObject, "name").asInstanceOf[java.lang.String]            
                ))
            }
            session.dispose()          
        })       
        JavaConversions.asScalaBuffer(results).toIterator      
    })
  }

  def Local_2(spark: SparkSession, ds: Dataset[Drools_1Schema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/blueprint/droolsResult.json"""
    logger.logInfo(s"LocalTarget Local_2 fileName: ${fileName}")   
    val dsOut = ds  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("json")
    .save(fileName)    
 
  }

 

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class Expression_0Schema(
    var id: java.math.BigDecimal,
    var name: java.lang.String
) extends Serializable
case class Drools_1Schema(
    var id: java.math.BigDecimal,
    var name: java.lang.String
) extends Serializable

}


object bp_drools_1Job {
   def main(args: Array[String]): Unit = {
     new bp_drools_1Job().sparkMain(args)
  }
}

