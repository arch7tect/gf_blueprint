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

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types._
import org.drools.compiler.compiler.io.memory.MemoryFileSystem
import org.kie.api.KieBase
import org.kie.api.KieServices
import org.kie.api.builder.Message.Level
import org.kie.api.io.ResourceConfiguration
import org.kie.api.io.ResourceType
import org.kie.internal.builder.DecisionTableConfiguration
import org.kie.internal.builder.DecisionTableInputType
import org.kie.internal.builder.KnowledgeBuilderFactory
import scala.collection.mutable
import scala.reflect.runtime.universe._

object GlobalDrools_Apply_Strategy {
    var kbase: KieBase = null
}

class BELFIUS_applicationsJob extends ETLJobBase {

  override def getApplicationName: String = {
    "BELFIUS_applications"
  }
  
  def run(spark: SparkSession): Any = {
    	val DS_XML_REQ = getDS_XML_REQ(spark)


    	val Drools_Apply_Strategy = getDrools_Apply_Strategy(spark, DS_XML_REQ)


    	val DS_AGGR_RESP = getDS_AGGR_RESP(spark, Drools_Apply_Strategy)


    	val PRODUCT_CATALOG = getPRODUCT_CATALOG(spark)


    	val Join_Catalog = getJoin_Catalog(spark, DS_AGGR_RESP, PRODUCT_CATALOG)


    	CSV_FUSION_RISK(spark, Join_Catalog)

    	val XML_TO_CSM = getXML_TO_CSM(spark, Drools_Apply_Strategy)


    	DS_XML_RESP(spark, XML_TO_CSM)
 
  }

  def getDS_XML_REQ(spark: SparkSession): Dataset[DS_XML_REQSchema] = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
   	import org.apache.spark.sql.types._
    val schema = newProductEncoder(typeTag[DS_XML_REQSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/user/applications/*"""
    }

    try {
      var ds = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", """application""")
      .option("charset", """UTF-8""")
      .option("samplingRatio", """1.0""")
      .option("excludeAttribute", """false""")
      .option("treatEmptyValuesAsNulls", """false""")
      .option("mode", """PERMISSIVE""")
      .option("columnNameOfCorruptRecord", """_corrupt_record""")
      .option("attributePrefix", """_""")
      .option("valueTag", """_VALUE""")
      .option("ignoreSurroundingSpaces", """false""")
      .load(path)
      ds.select(
            $"ApplicationID" as "ApplicationID" ,
            $"AssociatedModelName" as "AssociatedModelName" ,
            $"ProductType" as "ProductType" ,
            $"ProposedProduct" as "ProposedProduct" ,
            $"StrategyName" as "StrategyName" ,
            $"StrategyVersion" as "StrategyVersion" ,
            $"Customer.BirthDate" as "Customer_BirthDate" ,
            $"Customer.FirstName" as "Customer_FirstName" ,
            $"Customer.Gender" as "Customer_Gender" ,
            $"Customer.INN" as "Customer_INN" ,
            $"Customer.LastName" as "Customer_LastName" ,
            $"Customer.PlaceOfBirth" as "Customer_PlaceOfBirth" ,
            $"Customer.Snils" as "Customer_Snils" ,
            $"Customer.citizenshipId" as "Customer_citizenshipId" ,
            $"Customer.clientId" as "Customer_clientId" ,
            $"FinancialInformation.A1" as "FinancialInformation_A1" ,
            $"FinancialInformation.A2" as "FinancialInformation_A2" ,
            $"FinancialInformation.A3" as "FinancialInformation_A3" ,
            $"FinancialInformation.A4" as "FinancialInformation_A4" ,
            $"FinancialInformation.AverageTotalAssets" as "FinancialInformation_AverageTotalAssets" ,
            $"FinancialInformation.Currency" as "FinancialInformation_Currency" ,
            $"FinancialInformation.L1" as "FinancialInformation_L1" ,
            $"FinancialInformation.L2" as "FinancialInformation_L2" ,
            $"FinancialInformation.L3" as "FinancialInformation_L3" ,
            $"FinancialInformation.L4" as "FinancialInformation_L4" ,
            $"FinancialInformation.Profit" as "FinancialInformation_Profit" ,
            $"FinancialInformation.SalesRevenue" as "FinancialInformation_SalesRevenue" ,
            $"FinancialInformation.WorkingCapital" as "FinancialInformation_WorkingCapital" ,
            $"GeneralInformation.NumMonthsActivity" as "GeneralInformation_NumMonthsActivity" ,
            $"GeneralInformation.NumMonthsSinceReg" as "GeneralInformation_NumMonthsSinceReg" ,
            $"RequestedProduct.AmortizationType" as "RequestedProduct_AmortizationType" ,
            $"RequestedProduct.Amount" as "RequestedProduct_Amount" ,
            $"RequestedProduct.Currency" as "RequestedProduct_Currency" ,
            $"RequestedProduct.InterestRate" as "RequestedProduct_InterestRate" ,
            $"RequestedProduct.Periodicity" as "RequestedProduct_Periodicity" ,
            $"RequestedProduct.Term" as "RequestedProduct_Term" 
        ).as[DS_XML_REQSchema]
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[DS_XML_REQSchema]      }

  }
  def getPRODUCT_CATALOG(spark: SparkSession) = {
  	import spark.implicits._
    import scala.reflect.runtime.universe._
    val fileName = s"""${_defaultFS}"""
    
    spark
    .read 
    .format("json")
    .load(fileName) 		
    .as[PRODUCT_CATALOGSchema]
 
  }

  def getDrools_Apply_Strategy(spark: SparkSession, DS_XML_REQ: Dataset[DS_XML_REQSchema]) = {
    import spark.implicits._
    DS_XML_REQ.mapPartitions(partition => {
        if(GlobalDrools_Apply_Strategy.kbase == null) {
            val fs = org.apache.hadoop.fs.FileSystem.get(new Configuration())
            val kieServices = KieServices.Factory.get()
            val kfs = kieServices.newKieFileSystem()
            val kieRepository = kieServices.getRepository()
        
            val kieBuilder = kieServices.newKieBuilder(kfs)
            kieBuilder.buildAll()
            if (kieBuilder.getResults().hasMessages(Level.ERROR)) {
                throw new RuntimeException("Build Errors:\n" + kieBuilder.getResults().toString())
            }
            val module = kieBuilder.getKieModule()
            val kieContainer = kieServices.newKieContainer(module.getReleaseId())
            val kieBaseName = kieContainer.getKieBaseNames().iterator().next()
            GlobalDrools_Apply_Strategy.kbase = kieContainer.getKieBase(kieBaseName)
        }
        val results = new java.util.ArrayList[Drools_Apply_StrategySchema]()    
        partition.sliding(_slideSize, _slideSize).foreach(slide => {
            val session = GlobalDrools_Apply_Strategy.kbase.newKieSession()
            if (_debug) {
                KieServices.Factory.get().getLoggers().newConsoleLogger(session)
            }
            val factType = GlobalDrools_Apply_Strategy.kbase.getFactType("", "Application");        
            for(row <- slide){
                val obj = factType.newInstance()
                factType.set(obj, "ApplicationID", row.ApplicationID)
                factType.set(obj, "AssociatedModelName", row.AssociatedModelName)
                factType.set(obj, "ProductType", row.ProductType)
                factType.set(obj, "ProposedProduct", row.ProposedProduct)
                factType.set(obj, "StrategyName", row.StrategyName)
                factType.set(obj, "StrategyVersion", row.StrategyVersion)
                factType.set(obj, "Customer_BirthDate", row.Customer_BirthDate)
                factType.set(obj, "Customer_FirstName", row.Customer_FirstName)
                factType.set(obj, "Customer_Gender", row.Customer_Gender)
                factType.set(obj, "Customer_INN", row.Customer_INN)
                factType.set(obj, "Customer_LastName", row.Customer_LastName)
                factType.set(obj, "Customer_PlaceOfBirth", row.Customer_PlaceOfBirth)
                factType.set(obj, "Customer_Snils", row.Customer_Snils)
                factType.set(obj, "Customer_citizenshipId", row.Customer_citizenshipId)
                factType.set(obj, "Customer_clientId", row.Customer_clientId)
                factType.set(obj, "FinancialInformation_A1", row.FinancialInformation_A1)
                factType.set(obj, "FinancialInformation_A2", row.FinancialInformation_A2)
                factType.set(obj, "FinancialInformation_A3", row.FinancialInformation_A3)
                factType.set(obj, "FinancialInformation_A4", row.FinancialInformation_A4)
                factType.set(obj, "FinancialInformation_AverageTotalAssets", row.FinancialInformation_AverageTotalAssets)
                factType.set(obj, "FinancialInformation_Currency", row.FinancialInformation_Currency)
                factType.set(obj, "FinancialInformation_L1", row.FinancialInformation_L1)
                factType.set(obj, "FinancialInformation_L2", row.FinancialInformation_L2)
                factType.set(obj, "FinancialInformation_L3", row.FinancialInformation_L3)
                factType.set(obj, "FinancialInformation_L4", row.FinancialInformation_L4)
                factType.set(obj, "FinancialInformation_Profit", row.FinancialInformation_Profit)
                factType.set(obj, "FinancialInformation_SalesRevenue", row.FinancialInformation_SalesRevenue)
                factType.set(obj, "FinancialInformation_WorkingCapital", row.FinancialInformation_WorkingCapital)
                factType.set(obj, "GeneralInformation_NumMonthsActivity", row.GeneralInformation_NumMonthsActivity)
                factType.set(obj, "GeneralInformation_NumMonthsSinceReg", row.GeneralInformation_NumMonthsSinceReg)
                factType.set(obj, "RequestedProduct_AmortizationType", row.RequestedProduct_AmortizationType)
                factType.set(obj, "RequestedProduct_Amount", row.RequestedProduct_Amount)
                factType.set(obj, "RequestedProduct_Currency", row.RequestedProduct_Currency)
                factType.set(obj, "RequestedProduct_InterestRate", row.RequestedProduct_InterestRate)
                factType.set(obj, "RequestedProduct_Periodicity", row.RequestedProduct_Periodicity)
                factType.set(obj, "RequestedProduct_Term", row.RequestedProduct_Term)
                session.insert(obj)
            }
            session.fireAllRules()  
            val resultFactType = GlobalDrools_Apply_Strategy.kbase.getFactType("", "EstimatedApplication")    
            val resultsQuery = session.getQueryResults("getEstimation")
            val resultIterator = resultsQuery.iterator()
            while(resultIterator.hasNext()){
                val r = resultIterator.next()
                val resultObject = r.get("EstimatedApplicationList")
                results.add(Drools_Apply_StrategySchema(    
                    ApplicationID = resultFactType.get(resultObject, "ApplicationID").asInstanceOf[java.lang.String],    
                    AssociatedModelName = resultFactType.get(resultObject, "AssociatedModelName").asInstanceOf[java.lang.String],    
                    ProductType = resultFactType.get(resultObject, "ProductType").asInstanceOf[java.lang.String],    
                    ProposedProduct = resultFactType.get(resultObject, "ProposedProduct").asInstanceOf[java.lang.String],    
                    StrategyName = resultFactType.get(resultObject, "StrategyName").asInstanceOf[java.lang.String],    
                    StrategyVersion = resultFactType.get(resultObject, "StrategyVersion").asInstanceOf[java.lang.Long],    
                    Customer_BirthDate = resultFactType.get(resultObject, "Customer_BirthDate").asInstanceOf[java.lang.String],    
                    Customer_FirstName = resultFactType.get(resultObject, "Customer_FirstName").asInstanceOf[java.lang.String],    
                    Customer_Gender = resultFactType.get(resultObject, "Customer_Gender").asInstanceOf[java.lang.String],    
                    Customer_INN = resultFactType.get(resultObject, "Customer_INN").asInstanceOf[java.lang.Long],    
                    Customer_LastName = resultFactType.get(resultObject, "Customer_LastName").asInstanceOf[java.lang.String],    
                    Customer_PlaceOfBirth = resultFactType.get(resultObject, "Customer_PlaceOfBirth").asInstanceOf[java.lang.String],    
                    Customer_Snils = resultFactType.get(resultObject, "Customer_Snils").asInstanceOf[java.lang.String],    
                    Customer_citizenshipId = resultFactType.get(resultObject, "Customer_citizenshipId").asInstanceOf[java.lang.String],    
                    Customer_clientId = resultFactType.get(resultObject, "Customer_clientId").asInstanceOf[java.lang.Long],    
                    FinancialInformation_A1 = resultFactType.get(resultObject, "FinancialInformation_A1").asInstanceOf[java.lang.Long],    
                    FinancialInformation_A2 = resultFactType.get(resultObject, "FinancialInformation_A2").asInstanceOf[java.lang.Long],    
                    FinancialInformation_A3 = resultFactType.get(resultObject, "FinancialInformation_A3").asInstanceOf[java.lang.Long],    
                    FinancialInformation_A4 = resultFactType.get(resultObject, "FinancialInformation_A4").asInstanceOf[java.lang.Long],    
                    FinancialInformation_AverageTotalAssets = resultFactType.get(resultObject, "FinancialInformation_AverageTotalAssets").asInstanceOf[java.lang.Long],    
                    FinancialInformation_Currency = resultFactType.get(resultObject, "FinancialInformation_Currency").asInstanceOf[java.lang.String],    
                    FinancialInformation_L1 = resultFactType.get(resultObject, "FinancialInformation_L1").asInstanceOf[java.lang.Long],    
                    FinancialInformation_L2 = resultFactType.get(resultObject, "FinancialInformation_L2").asInstanceOf[java.lang.Long],    
                    FinancialInformation_L3 = resultFactType.get(resultObject, "FinancialInformation_L3").asInstanceOf[java.lang.Long],    
                    FinancialInformation_L4 = resultFactType.get(resultObject, "FinancialInformation_L4").asInstanceOf[java.lang.Long],    
                    FinancialInformation_Profit = resultFactType.get(resultObject, "FinancialInformation_Profit").asInstanceOf[java.lang.Long],    
                    FinancialInformation_SalesRevenue = resultFactType.get(resultObject, "FinancialInformation_SalesRevenue").asInstanceOf[java.lang.Long],    
                    FinancialInformation_WorkingCapital = resultFactType.get(resultObject, "FinancialInformation_WorkingCapital").asInstanceOf[java.lang.Long],    
                    GeneralInformation_NumMonthsActivity = resultFactType.get(resultObject, "GeneralInformation_NumMonthsActivity").asInstanceOf[java.lang.Long],    
                    GeneralInformation_NumMonthsSinceReg = resultFactType.get(resultObject, "GeneralInformation_NumMonthsSinceReg").asInstanceOf[java.lang.Long],    
                    RequestedProduct_AmortizationType = resultFactType.get(resultObject, "RequestedProduct_AmortizationType").asInstanceOf[java.lang.String],    
                    RequestedProduct_Amount = resultFactType.get(resultObject, "RequestedProduct_Amount").asInstanceOf[java.lang.Long],    
                    RequestedProduct_Currency = resultFactType.get(resultObject, "RequestedProduct_Currency").asInstanceOf[java.lang.String],    
                    RequestedProduct_InterestRate = resultFactType.get(resultObject, "RequestedProduct_InterestRate").asInstanceOf[java.lang.Double],    
                    RequestedProduct_Periodicity = resultFactType.get(resultObject, "RequestedProduct_Periodicity").asInstanceOf[java.lang.Long],    
                    RequestedProduct_Term = resultFactType.get(resultObject, "RequestedProduct_Term").asInstanceOf[java.lang.Long],    
                    ApplicationClass = resultFactType.get(resultObject, "ApplicationClass").asInstanceOf[java.lang.Integer]            
                ))
            }
            session.dispose()          
        })       
        JavaConversions.asScalaBuffer(results).toIterator      
    })
  }

  def getDS_AGGR_RESP(spark: SparkSession, Drools_Apply_Strategy: Dataset[Drools_Apply_StrategySchema]) = {
    import spark.implicits._
        Drools_Apply_Strategy
             .groupBy("ApplicationClass").agg($"ApplicationClass")
    .as[DS_AGGR_RESPSchema]  
  }

  def getJoin_Catalog(spark: SparkSession, DS_AGGR_RESP: Dataset[DS_AGGR_RESPSchema], PRODUCT_CATALOG: Dataset[PRODUCT_CATALOGSchema]) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    DS_AGGR_RESP.joinWith(PRODUCT_CATALOG, 
        lit(true), 
        "cross")
    .select(
    		col("_1.ApplicationClass")		.alias("ApplicationClass")		
    )    
    .as[Join_CatalogSchema]
  }

  def getXML_TO_CSM(spark: SparkSession, Drools_Apply_Strategy: Dataset[Drools_Apply_StrategySchema]) = {
    import spark.implicits._
    	  val ds0 = Drools_Apply_Strategy
          ds0.select("ApplicationID", "AssociatedModelName", "ProductType", "ProposedProduct", "StrategyName", "StrategyVersion", "Customer_BirthDate", "Customer_FirstName", "Customer_Gender", "Customer_INN", "Customer_LastName", "Customer_PlaceOfBirth", "Customer_Snils", "Customer_citizenshipId", "Customer_clientId", "FinancialInformation_A1", "FinancialInformation_A2", "FinancialInformation_A3", "FinancialInformation_A4", "FinancialInformation_AverageTotalAssets", "FinancialInformation_Currency", "FinancialInformation_L1", "FinancialInformation_L2", "FinancialInformation_L3", "FinancialInformation_L4", "FinancialInformation_Profit", "FinancialInformation_SalesRevenue", "FinancialInformation_WorkingCapital", "GeneralInformation_NumMonthsActivity", "GeneralInformation_NumMonthsSinceReg", "RequestedProduct_AmortizationType", "RequestedProduct_Amount", "RequestedProduct_Currency", "RequestedProduct_InterestRate", "RequestedProduct_Periodicity", "RequestedProduct_Term", "ApplicationClass")
          .toDF("ApplicationID", "AssociatedModelName", "ProductType", "ProposedProduct", "StrategyName", "StrategyVersion", "Customer_BirthDate", "Customer_FirstName", "Customer_Gender", "Customer_INN", "Customer_LastName", "Customer_PlaceOfBirth", "Customer_Snils", "Customer_citizenshipId", "Customer_clientId", "FinancialInformation_A1", "FinancialInformation_A2", "FinancialInformation_A3", "FinancialInformation_A4", "FinancialInformation_AverageTotalAssets", "FinancialInformation_Currency", "FinancialInformation_L1", "FinancialInformation_L2", "FinancialInformation_L3", "FinancialInformation_L4", "FinancialInformation_Profit", "FinancialInformation_SalesRevenue", "FinancialInformation_WorkingCapital", "GeneralInformation_NumMonthsActivity", "GeneralInformation_NumMonthsSinceReg", "RequestedProduct_AmortizationType", "RequestedProduct_Amount", "RequestedProduct_Currency", "RequestedProduct_InterestRate", "RequestedProduct_Periodicity", "RequestedProduct_Term", "ApplicationClass")
          .as[XML_TO_CSMSchema]
  }

  def CSV_FUSION_RISK(spark: SparkSession, ds: Dataset[Join_CatalogSchema]): Unit = {
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file://"
      s"${fs}"
    }
    logger.logInfo(s"CSVTarget CSV_FUSION_RISK path: ${path}")
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (true && fs.exists(new org.apache.hadoop.fs.Path(path))) {
      fs.delete(new org.apache.hadoop.fs.Path(path), true)
    }
    
    
    ds.write
      .option("header", """false""")
 
      .option("charset", """UTF-8""") 
      .option("sep", """,""") 
      .option("quote", """"""")
      .option("escape", """\""") 
      .option("comment", """#""") 
      .option("timestampFormat", """yyyy-MM-dd""")
 
      .csv(path)
  }

  def DS_XML_RESP(spark: SparkSession, ds: Dataset[XML_TO_CSMSchema]): Unit = {
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file://"
      s"${fs}"
    }
    logger.logInfo(s"CSVTarget DS_XML_RESP path: ${path}")
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (true && fs.exists(new org.apache.hadoop.fs.Path(path))) {
      fs.delete(new org.apache.hadoop.fs.Path(path), true)
    }
    
    
    ds.write
      .option("header", """false""")
 
      .option("charset", """UTF-8""") 
      .option("sep", """,""") 
      .option("quote", """"""")
      .option("escape", """\""") 
      .option("comment", """#""") 
      .option("timestampFormat", """yyyy-MM-dd""")
 
      .csv(path)
  }

 

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class DS_XML_REQSchema(
    var ApplicationID: java.lang.String,
    var AssociatedModelName: java.lang.String,
    var ProductType: java.lang.String,
    var ProposedProduct: java.lang.String,
    var StrategyName: java.lang.String,
    var StrategyVersion: java.lang.Long,
    var Customer_BirthDate: java.lang.String,
    var Customer_FirstName: java.lang.String,
    var Customer_Gender: java.lang.String,
    var Customer_INN: java.lang.Long,
    var Customer_LastName: java.lang.String,
    var Customer_PlaceOfBirth: java.lang.String,
    var Customer_Snils: java.lang.String,
    var Customer_citizenshipId: java.lang.String,
    var Customer_clientId: java.lang.Long,
    var FinancialInformation_A1: java.lang.Long,
    var FinancialInformation_A2: java.lang.Long,
    var FinancialInformation_A3: java.lang.Long,
    var FinancialInformation_A4: java.lang.Long,
    var FinancialInformation_AverageTotalAssets: java.lang.Long,
    var FinancialInformation_Currency: java.lang.String,
    var FinancialInformation_L1: java.lang.Long,
    var FinancialInformation_L2: java.lang.Long,
    var FinancialInformation_L3: java.lang.Long,
    var FinancialInformation_L4: java.lang.Long,
    var FinancialInformation_Profit: java.lang.Long,
    var FinancialInformation_SalesRevenue: java.lang.Long,
    var FinancialInformation_WorkingCapital: java.lang.Long,
    var GeneralInformation_NumMonthsActivity: java.lang.Long,
    var GeneralInformation_NumMonthsSinceReg: java.lang.Long,
    var RequestedProduct_AmortizationType: java.lang.String,
    var RequestedProduct_Amount: java.lang.Long,
    var RequestedProduct_Currency: java.lang.String,
    var RequestedProduct_InterestRate: java.lang.Double,
    var RequestedProduct_Periodicity: java.lang.Long,
    var RequestedProduct_Term: java.lang.Long
) extends Serializable
case class PRODUCT_CATALOGSchema(
) extends Serializable
case class Drools_Apply_StrategySchema(
    var ApplicationID: java.lang.String,
    var AssociatedModelName: java.lang.String,
    var ProductType: java.lang.String,
    var ProposedProduct: java.lang.String,
    var StrategyName: java.lang.String,
    var StrategyVersion: java.lang.Long,
    var Customer_BirthDate: java.lang.String,
    var Customer_FirstName: java.lang.String,
    var Customer_Gender: java.lang.String,
    var Customer_INN: java.lang.Long,
    var Customer_LastName: java.lang.String,
    var Customer_PlaceOfBirth: java.lang.String,
    var Customer_Snils: java.lang.String,
    var Customer_citizenshipId: java.lang.String,
    var Customer_clientId: java.lang.Long,
    var FinancialInformation_A1: java.lang.Long,
    var FinancialInformation_A2: java.lang.Long,
    var FinancialInformation_A3: java.lang.Long,
    var FinancialInformation_A4: java.lang.Long,
    var FinancialInformation_AverageTotalAssets: java.lang.Long,
    var FinancialInformation_Currency: java.lang.String,
    var FinancialInformation_L1: java.lang.Long,
    var FinancialInformation_L2: java.lang.Long,
    var FinancialInformation_L3: java.lang.Long,
    var FinancialInformation_L4: java.lang.Long,
    var FinancialInformation_Profit: java.lang.Long,
    var FinancialInformation_SalesRevenue: java.lang.Long,
    var FinancialInformation_WorkingCapital: java.lang.Long,
    var GeneralInformation_NumMonthsActivity: java.lang.Long,
    var GeneralInformation_NumMonthsSinceReg: java.lang.Long,
    var RequestedProduct_AmortizationType: java.lang.String,
    var RequestedProduct_Amount: java.lang.Long,
    var RequestedProduct_Currency: java.lang.String,
    var RequestedProduct_InterestRate: java.lang.Double,
    var RequestedProduct_Periodicity: java.lang.Long,
    var RequestedProduct_Term: java.lang.Long,
    var ApplicationClass: java.lang.Integer
) extends Serializable
case class DS_AGGR_RESPSchema(
    var ApplicationClass: java.lang.Integer
) extends Serializable
case class Join_CatalogSchema(
    var ApplicationClass: java.lang.Integer
) extends Serializable
case class XML_TO_CSMSchema(
    var ApplicationID: java.lang.String,
    var AssociatedModelName: java.lang.String,
    var ProductType: java.lang.String,
    var ProposedProduct: java.lang.String,
    var StrategyName: java.lang.String,
    var StrategyVersion: java.lang.Long,
    var Customer_BirthDate: java.lang.String,
    var Customer_FirstName: java.lang.String,
    var Customer_Gender: java.lang.String,
    var Customer_INN: java.lang.Long,
    var Customer_LastName: java.lang.String,
    var Customer_PlaceOfBirth: java.lang.String,
    var Customer_Snils: java.lang.String,
    var Customer_citizenshipId: java.lang.String,
    var Customer_clientId: java.lang.Long,
    var FinancialInformation_A1: java.lang.Long,
    var FinancialInformation_A2: java.lang.Long,
    var FinancialInformation_A3: java.lang.Long,
    var FinancialInformation_A4: java.lang.Long,
    var FinancialInformation_AverageTotalAssets: java.lang.Long,
    var FinancialInformation_Currency: java.lang.String,
    var FinancialInformation_L1: java.lang.Long,
    var FinancialInformation_L2: java.lang.Long,
    var FinancialInformation_L3: java.lang.Long,
    var FinancialInformation_L4: java.lang.Long,
    var FinancialInformation_Profit: java.lang.Long,
    var FinancialInformation_SalesRevenue: java.lang.Long,
    var FinancialInformation_WorkingCapital: java.lang.Long,
    var GeneralInformation_NumMonthsActivity: java.lang.Long,
    var GeneralInformation_NumMonthsSinceReg: java.lang.Long,
    var RequestedProduct_AmortizationType: java.lang.String,
    var RequestedProduct_Amount: java.lang.Long,
    var RequestedProduct_Currency: java.lang.String,
    var RequestedProduct_InterestRate: java.lang.Double,
    var RequestedProduct_Periodicity: java.lang.Long,
    var RequestedProduct_Term: java.lang.Long,
    var ApplicationClass: java.lang.Integer
) extends Serializable

}


object BELFIUS_applicationsJob {
   def main(args: Array[String]): Unit = {
     new BELFIUS_applicationsJob().sparkMain(args)
  }
}

