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



class BAI_PDModelJob extends ETLJobBase {

  override def getApplicationName: String = {
    "BAI_PDModel"
  }
  
  def run(spark: SparkSession): Any = {
    	val DictPortfolio = getDictPortfolio(spark)


    	Local_1(spark, DictPortfolio)
    	val GE_EconomicActivityMissing = getGE_EconomicActivityMissing(spark)


    	Local_4(spark, GE_EconomicActivityMissing)
    	val rcdataAccounts = getrcdataAccounts(spark)


    	val Sort_17 = getSort_17(spark, rcdataAccounts)


    	Local_7(spark, Sort_17)
    	val rcdataForbearance = getrcdataForbearance(spark)


    	Local_10(spark, rcdataForbearance)
    	val Sectores_de_Economia = getSectores_de_Economia(spark)


    	Local_13(spark, Sectores_de_Economia)
    	val WriteOff = getWriteOff(spark)


    	Local_16(spark, WriteOff)
 
  }

  def getDictPortfolio(spark: SparkSession) = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
    val schema = newProductEncoder(typeTag[DictPortfolioSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/tmp/ModelingPD_BAI_rawdata/DictPortfolio.csv"""
    }
    
    try {
      spark.read
.schema(schema)
      .option("header", """true""") 
      .option("charset", """UTF-8""")
      .option("sep", """;""") 
      .option("quote", """"""") 
      .option("escape", """\""")
 
      .option("comment", """#""") 
  
      .csv(path)
 
.as[DictPortfolioSchema]      
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[DictPortfolioSchema]      }
    
  }

  def getGE_EconomicActivityMissing(spark: SparkSession) = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
    val schema = newProductEncoder(typeTag[GE_EconomicActivityMissingSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/tmp/ModelingPD_BAI_rawdata/GE_EconomicActivityMissing.csv"""
    }
    
    try {
      spark.read
.schema(schema)
      .option("header", """true""") 
      .option("charset", """UTF-8""")
      .option("sep", """;""") 
      .option("quote", """"""") 
      .option("escape", """\""")
 
      .option("comment", """#""") 
  
      .csv(path)
 
.as[GE_EconomicActivityMissingSchema]      
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[GE_EconomicActivityMissingSchema]      }
    
  }

  def getrcdataAccounts(spark: SparkSession) = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
    val schema = newProductEncoder(typeTag[rcdataAccountsSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/tmp/ModelingPD_BAI_rawdata/rcdataAccounts.csv"""
    }
    
    try {
      spark.read
.schema(schema)
      .option("header", """true""") 
      .option("charset", """UTF-8""")
      .option("sep", """;""") 
      .option("quote", """'""") 
      .option("escape", """/""")
 
      .option("comment", """#""") 
  
      .csv(path)
 
.as[rcdataAccountsSchema]      
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[rcdataAccountsSchema]      }
    
  }

  def getrcdataForbearance(spark: SparkSession) = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
    val schema = newProductEncoder(typeTag[rcdataForbearanceSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/tmp/ModelingPD_BAI_rawdata/rcdataForbearance.csv"""
    }
    
    try {
      spark.read
.schema(schema)
      .option("header", """true""") 
      .option("charset", """UTF-8""")
      .option("sep", """;""") 
      .option("quote", """'""") 
      .option("escape", """/""")
 
      .option("comment", """#""") 
  
      .csv(path)
 
.as[rcdataForbearanceSchema]      
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[rcdataForbearanceSchema]      }
    
  }

  def getSectores_de_Economia(spark: SparkSession) = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
    val schema = newProductEncoder(typeTag[Sectores_de_EconomiaSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/tmp/ModelingPD_BAI_rawdata/Sectores_de_Economia.csv"""
    }
    
    try {
      spark.read
.schema(schema)
      .option("header", """true""") 
      .option("charset", """UTF-8""")
      .option("sep", """;""") 
      .option("quote", """"""") 
      .option("escape", """\""")
 
      .option("comment", """#""") 
  
      .csv(path)
 
.as[Sectores_de_EconomiaSchema]      
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[Sectores_de_EconomiaSchema]      }
    
  }

  def getWriteOff(spark: SparkSession) = {
  	import spark.implicits._
   	import scala.reflect.runtime.universe._
    val schema = newProductEncoder(typeTag[WriteOffSchema]).schema
    val path = {
      val fs = if (true) s"${_defaultFS}" else "file:///"
      s"""${fs}/tmp/ModelingPD_BAI_rawdata/WriteOff.csv"""
    }
    
    try {
      spark.read
.schema(schema)
      .option("header", """true""") 
      .option("charset", """UTF-8""")
      .option("sep", """;""") 
      .option("quote", """"""") 
      .option("escape", """\""")
 
      .option("comment", """#""") 
  
      .csv(path)
 
.as[WriteOffSchema]      
	  } catch {
          case e: UnsupportedOperationException =>  spark.emptyDataset[WriteOffSchema]      }
    
  }

  def getSort_17(spark: SparkSession, rcdataAccounts: Dataset[rcdataAccountsSchema]) = {
    import spark.implicits._
    rcdataAccounts.orderBy(rcdataAccounts("ID_Customer"), rcdataAccounts("ID_Account"), rcdataAccounts("D_ReferenceDate"))
  }

  def Local_1(spark: SparkSession, ds: Dataset[DictPortfolioSchema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/ModelingPD_BAI_rawdata/DictPortfolio.parquet"""
    logger.logInfo(s"LocalTarget Local_1 fileName: ${fileName}")   
    val dsOut = ds.select(ds("Code").alias("code"), ds("Name").alias("name"), ds("Name_Eng").alias("name_eng"), ds("_c3").alias("_c3"))  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save(fileName)    
    val partitions = Seq()
    def makeTypeDescription(dataType: org.apache.spark.sql.types.DataType): String = {
      if (dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {
        "ARRAY<" + makeTypeDescription(dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType) + ">"
      }
      else if (dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
        "STRUCT<" + dataType.asInstanceOf[org.apache.spark.sql.types.StructType].toList.map(f=>f.name + ": " + makeTypeDescription(f.dataType)).mkString(", ") + ">"
      }
      else {
        dataType.typeName
      }
    }
    var fieldStr = dsOut.schema.fields.filterNot(f => partitions.contains(f.name.toLowerCase)).map((f)=>{f.name.toLowerCase + " " + makeTypeDescription(f.dataType)}).mkString(", ")
    var partStr = dsOut.schema.fields.filter(f => partitions.contains(f.name.toLowerCase)).map(f => f.name.toLowerCase + " " + makeTypeDescription(f.dataType)).mkString(", ")
    spark.sql(s"""DROP TABLE IF EXISTS DictPortfolio""")
    val createQuery = s"""CREATE EXTERNAL TABLE DictPortfolio(${fieldStr})
    STORED AS PARQUET
    LOCATION '/tmp/ModelingPD_BAI_rawdata/DictPortfolio.parquet'"""
    logger.logInfo(s"Create external table:\n${createQuery}")   
    spark.sql(createQuery)
 
  }

  def Local_4(spark: SparkSession, ds: Dataset[GE_EconomicActivityMissingSchema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/ModelingPD_BAI_rawdata/GE_EconomicActivityMissing.parquet"""
    logger.logInfo(s"LocalTarget Local_4 fileName: ${fileName}")   
    val dsOut = ds.select(ds("ID_Account").alias("id_account"), ds("EconomicActivity").alias("economicactivity"), ds("ID_Customer").alias("id_customer"), ds("Nperiods").alias("nperiods"), ds("minStartDate").alias("minstartdate"), ds("minStartDate_Year").alias("minstartdate_year"), ds("CAE").alias("cae"))  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save(fileName)    
    val partitions = Seq()
    def makeTypeDescription(dataType: org.apache.spark.sql.types.DataType): String = {
      if (dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {
        "ARRAY<" + makeTypeDescription(dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType) + ">"
      }
      else if (dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
        "STRUCT<" + dataType.asInstanceOf[org.apache.spark.sql.types.StructType].toList.map(f=>f.name + ": " + makeTypeDescription(f.dataType)).mkString(", ") + ">"
      }
      else {
        dataType.typeName
      }
    }
    var fieldStr = dsOut.schema.fields.filterNot(f => partitions.contains(f.name.toLowerCase)).map((f)=>{f.name.toLowerCase + " " + makeTypeDescription(f.dataType)}).mkString(", ")
    var partStr = dsOut.schema.fields.filter(f => partitions.contains(f.name.toLowerCase)).map(f => f.name.toLowerCase + " " + makeTypeDescription(f.dataType)).mkString(", ")
    spark.sql(s"""DROP TABLE IF EXISTS GE_EconomicActivityMissing""")
    val createQuery = s"""CREATE EXTERNAL TABLE GE_EconomicActivityMissing(${fieldStr})
    STORED AS PARQUET
    LOCATION '/tmp/ModelingPD_BAI_rawdata/GE_EconomicActivityMissing.parquet'"""
    logger.logInfo(s"Create external table:\n${createQuery}")   
    spark.sql(createQuery)
 
  }

  def Local_7(spark: SparkSession, ds: Dataset[rcdataAccountsSchema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/ModelingPD_BAI_rawdata/rcdataAccounts.parquet"""
    logger.logInfo(s"LocalTarget Local_7 fileName: ${fileName}")   
    val dsOut = ds.select(ds("ID_Entity").alias("id_entity"), ds("ID_Account").alias("id_account"), ds("ID_AccountMask").alias("id_accountmask"), ds("D_ReferenceDate").alias("d_referencedate"), ds("ID_Customer").alias("id_customer"), ds("C_Portfolio").alias("c_portfolio"), ds("C_ProductType").alias("c_producttype"), ds("C_ProductSubType").alias("c_productsubtype"), ds("C_Rating").alias("c_rating"), ds("C_OriginationScoring").alias("c_originationscoring"), ds("C_BehaviourScoring").alias("c_behaviourscoring"), ds("N_OutstandingPrincipalAmount").alias("n_outstandingprincipalamount"), ds("N_AccruedInterestAmount").alias("n_accruedinterestamount"), ds("N_PastDueAmount").alias("n_pastdueamount"), ds("N_WriteOffAmount").alias("n_writeoffamount"), ds("N_CreditLineAmount").alias("n_creditlineamount"), ds("N_UsedCreditLineAmount").alias("n_usedcreditlineamount"), ds("C_CreditLineType").alias("c_creditlinetype"), ds("F_CreditLineConditionality").alias("f_creditlineconditionality"), ds("C_Currency").alias("c_currency"), ds("D_StartDate").alias("d_startdate"), ds("D_MaturityDate").alias("d_maturitydate"), ds("N_DaysInArrears").alias("n_daysinarrears"), ds("N_InterestRate").alias("n_interestrate"), ds("N_ContractualInterestSpread").alias("n_contractualinterestspread"), ds("C_InterestBasis").alias("c_interestbasis"), ds("C_InterestRateType").alias("c_interestratetype"), ds("C_IndexRate").alias("c_indexrate"), ds("N_PenaltyInterestSpread").alias("n_penaltyinterestspread"), ds("N_EffectiveInterestRate").alias("n_effectiveinterestrate"), ds("N_EffectiveInterestSpread").alias("n_effectiveinterestspread"), ds("F_InterestSuspended").alias("f_interestsuspended"), ds("C_AmortizationType").alias("c_amortizationtype"), ds("C_FreqPayPrincipal").alias("c_freqpayprincipal"), ds("C_FreqPayInterest").alias("c_freqpayinterest"), ds("D_FirstPrincipalPayDate").alias("d_firstprincipalpaydate"), ds("D_FirstInterestPayDate").alias("d_firstinterestpaydate"), ds("D_PrevPrincipalPayDate").alias("d_prevprincipalpaydate"), ds("D_PrevInterestPayDate").alias("d_previnterestpaydate"), ds("D_IntermPrincipalGracePeriodStartDate").alias("d_intermprincipalgraceperiodstartdate"), ds("D_IntermPrincipalGracePeriodEndDate").alias("d_intermprincipalgraceperiodenddate"), ds("D_IntermInterestGracePeriodStartDate").alias("d_interminterestgraceperiodstartdate"), ds("D_IntermInterestGracePeriodEndDate").alias("d_interminterestgraceperiodenddate"), ds("N_PrincipalAmortizationRate").alias("n_principalamortizationrate"), ds("N_PrincipalAmortizationAmount").alias("n_principalamortizationamount"), ds("N_LastPrincipalPayAmount").alias("n_lastprincipalpayamount"), ds("N_RealEstateCollateralValue").alias("n_realestatecollateralvalue"), ds("N_RealEstateLegalClaimValue").alias("n_realestatelegalclaimvalue"), ds("C_RealEstateCollateralType").alias("c_realestatecollateraltype"), ds("N_FinancialCollateralValue").alias("n_financialcollateralvalue"), ds("C_FinancialCollateralType").alias("c_financialcollateraltype"), ds("N_ThirdPartyGuaranteesValue").alias("n_thirdpartyguaranteesvalue"), ds("C_ThirdPartyGuaranteesType").alias("c_thirdpartyguaranteestype"), ds("N_OtherTangibleAssetsValue").alias("n_othertangibleassetsvalue"), ds("C_OtherTangibleAssetsType").alias("c_othertangibleassetstype"), ds("F_Forbearance").alias("f_forbearance"), ds("C_ForbearanceType").alias("c_forbearancetype"), ds("D_WriteOffDate").alias("d_writeoffdate"), ds("N_RemissionPrincipalAmount").alias("n_remissionprincipalamount"), ds("N_RemissionInterestAmount").alias("n_remissioninterestamount"), ds("D_RemissionDate").alias("d_remissiondate"), ds("F_LoanSales").alias("f_loansales"), ds("D_LoanSalesDate").alias("d_loansalesdate"), ds("N_LoanSalesPrice").alias("n_loansalesprice"), ds("F_Repossession").alias("f_repossession"), ds("F_RecoveryProcess").alias("f_recoveryprocess"), ds("C_RecoveryPhase").alias("c_recoveryphase"), ds("F_Default").alias("f_default"), ds("C_DefaultCriteria").alias("c_defaultcriteria"), ds("AuxField1").alias("auxfield1"), ds("AuxField2").alias("auxfield2"), ds("AuxField3").alias("auxfield3"), ds("AuxField4").alias("auxfield4"), ds("AuxField5").alias("auxfield5"), ds("AuxField6").alias("auxfield6"), ds("AuxField7").alias("auxfield7"), ds("AuxField8").alias("auxfield8"), ds("AuxField9").alias("auxfield9"), ds("AuxField10").alias("auxfield10"), ds("AuxField11").alias("auxfield11"), ds("AuxField12").alias("auxfield12"), ds("AuxField13").alias("auxfield13"), ds("AuxField14").alias("auxfield14"), ds("AuxField15").alias("auxfield15"), ds("AuxField16").alias("auxfield16"), ds("AuxField17").alias("auxfield17"), ds("AuxField18").alias("auxfield18"), ds("AuxField19").alias("auxfield19"), ds("AuxField20").alias("auxfield20"), ds("AuxField21").alias("auxfield21"), ds("AuxField22").alias("auxfield22"), ds("AuxField23").alias("auxfield23"), ds("AuxField24").alias("auxfield24"), ds("AuxField25").alias("auxfield25"), ds("AuxField26").alias("auxfield26"), ds("AuxField27").alias("auxfield27"), ds("AuxField28").alias("auxfield28"), ds("AuxField29").alias("auxfield29"), ds("AuxField30").alias("auxfield30"), ds("AuxField31").alias("auxfield31"), ds("AuxField32").alias("auxfield32"), ds("AuxField33").alias("auxfield33"), ds("AuxField34").alias("auxfield34"), ds("AuxField35").alias("auxfield35"), ds("AuxField36").alias("auxfield36"), ds("AuxField37").alias("auxfield37"), ds("AuxField38").alias("auxfield38"), ds("AuxField39").alias("auxfield39"), ds("AuxField40").alias("auxfield40"), ds("AuxField41").alias("auxfield41"), ds("AuxField42").alias("auxfield42"), ds("AuxField43").alias("auxfield43"), ds("AuxField44").alias("auxfield44"), ds("AuxField45").alias("auxfield45"), ds("AuxField46").alias("auxfield46"), ds("AuxField47").alias("auxfield47"), ds("AuxField48").alias("auxfield48"), ds("AuxField49").alias("auxfield49"), ds("AuxField50").alias("auxfield50"), ds("AuxField51").alias("auxfield51"), ds("AuxField52").alias("auxfield52"), ds("AuxField53").alias("auxfield53"), ds("AuxField54").alias("auxfield54"), ds("AuxField55").alias("auxfield55"), ds("AuxField56").alias("auxfield56"), ds("AuxField57").alias("auxfield57"), ds("AuxField58").alias("auxfield58"), ds("AuxField59").alias("auxfield59"), ds("AuxField60").alias("auxfield60"), ds("AuxField61").alias("auxfield61"), ds("AuxField62").alias("auxfield62"), ds("AuxField63").alias("auxfield63"), ds("AuxField64").alias("auxfield64"), ds("AuxField65").alias("auxfield65"), ds("AuxField66").alias("auxfield66"), ds("AuxField67").alias("auxfield67"), ds("AuxField68").alias("auxfield68"), ds("AuxField69").alias("auxfield69"), ds("AuxField70").alias("auxfield70"), ds("AuxField71").alias("auxfield71"), ds("AuxField72").alias("auxfield72"), ds("AuxField73").alias("auxfield73"), ds("AuxField74").alias("auxfield74"), ds("AuxField75").alias("auxfield75"), ds("AuxField76").alias("auxfield76"), ds("AuxField77").alias("auxfield77"), ds("AuxField78").alias("auxfield78"), ds("AuxField79").alias("auxfield79"), ds("AuxField80").alias("auxfield80"), ds("AuxField81").alias("auxfield81"), ds("AuxField82").alias("auxfield82"), ds("AuxField83").alias("auxfield83"), ds("AuxField84").alias("auxfield84"), ds("AuxField85").alias("auxfield85"), ds("AuxField86").alias("auxfield86"), ds("AuxField87").alias("auxfield87"), ds("AuxField88").alias("auxfield88"), ds("AuxField89").alias("auxfield89"), ds("AuxField90").alias("auxfield90"), ds("AuxField91").alias("auxfield91"), ds("AuxField92").alias("auxfield92"), ds("AuxField93").alias("auxfield93"), ds("AuxField94").alias("auxfield94"), ds("AuxField95").alias("auxfield95"), ds("AuxField96").alias("auxfield96"), ds("AuxField97").alias("auxfield97"), ds("AuxField98").alias("auxfield98"), ds("AuxField99").alias("auxfield99"), ds("AuxField100").alias("auxfield100"), ds("AuxField101").alias("auxfield101"), ds("AuxField102").alias("auxfield102"), ds("AuxField103").alias("auxfield103"), ds("AuxField104").alias("auxfield104"), ds("AuxField105").alias("auxfield105"), ds("AuxField106").alias("auxfield106"), ds("AuxField107").alias("auxfield107"), ds("AuxField108").alias("auxfield108"), ds("AuxField109").alias("auxfield109"), ds("AuxField110").alias("auxfield110"), ds("AuxField111").alias("auxfield111"), ds("AuxField112").alias("auxfield112"), ds("AuxField113").alias("auxfield113"), ds("AuxField114").alias("auxfield114"), ds("AuxField115").alias("auxfield115"), ds("AuxField116").alias("auxfield116"), ds("AuxField117").alias("auxfield117"), ds("AuxField118").alias("auxfield118"), ds("AuxField119").alias("auxfield119"), ds("AuxField120").alias("auxfield120"), ds("AuxField121").alias("auxfield121"), ds("AuxField122").alias("auxfield122"), ds("AuxField123").alias("auxfield123"), ds("AuxField124").alias("auxfield124"), ds("AuxField125").alias("auxfield125"), ds("AuxField126").alias("auxfield126"), ds("AuxField127").alias("auxfield127"), ds("AuxField128").alias("auxfield128"), ds("AuxField129").alias("auxfield129"), ds("AuxField130").alias("auxfield130"), ds("AuxField131").alias("auxfield131"), ds("AuxField132").alias("auxfield132"), ds("AuxField133").alias("auxfield133"), ds("AuxField134").alias("auxfield134"), ds("AuxField135").alias("auxfield135"), ds("AuxField136").alias("auxfield136"), ds("AuxField137").alias("auxfield137"), ds("AuxField138").alias("auxfield138"), ds("AuxField139").alias("auxfield139"), ds("AuxField140").alias("auxfield140"))  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save(fileName)    
    val partitions = Seq()
    def makeTypeDescription(dataType: org.apache.spark.sql.types.DataType): String = {
      if (dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {
        "ARRAY<" + makeTypeDescription(dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType) + ">"
      }
      else if (dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
        "STRUCT<" + dataType.asInstanceOf[org.apache.spark.sql.types.StructType].toList.map(f=>f.name + ": " + makeTypeDescription(f.dataType)).mkString(", ") + ">"
      }
      else {
        dataType.typeName
      }
    }
    var fieldStr = dsOut.schema.fields.filterNot(f => partitions.contains(f.name.toLowerCase)).map((f)=>{f.name.toLowerCase + " " + makeTypeDescription(f.dataType)}).mkString(", ")
    var partStr = dsOut.schema.fields.filter(f => partitions.contains(f.name.toLowerCase)).map(f => f.name.toLowerCase + " " + makeTypeDescription(f.dataType)).mkString(", ")
    spark.sql(s"""DROP TABLE IF EXISTS rcdataAccounts""")
    val createQuery = s"""CREATE EXTERNAL TABLE rcdataAccounts(${fieldStr})
    STORED AS PARQUET
    LOCATION '/tmp/ModelingPD_BAI_rawdata/rcdataAccounts.parquet'"""
    logger.logInfo(s"Create external table:\n${createQuery}")   
    spark.sql(createQuery)
 
  }

  def Local_10(spark: SparkSession, ds: Dataset[rcdataForbearanceSchema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/ModelingPD_BAI_rawdata/rcdataForbearance.parquet"""
    logger.logInfo(s"LocalTarget Local_10 fileName: ${fileName}")   
    val dsOut = ds.select(ds("D_ReferenceDate").alias("d_referencedate"), ds("ID_Customer").alias("id_customer"), ds("D_ForbearanceDate").alias("d_forbearancedate"), ds("ID_OriginalAccount").alias("id_originalaccount"), ds("ID_WorkoutAccount").alias("id_workoutaccount"), ds("C_ForbearanceType").alias("c_forbearancetype"), ds("N_ForbearanceAmount").alias("n_forbearanceamount"))  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save(fileName)    
    val partitions = Seq()
    def makeTypeDescription(dataType: org.apache.spark.sql.types.DataType): String = {
      if (dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {
        "ARRAY<" + makeTypeDescription(dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType) + ">"
      }
      else if (dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
        "STRUCT<" + dataType.asInstanceOf[org.apache.spark.sql.types.StructType].toList.map(f=>f.name + ": " + makeTypeDescription(f.dataType)).mkString(", ") + ">"
      }
      else {
        dataType.typeName
      }
    }
    var fieldStr = dsOut.schema.fields.filterNot(f => partitions.contains(f.name.toLowerCase)).map((f)=>{f.name.toLowerCase + " " + makeTypeDescription(f.dataType)}).mkString(", ")
    var partStr = dsOut.schema.fields.filter(f => partitions.contains(f.name.toLowerCase)).map(f => f.name.toLowerCase + " " + makeTypeDescription(f.dataType)).mkString(", ")
    spark.sql(s"""DROP TABLE IF EXISTS rcdataForbearance""")
    val createQuery = s"""CREATE EXTERNAL TABLE rcdataForbearance(${fieldStr})
    STORED AS PARQUET
    LOCATION '/tmp/ModelingPD_BAI_rawdata/rcdataForbearance.parquet'"""
    logger.logInfo(s"Create external table:\n${createQuery}")   
    spark.sql(createQuery)
 
  }

  def Local_13(spark: SparkSession, ds: Dataset[Sectores_de_EconomiaSchema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/ModelingPD_BAI_rawdata/Sectores_de_Economia.parquet"""
    logger.logInfo(s"LocalTarget Local_13 fileName: ${fileName}")   
    val dsOut = ds.select(ds("ContractReference").alias("contractreference"), ds("EconomicActivity").alias("economicactivity"))  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save(fileName)    
    val partitions = Seq()
    def makeTypeDescription(dataType: org.apache.spark.sql.types.DataType): String = {
      if (dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {
        "ARRAY<" + makeTypeDescription(dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType) + ">"
      }
      else if (dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
        "STRUCT<" + dataType.asInstanceOf[org.apache.spark.sql.types.StructType].toList.map(f=>f.name + ": " + makeTypeDescription(f.dataType)).mkString(", ") + ">"
      }
      else {
        dataType.typeName
      }
    }
    var fieldStr = dsOut.schema.fields.filterNot(f => partitions.contains(f.name.toLowerCase)).map((f)=>{f.name.toLowerCase + " " + makeTypeDescription(f.dataType)}).mkString(", ")
    var partStr = dsOut.schema.fields.filter(f => partitions.contains(f.name.toLowerCase)).map(f => f.name.toLowerCase + " " + makeTypeDescription(f.dataType)).mkString(", ")
    spark.sql(s"""DROP TABLE IF EXISTS Sectores_de_Economia""")
    val createQuery = s"""CREATE EXTERNAL TABLE Sectores_de_Economia(${fieldStr})
    STORED AS PARQUET
    LOCATION '/tmp/ModelingPD_BAI_rawdata/Sectores_de_Economia.parquet'"""
    logger.logInfo(s"Create external table:\n${createQuery}")   
    spark.sql(createQuery)
 
  }

  def Local_16(spark: SparkSession, ds: Dataset[WriteOffSchema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/ModelingPD_BAI_rawdata/WriteOff.parquet"""
    logger.logInfo(s"LocalTarget Local_16 fileName: ${fileName}")   
    val dsOut = ds.select(ds("ID_Entity").alias("id_entity"), ds("ID_Account").alias("id_account"), ds("ID_AccountMask").alias("id_accountmask"), ds("D_ReferenceDate").alias("d_referencedate"), ds("ID_Customer").alias("id_customer"), ds("C_Portfolio").alias("c_portfolio"), ds("C_ProductType").alias("c_producttype"), ds("C_ProductSubType").alias("c_productsubtype"), ds("N_OutstandingPrincipalAmount").alias("n_outstandingprincipalamount"), ds("N_AccruedInterestAmount").alias("n_accruedinterestamount"), ds("N_PastDueAmount").alias("n_pastdueamount"), ds("N_WriteOffAmount").alias("n_writeoffamount"), ds("C_Currency").alias("c_currency"), ds("D_StartDate").alias("d_startdate"), ds("D_MaturityDate").alias("d_maturitydate"), ds("N_DaysInArrears").alias("n_daysinarrears"), ds("AuxField3").alias("auxfield3"), ds("AuxField4").alias("auxfield4"), ds("AuxField50").alias("auxfield50"), ds("AuxField52").alias("auxfield52"))  
    dsOut    .write
    .mode(SaveMode.Overwrite)
    .format("parquet")
    .save(fileName)    
    val partitions = Seq()
    def makeTypeDescription(dataType: org.apache.spark.sql.types.DataType): String = {
      if (dataType.isInstanceOf[org.apache.spark.sql.types.ArrayType]) {
        "ARRAY<" + makeTypeDescription(dataType.asInstanceOf[org.apache.spark.sql.types.ArrayType].elementType) + ">"
      }
      else if (dataType.isInstanceOf[org.apache.spark.sql.types.StructType]) {
        "STRUCT<" + dataType.asInstanceOf[org.apache.spark.sql.types.StructType].toList.map(f=>f.name + ": " + makeTypeDescription(f.dataType)).mkString(", ") + ">"
      }
      else {
        dataType.typeName
      }
    }
    var fieldStr = dsOut.schema.fields.filterNot(f => partitions.contains(f.name.toLowerCase)).map((f)=>{f.name.toLowerCase + " " + makeTypeDescription(f.dataType)}).mkString(", ")
    var partStr = dsOut.schema.fields.filter(f => partitions.contains(f.name.toLowerCase)).map(f => f.name.toLowerCase + " " + makeTypeDescription(f.dataType)).mkString(", ")
    spark.sql(s"""DROP TABLE IF EXISTS WriteOff""")
    val createQuery = s"""CREATE EXTERNAL TABLE WriteOff(${fieldStr})
    STORED AS PARQUET
    LOCATION '/tmp/ModelingPD_BAI_rawdata/WriteOff.parquet'"""
    logger.logInfo(s"Create external table:\n${createQuery}")   
    spark.sql(createQuery)
 
  }


    override def initBuilder(builder: SparkSession.Builder): SparkSession.Builder = {
        builder.enableHiveSupport()
    } 

org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
case class DictPortfolioSchema(
    var Code: java.lang.String,
    var Name: java.lang.String,
    var Name_Eng: java.lang.String,
    var _c3: java.lang.String
) extends Serializable
case class GE_EconomicActivityMissingSchema(
    var ID_Account: java.lang.Long,
    var EconomicActivity: java.lang.String,
    var ID_Customer: java.lang.Integer,
    var Nperiods: java.lang.Integer,
    var minStartDate: java.lang.String,
    var minStartDate_Year: java.lang.Integer,
    var CAE: java.lang.Integer
) extends Serializable
case class rcdataAccountsSchema(
    var ID_Entity: java.lang.String,
    var ID_Account: java.lang.Long,
    var ID_AccountMask: java.lang.String,
    var D_ReferenceDate: java.lang.Integer,
    var ID_Customer: java.lang.Integer,
    var C_Portfolio: java.lang.String,
    var C_ProductType: java.lang.String,
    var C_ProductSubType: java.lang.String,
    var C_Rating: java.lang.String,
    var C_OriginationScoring: java.lang.String,
    var C_BehaviourScoring: java.lang.String,
    var N_OutstandingPrincipalAmount: java.lang.Double,
    var N_AccruedInterestAmount: java.lang.Double,
    var N_PastDueAmount: java.lang.Double,
    var N_WriteOffAmount: java.lang.Double,
    var N_CreditLineAmount: java.lang.Double,
    var N_UsedCreditLineAmount: java.lang.Double,
    var C_CreditLineType: java.lang.String,
    var F_CreditLineConditionality: java.lang.Integer,
    var C_Currency: java.lang.String,
    var D_StartDate: java.lang.String,
    var D_MaturityDate: java.lang.String,
    var N_DaysInArrears: java.lang.Integer,
    var N_InterestRate: java.lang.Double,
    var N_ContractualInterestSpread: java.lang.Double,
    var C_InterestBasis: java.lang.Integer,
    var C_InterestRateType: java.lang.Integer,
    var C_IndexRate: java.lang.String,
    var N_PenaltyInterestSpread: java.lang.Double,
    var N_EffectiveInterestRate: java.lang.Double,
    var N_EffectiveInterestSpread: java.lang.Double,
    var F_InterestSuspended: java.lang.Integer,
    var C_AmortizationType: java.lang.Integer,
    var C_FreqPayPrincipal: java.lang.Integer,
    var C_FreqPayInterest: java.lang.Integer,
    var D_FirstPrincipalPayDate: java.lang.String,
    var D_FirstInterestPayDate: java.lang.String,
    var D_PrevPrincipalPayDate: java.lang.String,
    var D_PrevInterestPayDate: java.lang.String,
    var D_IntermPrincipalGracePeriodStartDate: java.lang.String,
    var D_IntermPrincipalGracePeriodEndDate: java.lang.String,
    var D_IntermInterestGracePeriodStartDate: java.lang.String,
    var D_IntermInterestGracePeriodEndDate: java.lang.String,
    var N_PrincipalAmortizationRate: java.lang.Double,
    var N_PrincipalAmortizationAmount: java.lang.Double,
    var N_LastPrincipalPayAmount: java.lang.Double,
    var N_RealEstateCollateralValue: java.lang.Double,
    var N_RealEstateLegalClaimValue: java.lang.Double,
    var C_RealEstateCollateralType: java.lang.String,
    var N_FinancialCollateralValue: java.lang.Double,
    var C_FinancialCollateralType: java.lang.String,
    var N_ThirdPartyGuaranteesValue: java.lang.Double,
    var C_ThirdPartyGuaranteesType: java.lang.String,
    var N_OtherTangibleAssetsValue: java.lang.Double,
    var C_OtherTangibleAssetsType: java.lang.String,
    var F_Forbearance: java.lang.Integer,
    var C_ForbearanceType: java.lang.Integer,
    var D_WriteOffDate: java.lang.String,
    var N_RemissionPrincipalAmount: java.lang.Double,
    var N_RemissionInterestAmount: java.lang.Double,
    var D_RemissionDate: java.lang.String,
    var F_LoanSales: java.lang.Integer,
    var D_LoanSalesDate: java.lang.String,
    var N_LoanSalesPrice: java.lang.Double,
    var F_Repossession: java.lang.Integer,
    var F_RecoveryProcess: java.lang.Integer,
    var C_RecoveryPhase: java.lang.Integer,
    var F_Default: java.lang.Integer,
    var C_DefaultCriteria: java.lang.Integer,
    var AuxField1: java.lang.String,
    var AuxField2: java.lang.String,
    var AuxField3: java.lang.String,
    var AuxField4: java.lang.String,
    var AuxField5: java.lang.String,
    var AuxField6: java.lang.String,
    var AuxField7: java.lang.String,
    var AuxField8: java.lang.String,
    var AuxField9: java.lang.String,
    var AuxField10: java.lang.String,
    var AuxField11: java.lang.String,
    var AuxField12: java.lang.String,
    var AuxField13: java.lang.String,
    var AuxField14: java.lang.String,
    var AuxField15: java.lang.String,
    var AuxField16: java.lang.String,
    var AuxField17: java.lang.String,
    var AuxField18: java.lang.String,
    var AuxField19: java.lang.String,
    var AuxField20: java.lang.String,
    var AuxField21: java.lang.String,
    var AuxField22: java.lang.String,
    var AuxField23: java.lang.String,
    var AuxField24: java.lang.String,
    var AuxField25: java.lang.String,
    var AuxField26: java.lang.String,
    var AuxField27: java.lang.String,
    var AuxField28: java.lang.String,
    var AuxField29: java.lang.String,
    var AuxField30: java.lang.String,
    var AuxField31: java.lang.String,
    var AuxField32: java.lang.String,
    var AuxField33: java.lang.String,
    var AuxField34: java.lang.String,
    var AuxField35: java.lang.String,
    var AuxField36: java.lang.String,
    var AuxField37: java.lang.String,
    var AuxField38: java.lang.String,
    var AuxField39: java.lang.String,
    var AuxField40: java.lang.String,
    var AuxField41: java.lang.String,
    var AuxField42: java.lang.String,
    var AuxField43: java.lang.String,
    var AuxField44: java.lang.String,
    var AuxField45: java.lang.String,
    var AuxField46: java.lang.String,
    var AuxField47: java.lang.String,
    var AuxField48: java.lang.String,
    var AuxField49: java.lang.String,
    var AuxField50: java.lang.Double,
    var AuxField51: java.lang.Double,
    var AuxField52: java.lang.Double,
    var AuxField53: java.lang.Double,
    var AuxField54: java.lang.String,
    var AuxField55: java.lang.String,
    var AuxField56: java.lang.String,
    var AuxField57: java.lang.String,
    var AuxField58: java.lang.String,
    var AuxField59: java.lang.String,
    var AuxField60: java.lang.String,
    var AuxField61: java.lang.String,
    var AuxField62: java.lang.String,
    var AuxField63: java.lang.String,
    var AuxField64: java.lang.String,
    var AuxField65: java.lang.String,
    var AuxField66: java.lang.String,
    var AuxField67: java.lang.String,
    var AuxField68: java.lang.String,
    var AuxField69: java.lang.String,
    var AuxField70: java.lang.String,
    var AuxField71: java.lang.String,
    var AuxField72: java.lang.String,
    var AuxField73: java.lang.String,
    var AuxField74: java.lang.String,
    var AuxField75: java.lang.String,
    var AuxField76: java.lang.String,
    var AuxField77: java.lang.String,
    var AuxField78: java.lang.String,
    var AuxField79: java.lang.String,
    var AuxField80: java.lang.String,
    var AuxField81: java.lang.String,
    var AuxField82: java.lang.String,
    var AuxField83: java.lang.String,
    var AuxField84: java.lang.String,
    var AuxField85: java.lang.String,
    var AuxField86: java.lang.String,
    var AuxField87: java.lang.String,
    var AuxField88: java.lang.String,
    var AuxField89: java.lang.String,
    var AuxField90: java.lang.String,
    var AuxField91: java.lang.String,
    var AuxField92: java.lang.String,
    var AuxField93: java.lang.String,
    var AuxField94: java.lang.String,
    var AuxField95: java.lang.String,
    var AuxField96: java.lang.String,
    var AuxField97: java.lang.String,
    var AuxField98: java.lang.String,
    var AuxField99: java.lang.String,
    var AuxField100: java.lang.String,
    var AuxField101: java.lang.String,
    var AuxField102: java.lang.String,
    var AuxField103: java.lang.String,
    var AuxField104: java.lang.String,
    var AuxField105: java.lang.String,
    var AuxField106: java.lang.String,
    var AuxField107: java.lang.String,
    var AuxField108: java.lang.String,
    var AuxField109: java.lang.String,
    var AuxField110: java.lang.String,
    var AuxField111: java.lang.String,
    var AuxField112: java.lang.String,
    var AuxField113: java.lang.String,
    var AuxField114: java.lang.String,
    var AuxField115: java.lang.String,
    var AuxField116: java.lang.String,
    var AuxField117: java.lang.String,
    var AuxField118: java.lang.String,
    var AuxField119: java.lang.String,
    var AuxField120: java.lang.String,
    var AuxField121: java.lang.String,
    var AuxField122: java.lang.String,
    var AuxField123: java.lang.String,
    var AuxField124: java.lang.String,
    var AuxField125: java.lang.String,
    var AuxField126: java.lang.String,
    var AuxField127: java.lang.String,
    var AuxField128: java.lang.String,
    var AuxField129: java.lang.String,
    var AuxField130: java.lang.String,
    var AuxField131: java.lang.String,
    var AuxField132: java.lang.String,
    var AuxField133: java.lang.String,
    var AuxField134: java.lang.String,
    var AuxField135: java.lang.String,
    var AuxField136: java.lang.String,
    var AuxField137: java.lang.String,
    var AuxField138: java.lang.String,
    var AuxField139: java.lang.String,
    var AuxField140: java.lang.String
) extends Serializable
case class rcdataForbearanceSchema(
    var D_ReferenceDate: java.lang.Integer,
    var ID_Customer: java.lang.Integer,
    var D_ForbearanceDate: java.lang.String,
    var ID_OriginalAccount: java.lang.String,
    var ID_WorkoutAccount: java.lang.Long,
    var C_ForbearanceType: java.lang.String,
    var N_ForbearanceAmount: java.lang.Double
) extends Serializable
case class Sectores_de_EconomiaSchema(
    var ContractReference: java.math.BigDecimal,
    var EconomicActivity: java.lang.String
) extends Serializable
case class WriteOffSchema(
    var ID_Entity: java.lang.String,
    var ID_Account: java.lang.Long,
    var ID_AccountMask: java.lang.Double,
    var D_ReferenceDate: java.lang.Integer,
    var ID_Customer: java.lang.Integer,
    var C_Portfolio: java.lang.String,
    var C_ProductType: java.lang.String,
    var C_ProductSubType: java.lang.String,
    var N_OutstandingPrincipalAmount: java.lang.Double,
    var N_AccruedInterestAmount: java.lang.Double,
    var N_PastDueAmount: java.lang.Double,
    var N_WriteOffAmount: java.lang.Double,
    var C_Currency: java.lang.String,
    var D_StartDate: java.lang.String,
    var D_MaturityDate: java.lang.String,
    var N_DaysInArrears: java.lang.Integer,
    var AuxField3: java.lang.String,
    var AuxField4: java.lang.String,
    var AuxField50: java.lang.Double,
    var AuxField52: java.lang.String
) extends Serializable


}


object BAI_PDModelJob {
   def main(args: Array[String]): Unit = {
     new BAI_PDModelJob().sparkMain(args)
  }
}

