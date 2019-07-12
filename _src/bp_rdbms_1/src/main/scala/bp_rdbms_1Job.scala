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

import org.apache.spark.sql.types.NullType


class bp_rdbms_1Job extends ETLJobBase {

  override def getApplicationName: String = {
    "bp_rdbms_1"
  }
  
  def run(spark: SparkSession): Any = {
    	val etl_project = getetl_project(spark)


    	val etl_transformation = getetl_transformation(spark)


    	val Join4 = getJoin4(spark, etl_project, etl_transformation)


    	val auth_auditinfo = getauth_auditinfo(spark)


    	val Join9 = getJoin9(spark, Join4, auth_auditinfo)


    	val SQL_transformationstep = getSQL_transformationstep(spark)


    	val Spark_SQL13 = getSpark_SQL13(spark, Join9, SQL_transformationstep)


    	transformations_json(spark, Spark_SQL13)
 
  }
    
  def getetl_project(spark: SparkSession) = {
  
    import spark.implicits._
  
    val sqlText = s"""select 
                        dtype,
                        e_id,
                        name,
                        svncommitmessage,
                        svnenabled,
                        svnpassword,
                        svnurl,
                        svnusername
                      from etl_project"""
    
    logger.logInfo(s"SQLSource etl_project query: ${sqlText}")    
    val contextName = "datagram"
    val context: JdbcETLContext = getContext(contextName).asInstanceOf[JdbcETLContext]
        
    val ds = spark.read.format("jdbc").options(Map(
               "url" -> context._url,
               "dbtable" -> ("(" + sqlText + ") t"),
               "driver" -> context._driverClassName,
               "user" -> context._user,
               "password" -> context._password,
               "fetchSize" -> _fetchSize.toString
            ))		    
    ds.load().map{ row => etl_projectSchema(
	  	row.getAs[java.lang.String]("dtype"),	        
	  	row.getAs[java.lang.Long]("e_id"),	        
	  	row.getAs[java.lang.String]("name"),	        
	  	row.getAs[java.lang.String]("svncommitmessage"),	        
	  	row.getAs[java.lang.Boolean]("svnenabled"),	        
	  	row.getAs[java.lang.String]("svnpassword"),	        
	  	row.getAs[java.lang.String]("svnurl"),	        
	  	row.getAs[java.lang.String]("svnusername")	        
	)}
  }
    
  def getetl_transformation(spark: SparkSession) = {
  
    import spark.implicits._
  
    val sqlText = s"""select 
                        auditinfo_auditinfo_e_id,
                        dtype,
                        e_id,
                        jsonview,
                        label,
                        name,
                        project_project_e_id
                      from etl_transformation"""
    
    logger.logInfo(s"SQLSource etl_transformation query: ${sqlText}")    
    val contextName = "datagram"
    val context: JdbcETLContext = getContext(contextName).asInstanceOf[JdbcETLContext]
        
    val ds = spark.read.format("jdbc").options(Map(
               "url" -> context._url,
               "dbtable" -> ("(" + sqlText + ") t"),
               "driver" -> context._driverClassName,
               "user" -> context._user,
               "password" -> context._password,
               "fetchSize" -> _fetchSize.toString
            ))		    
    ds.load().map{ row => etl_transformationSchema(
	  	row.getAs[java.lang.Long]("auditinfo_auditinfo_e_id"),	        
	  	row.getAs[java.lang.String]("dtype"),	        
	  	row.getAs[java.lang.Long]("e_id"),	        
	  	row.getAs[java.lang.String]("jsonview"),	        
	  	row.getAs[java.lang.String]("label"),	        
	  	row.getAs[java.lang.String]("name"),	        
	  	row.getAs[java.lang.Long]("project_project_e_id")	        
	)}
  }
    
  def getauth_auditinfo(spark: SparkSession) = {
  
    import spark.implicits._
  
    val sqlText = s"""select 
                        changeuser,
                        createuser,
                        dtype,
                        e_container,
                        econtainer_class,
                        e_container_feature_name,
                        e_id
                      from auth_auditinfo"""
    
    logger.logInfo(s"SQLSource auth_auditinfo query: ${sqlText}")    
    val contextName = "datagram"
    val context: JdbcETLContext = getContext(contextName).asInstanceOf[JdbcETLContext]
        
    val ds = spark.read.format("jdbc").options(Map(
               "url" -> context._url,
               "dbtable" -> ("(" + sqlText + ") t"),
               "driver" -> context._driverClassName,
               "user" -> context._user,
               "password" -> context._password,
               "fetchSize" -> _fetchSize.toString
            ))		    
    ds.load().map{ row => auth_auditinfoSchema(
	  	row.getAs[java.lang.String]("changeuser"),	        
	  	row.getAs[java.lang.String]("createuser"),	        
	  	row.getAs[java.lang.String]("dtype"),	        
	  	row.getAs[java.lang.String]("e_container"),	        
	  	row.getAs[java.lang.String]("econtainer_class"),	        
	  	row.getAs[java.lang.String]("e_container_feature_name"),	        
	  	row.getAs[java.lang.Long]("e_id")	        
	)}
  }
    
  def getSQL_transformationstep(spark: SparkSession) = {
  
    import spark.implicits._
  
    val sqlText = s"""select 
                        batchsize,
                        checkpoint,
                        context_context_e_id,
                        dtype,
                        e_id,
                        explain,
                        expression,
                        fieldname,
                        finalexpression,
                        initexpression,
                        inputfacttypename,
                        jointype,
                        label,
                        labelfieldname,
                        mergeexpression,
                        methodname,
                        modelfile,
                        name,
                        outputport_outputport_e_id,
                        pivotfield,
                        port_errorport_e_id,
                        port_inputport_e_id,
                        port_joineeport_e_id,
                        port_unionport_e_id,
                        resultfactname,
                        resultfacttypename,
                        resultqueryname,
                        samplesize,
                        sequencedname,
                        sequencetype,
                        statement,
                        transformationstep_transformation_e_id,
                        transformation_transformationsteps_idx,
                        userdefagg
                      from etl_transformationstep"""
    
    logger.logInfo(s"SQLSource SQL_transformationstep query: ${sqlText}")    
    val contextName = "datagram"
    val context: JdbcETLContext = getContext(contextName).asInstanceOf[JdbcETLContext]
        
    val ds = spark.read.format("jdbc").options(Map(
               "url" -> context._url,
               "dbtable" -> ("(" + sqlText + ") t"),
               "driver" -> context._driverClassName,
               "user" -> context._user,
               "password" -> context._password,
               "fetchSize" -> _fetchSize.toString
            ))		    
    ds.load().map{ row => SQL_transformationstepSchema(
	  	row.getAs[java.lang.Integer]("batchsize"),	        
	  	row.getAs[java.lang.Boolean]("checkpoint"),	        
	  	row.getAs[java.lang.Long]("context_context_e_id"),	        
	  	row.getAs[java.lang.String]("dtype"),	        
	  	row.getAs[java.lang.Long]("e_id"),	        
	  	row.getAs[java.lang.Boolean]("explain"),	        
	  	row.getAs[java.lang.String]("expression"),	        
	  	row.getAs[java.lang.String]("fieldname"),	        
	  	row.getAs[java.lang.String]("finalexpression"),	        
	  	row.getAs[java.lang.String]("initexpression"),	        
	  	row.getAs[java.lang.String]("inputfacttypename"),	        
	  	row.getAs[java.lang.String]("jointype"),	        
	  	row.getAs[java.lang.String]("label"),	        
	  	row.getAs[java.lang.String]("labelfieldname"),	        
	  	row.getAs[java.lang.String]("mergeexpression"),	        
	  	row.getAs[java.lang.String]("methodname"),	        
	  	row.getAs[java.lang.String]("modelfile"),	        
	  	row.getAs[java.lang.String]("name"),	        
	  	row.getAs[java.lang.Long]("outputport_outputport_e_id"),	        
	  	row.getAs[java.lang.String]("pivotfield"),	        
	  	row.getAs[java.lang.Long]("port_errorport_e_id"),	        
	  	row.getAs[java.lang.Long]("port_inputport_e_id"),	        
	  	row.getAs[java.lang.Long]("port_joineeport_e_id"),	        
	  	row.getAs[java.lang.Long]("port_unionport_e_id"),	        
	  	row.getAs[java.lang.String]("resultfactname"),	        
	  	row.getAs[java.lang.String]("resultfacttypename"),	        
	  	row.getAs[java.lang.String]("resultqueryname"),	        
	  	row.getAs[java.lang.Integer]("samplesize"),	        
	  	row.getAs[java.lang.String]("sequencedname"),	        
	  	row.getAs[java.lang.String]("sequencetype"),	        
	  	row.getAs[java.lang.String]("statement"),	        
	  	row.getAs[java.lang.Long]("transformationstep_transformation_e_id"),	        
	  	row.getAs[java.lang.Integer]("transformation_transformationsteps_idx"),	        
	  	row.getAs[java.lang.Boolean]("userdefagg")	        
	)}
  }

  def getJoin4(spark: SparkSession, etl_project: Dataset[etl_projectSchema], etl_transformation: Dataset[etl_transformationSchema]) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    etl_project.joinWith(etl_transformation, 
        etl_project("e_id") === etl_transformation("project_project_e_id"), 
        "right_outer")
    .select(
    		col("_1.dtype")		.alias("dtype"),  		
    		col("_2.e_id")		.alias("transformation_id"),  		
    		col("_1.name")		.alias("project_name"),  		
    		col("_1.svncommitmessage")		.alias("svncommitmessage"),  		
    		col("_1.svnenabled")		.alias("svnenabled"),  		
    		col("_1.svnpassword")		.alias("svnpassword"),  		
    		col("_1.svnurl")		.alias("svnurl"),  		
    		col("_1.svnusername")		.alias("svnusername"),  		
    		col("_2.auditinfo_auditinfo_e_id")		.alias("auditinfo_auditinfo_e_id"),  		
    		col("_2.name")		.alias("transformation_name")		
    )    
    .as[Join4Schema]
  }

  def getJoin9(spark: SparkSession, Join4: Dataset[Join4Schema], auth_auditinfo: Dataset[auth_auditinfoSchema]) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    Join4.joinWith(auth_auditinfo, 
        Join4("auditinfo_auditinfo_e_id") === auth_auditinfo("e_id"), 
        "left_outer")
    .select(
    		col("_1.project_name")		.alias("project_name"),  		
    		col("_1.transformation_name")		.alias("transformation_name"),  		
    		col("_2.changeuser")		.alias("changeuser"),  		
    		col("_2.createuser")		.alias("createuser"),  		
    		col("_1.transformation_id")		.alias("transformation_id")		
    )    
    .as[Join9Schema]
  }

  def getSpark_SQL13(spark: SparkSession, Join9: Dataset[Join9Schema], SQL_transformationstep: Dataset[SQL_transformationstepSchema]) = {
    import spark.implicits._
    	
    		Join9.createOrReplaceTempView("transformations")
    	
    	
    		SQL_transformationstep.createOrReplaceTempView("steps")
    	
    			
    	val sqlText = s"""select transformations.*, steps.*
    	                  from transformations inner join steps on
    	                  transformations.transformation_id = steps.transformationstep_transformation_e_id"""	
    	val queryResult = spark.sql(s"${sqlText}")
    	
    		
    	queryResult.as[Spark_SQL13Schema]      
  }

  def transformations_json(spark: SparkSession, ds: Dataset[Spark_SQL13Schema]): Unit = {
 
    val fileName = s"""${_defaultFS}/tmp/blueprint/transformations.json"""
    logger.logInfo(s"LocalTarget transformations_json fileName: ${fileName}")   
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
case class etl_projectSchema(
    var dtype: java.lang.String,
    var e_id: java.lang.Long,
    var name: java.lang.String,
    var svncommitmessage: java.lang.String,
    var svnenabled: java.lang.Boolean,
    var svnpassword: java.lang.String,
    var svnurl: java.lang.String,
    var svnusername: java.lang.String
) extends Serializable
case class etl_transformationSchema(
    var auditinfo_auditinfo_e_id: java.lang.Long,
    var dtype: java.lang.String,
    var e_id: java.lang.Long,
    var jsonview: java.lang.String,
    var label: java.lang.String,
    var name: java.lang.String,
    var project_project_e_id: java.lang.Long
) extends Serializable
case class auth_auditinfoSchema(
    var changeuser: java.lang.String,
    var createuser: java.lang.String,
    var dtype: java.lang.String,
    var e_container: java.lang.String,
    var econtainer_class: java.lang.String,
    var e_container_feature_name: java.lang.String,
    var e_id: java.lang.Long
) extends Serializable
case class SQL_transformationstepSchema(
    var batchsize: java.lang.Integer,
    var checkpoint: java.lang.Boolean,
    var context_context_e_id: java.lang.Long,
    var dtype: java.lang.String,
    var e_id: java.lang.Long,
    var explain: java.lang.Boolean,
    var expression: java.lang.String,
    var fieldname: java.lang.String,
    var finalexpression: java.lang.String,
    var initexpression: java.lang.String,
    var inputfacttypename: java.lang.String,
    var jointype: java.lang.String,
    var label: java.lang.String,
    var labelfieldname: java.lang.String,
    var mergeexpression: java.lang.String,
    var methodname: java.lang.String,
    var modelfile: java.lang.String,
    var name: java.lang.String,
    var outputport_outputport_e_id: java.lang.Long,
    var pivotfield: java.lang.String,
    var port_errorport_e_id: java.lang.Long,
    var port_inputport_e_id: java.lang.Long,
    var port_joineeport_e_id: java.lang.Long,
    var port_unionport_e_id: java.lang.Long,
    var resultfactname: java.lang.String,
    var resultfacttypename: java.lang.String,
    var resultqueryname: java.lang.String,
    var samplesize: java.lang.Integer,
    var sequencedname: java.lang.String,
    var sequencetype: java.lang.String,
    var statement: java.lang.String,
    var transformationstep_transformation_e_id: java.lang.Long,
    var transformation_transformationsteps_idx: java.lang.Integer,
    var userdefagg: java.lang.Boolean
) extends Serializable
case class Join4Schema(
    var dtype: java.lang.String,
    var transformation_id: java.lang.Long,
    var project_name: java.lang.String,
    var svncommitmessage: java.lang.String,
    var svnenabled: java.lang.Boolean,
    var svnpassword: java.lang.String,
    var svnurl: java.lang.String,
    var svnusername: java.lang.String,
    var auditinfo_auditinfo_e_id: java.lang.Long,
    var transformation_name: java.lang.String
) extends Serializable
case class Join9Schema(
    var project_name: java.lang.String,
    var transformation_name: java.lang.String,
    var changeuser: java.lang.String,
    var createuser: java.lang.String,
    var transformation_id: java.lang.Long
) extends Serializable
case class Spark_SQL13Schema(
    var project_name: java.lang.String,
    var transformation_name: java.lang.String,
    var changeuser: java.lang.String,
    var createuser: java.lang.String,
    var transformation_id: java.lang.Long,
    var batchsize: java.lang.Integer,
    var checkpoint: java.lang.Boolean,
    var context_context_e_id: java.lang.Long,
    var dtype: java.lang.String,
    var e_id: java.lang.Long,
    var explain: java.lang.Boolean,
    var expression: java.lang.String,
    var fieldname: java.lang.String,
    var finalexpression: java.lang.String,
    var initexpression: java.lang.String,
    var inputfacttypename: java.lang.String,
    var jointype: java.lang.String,
    var label: java.lang.String,
    var labelfieldname: java.lang.String,
    var mergeexpression: java.lang.String,
    var methodname: java.lang.String,
    var modelfile: java.lang.String,
    var name: java.lang.String,
    var outputport_outputport_e_id: java.lang.Long,
    var pivotfield: java.lang.String,
    var port_errorport_e_id: java.lang.Long,
    var port_inputport_e_id: java.lang.Long,
    var port_joineeport_e_id: java.lang.Long,
    var port_unionport_e_id: java.lang.Long,
    var resultfactname: java.lang.String,
    var resultfacttypename: java.lang.String,
    var resultqueryname: java.lang.String,
    var samplesize: java.lang.Integer,
    var sequencedname: java.lang.String,
    var sequencetype: java.lang.String,
    var statement: java.lang.String,
    var transformationstep_transformation_e_id: java.lang.Long,
    var transformation_transformationsteps_idx: java.lang.Integer,
    var userdefagg: java.lang.Boolean
) extends Serializable

}


object bp_rdbms_1Job {
   def main(args: Array[String]): Unit = {
     new bp_rdbms_1Job().sparkMain(args)
  }
}

