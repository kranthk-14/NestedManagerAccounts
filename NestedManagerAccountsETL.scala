package com.amazon.ads.etl.nestedmanageraccounts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties
import scala.util.{Try, Success, Failure}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Nested Manager Accounts ETL Pipeline - Scala Implementation
 * 
 * This Scala application executes the complete Nested Manager Accounts ETL pipeline
 * with temporal inheritance logic for Amazon's advertising platform.
 * 
 * Features:
 * - Temporal inheritance (accounts become root PN managers after hierarchy ends)
 * - Three-union processing (hierarchy, standalone, group)
 * - Comprehensive deduplication and consolidation
 * - Production-ready with proper error handling and monitoring
 * 
 * @author Data Engineering Team
 * @version 2.0
 */
object NestedManagerAccountsETL {
  
  // Configuration constants
  val APP_NAME = "NestedManagerAccountsETL"
  val VERSION = "2.0.0"
  val MAX_HIERARCHY_LEVELS = 12
  val PROCESSING_BATCH_SIZE = 100000
  
  // Database configuration
  case class DatabaseConfig(
    jdbcUrl: String,
    username: String,
    password: String,
    driver: String = "com.amazon.redshift.jdbc42.Driver"
  )
  
  // ETL configuration
  case class ETLConfig(
    processingStartDate: String = "2020-01-01",
    processingEndDate: String = "2099-12-31",
    enableDebugLogging: Boolean = false,
    saveIntermediateTables: Boolean = false,
    maxRetries: Int = 3
  )
  
  /**
   * Main entry point for the ETL pipeline
   */
  def main(args: Array[String]): Unit = {
    println(s"Starting $APP_NAME v$VERSION at ${getCurrentTimestamp}")
    
    val config = parseCommandLineArgs(args)
    val dbConfig = loadDatabaseConfig()
    
    val spark = initializeSparkSession()
    
    try {
      val etlRunner = new ETLPipelineRunner(spark, dbConfig, config)
      val result = etlRunner.runCompleteETL()
      
      result match {
        case Success(metrics) =>
          println("ETL Pipeline completed successfully!")
          printExecutionMetrics(metrics)
        case Failure(exception) =>
          println(s"ETL Pipeline failed: ${exception.getMessage}")
          exception.printStackTrace()
          System.exit(1)
      }
      
    } finally {
      spark.stop()
    }
  }
  
  /**
   * Initialize Spark Session with optimized settings
   */
  def initializeSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(APP_NAME)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .getOrCreate()
  }
  
  /**
   * Parse command line arguments
   */
  def parseCommandLineArgs(args: Array[String]): ETLConfig = {
    var config = ETLConfig()
    
    args.sliding(2, 2).foreach {
      case Array("--start-date", value) => config = config.copy(processingStartDate = value)
      case Array("--end-date", value) => config = config.copy(processingEndDate = value)
      case Array("--debug", "true") => config = config.copy(enableDebugLogging = true)
      case Array("--save-intermediate", "true") => config = config.copy(saveIntermediateTables = true)
      case Array("--max-retries", value) => config = config.copy(maxRetries = value.toInt)
      case _ => // Ignore unknown arguments
    }
    
    config
  }
  
  /**
   * Load database configuration from environment variables or properties file
   */
  def loadDatabaseConfig(): DatabaseConfig = {
    val jdbcUrl = sys.env.getOrElse("REDSHIFT_JDBC_URL", 
      "jdbc:redshift://cluster.region.redshift.amazonaws.com:5439/database")
    val username = sys.env.getOrElse("REDSHIFT_USERNAME", "etl_user")
    val password = sys.env.getOrElse("REDSHIFT_PASSWORD", "")
    
    DatabaseConfig(jdbcUrl, username, password)
  }
  
  def getCurrentTimestamp: String = {
    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
  }
  
  def printExecutionMetrics(metrics: ETLMetrics): Unit = {
    println("\n" + "="*60)
    println("ETL EXECUTION METRICS")
    println("="*60)
    println(s"Total Processing Time: ${metrics.totalProcessingTimeMinutes} minutes")
    println(s"Records Processed: ${metrics.totalRecordsProcessed:,}")
    println(s"Final Output Records: ${metrics.finalOutputRecords:,}")
    println(s"Unique Manager Accounts: ${metrics.uniqueManagerAccounts:,}")
    println(s"Root PN Managers: ${metrics.rootPnManagers:,}")
    println(s"Max Hierarchy Depth: ${metrics.maxHierarchyDepth}")
    println(s"Data Quality Score: ${metrics.dataQualityScore}%")
    println("="*60)
  }
}

/**
 * Main ETL Pipeline Runner Class
 */
class ETLPipelineRunner(
  spark: SparkSession,
  dbConfig: DatabaseConfig,
  etlConfig: ETLConfig
) {
  
  import spark.implicits._
  
  /**
   * Execute the complete ETL pipeline
   */
  def runCompleteETL(): Try[ETLMetrics] = {
    val startTime = System.currentTimeMillis()
    
    Try {
      println("Step 1: Extracting and normalizing account relationships...")
      extractAndNormalizeRelationships()
      
      println("Step 2: Building hierarchy structure...")
      buildHierarchyStructure()
      
      println("Step 3: Implementing temporal inheritance logic...")
      implementTemporalInheritance()
      
      println("Step 4: Creating three-union processing...")
      createThreeUnionProcessing()
      
      println("Step 5: Final deduplication and consolidation...")
      finalDeduplicationAndConsolidation()
      
      println("Step 6: Validating results and generating metrics...")
      val metrics = validateAndGenerateMetrics(startTime)
      
      println("Step 7: Granting permissions and cleanup...")
      grantPermissionsAndCleanup()
      
      metrics
    }
  }
  
  /**
   * Step 1: Extract and normalize account relationships
   */
  def extractAndNormalizeRelationships(): Unit = {
    val sqlCommands = Seq(
      // Load SQL from file and execute each step
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 1"),
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 2")
    )
    
    sqlCommands.foreach(executeSQLCommand)
  }
  
  /**
   * Step 2: Build hierarchy structure
   */
  def buildHierarchyStructure(): Unit = {
    val sqlCommands = Seq(
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 3"),
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 4"),
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 5"),
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 6"),
      loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 7")
    )
    
    sqlCommands.foreach(executeSQLCommand)
  }
  
  /**
   * Step 3: Implement temporal inheritance logic
   */
  def implementTemporalInheritance(): Unit = {
    val sqlCommand = loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 8")
    executeSQLCommand(sqlCommand)
  }
  
  /**
   * Step 4: Create three-union processing
   */
  def createThreeUnionProcessing(): Unit = {
    val sqlCommand = loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 9")
    executeSQLCommand(sqlCommand)
  }
  
  /**
   * Step 5: Final deduplication and consolidation
   */
  def finalDeduplicationAndConsolidation(): Unit = {
    val sqlCommand = loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 10")
    executeSQLCommand(sqlCommand)
  }
  
  /**
   * Step 6: Validate results and generate metrics
   */
  def validateAndGenerateMetrics(startTime: Long): ETLMetrics = {
    val endTime = System.currentTimeMillis()
    val processingTimeMinutes = (endTime - startTime) / (1000 * 60)
    
    // Execute validation queries
    val totalRecords = executeCountQuery("SELECT COUNT(*) FROM test_nma_changes")
    val uniqueManagers = executeCountQuery("SELECT COUNT(DISTINCT manager_account_id) FROM test_nma_changes")
    val rootPnManagers = executeCountQuery(
      "SELECT COUNT(DISTINCT manager_account_id) FROM test_nma_changes WHERE root_pn_mngr_flg = 1"
    )
    val maxDepth = executeCountQuery("SELECT COALESCE(MAX(level__), 0) FROM aapt_dw.d_mngr_accnt_nma_hierarchy")
    
    // Calculate data quality score
    val dataQualityScore = calculateDataQualityScore()
    
    ETLMetrics(
      totalProcessingTimeMinutes = processingTimeMinutes,
      totalRecordsProcessed = totalRecords,
      finalOutputRecords = totalRecords,
      uniqueManagerAccounts = uniqueManagers,
      rootPnManagers = rootPnManagers,
      maxHierarchyDepth = maxDepth.toInt,
      dataQualityScore = dataQualityScore
    )
  }
  
  /**
   * Step 7: Grant permissions and cleanup
   */
  def grantPermissionsAndCleanup(): Unit = {
    val sqlCommand = loadSQLFromFile("nested_manager_accounts_etl.sql", "STEP 11")
    executeSQLCommand(sqlCommand)
    
    // Cleanup intermediate tables if not saving
    if (!etlConfig.saveIntermediateTables) {
      cleanupIntermediateTables()
    }
  }
  
  /**
   * Execute SQL command with retry logic
   */
  def executeSQLCommand(sql: String): Unit = {
    var attempts = 0
    var success = false
    
    while (attempts < etlConfig.maxRetries && !success) {
      attempts += 1
      
      Try {
        val connection = getConnection()
        try {
          val statement = connection.prepareStatement(sql)
          statement.execute()
          success = true
          
          if (etlConfig.enableDebugLogging) {
            println(s"Successfully executed SQL command (attempt $attempts)")
          }
        } finally {
          connection.close()
        }
      } match {
        case Success(_) => success = true
        case Failure(exception) =>
          println(s"SQL execution failed (attempt $attempts): ${exception.getMessage}")
          if (attempts == etlConfig.maxRetries) {
            throw exception
          }
          Thread.sleep(5000 * attempts) // Exponential backoff
      }
    }
  }
  
  /**
   * Execute count query and return result
   */
  def executeCountQuery(sql: String): Long = {
    val connection = getConnection()
    try {
      val statement = connection.prepareStatement(sql)
      val resultSet = statement.executeQuery()
      if (resultSet.next()) {
        resultSet.getLong(1)
      } else {
        0L
      }
    } finally {
      connection.close()
    }
  }
  
  /**
   * Calculate data quality score based on validation checks
   */
  def calculateDataQualityScore(): Double = {
    val checks = Seq(
      // Check for overlapping segments (should be 0)
      ("Overlapping Segments", """
        SELECT COUNT(*) FROM (
          SELECT t1.manager_account_id FROM test_nma_changes t1
          JOIN test_nma_changes t2 ON t1.manager_account_id = t2.manager_account_id
            AND t1.entity_id = t2.entity_id AND t1.effective_start_date != t2.effective_start_date
          WHERE t1.effective_start_date < t2.effective_end_date
            AND t1.effective_end_date > t2.effective_start_date
        ) overlaps
      """),
      
      // Check for cycles (should be 0)
      ("Hierarchy Cycles", """
        SELECT COUNT(*) FROM (
          SELECT h1.chld_manager_account_id FROM aapt_dw.d_mngr_accnt_nma_hierarchy h1
          JOIN aapt_dw.d_mngr_accnt_nma_hierarchy h2
            ON h1.chld_manager_account_id = h2.prnt_manager_account_id
            AND h2.chld_manager_account_id = h1.root_manager_account_id
        ) cycles
      """),
      
      // Check for invalid dates (should be 0)
      ("Invalid Dates", """
        SELECT COUNT(*) FROM test_nma_changes
        WHERE effective_start_date > effective_end_date
           OR effective_start_date IS NULL
           OR effective_end_date IS NULL
      """)
    )
    
    val failedChecks = checks.map { case (name, sql) =>
      val count = executeCountQuery(sql)
      if (count > 0) {
        println(s"Data Quality Issue: $name - $count issues found")
        1
      } else {
        0
      }
    }.sum
    
    val totalChecks = checks.length
    val passedChecks = totalChecks - failedChecks
    
    (passedChecks.toDouble / totalChecks.toDouble) * 100.0
  }
  
  /**
   * Load SQL from file and extract specific step
   */
  def loadSQLFromFile(filename: String, stepName: String): String = {
    val source = scala.io.Source.fromFile(filename)
    try {
      val content = source.mkString
      extractSQLStep(content, stepName)
    } finally {
      source.close()
    }
  }
  
  /**
   * Extract specific step from SQL content
   */
  def extractSQLStep(content: String, stepName: String): String = {
    val stepPattern = s"-- =+ $stepName.*?(?=-- =+|$$)".r
    stepPattern.findFirstIn(content).getOrElse("")
  }
  
  /**
   * Get database connection
   */
  def getConnection(): Connection = {
    Class.forName(dbConfig.driver)
    DriverManager.getConnection(dbConfig.jdbcUrl, dbConfig.username, dbConfig.password)
  }
  
  /**
   * Cleanup intermediate tables
   */
  def cleanupIntermediateTables(): Unit = {
    val intermediateTables = Seq(
      "acc_links__",
      "normalized_relationships", 
      "resolved_relationships",
      "src_global_enty_rltnshp_hier_new",
      "subordinate___r",
      "subordinate__rpm",
      "d_mngr_accnt_nma_hierarchy_rtpn_",
      "final_segments",
      "dim_prtnr_adv_entt_mpng_nma__"
    )
    
    intermediateTables.foreach { table =>
      Try {
        executeSQLCommand(s"DROP TABLE IF EXISTS $table")
      } match {
        case Success(_) => println(s"Cleaned up intermediate table: $table")
        case Failure(_) => println(s"Failed to cleanup table: $table (may not exist)")
      }
    }
  }
}

/**
 * Data class for ETL execution metrics
 */
case class ETLMetrics(
  totalProcessingTimeMinutes: Long,
  totalRecordsProcessed: Long,
  finalOutputRecords: Long,
  uniqueManagerAccounts: Long,
  rootPnManagers: Long,
  maxHierarchyDepth: Int,
  dataQualityScore: Double
)

/**
 * Companion object with utility functions
 */
object ETLUtils {
  
  /**
   * Validate environment setup
   */
  def validateEnvironment(): Boolean = {
    val requiredEnvVars = Seq("REDSHIFT_JDBC_URL", "REDSHIFT_USERNAME", "REDSHIFT_PASSWORD")
    val missingVars = requiredEnvVars.filter(sys.env.get(_).isEmpty)
    
    if (missingVars.nonEmpty) {
      println(s"Missing required environment variables: ${missingVars.mkString(", ")}")
      false
    } else {
      true
    }
  }
  
  /**
   * Generate execution report
   */
  def generateExecutionReport(metrics: ETLMetrics): String = {
    s"""
    |Nested Manager Accounts ETL Execution Report
    |==========================================
    |
    |Execution Time: ${metrics.totalProcessingTimeMinutes} minutes
    |Records Processed: ${metrics.totalRecordsProcessed:,}
    |Final Output Records: ${metrics.finalOutputRecords:,}
    |Unique Manager Accounts: ${metrics.uniqueManagerAccounts:,}
    |Root PN Managers: ${metrics.rootPnManagers:,}
    |Max Hierarchy Depth: ${metrics.maxHierarchyDepth}
    |Data Quality Score: ${metrics.dataQualityScore}%
    |
    |Status: ${if (metrics.dataQualityScore >= 95.0) "SUCCESS" else "WARNING - Low Data Quality"}
    |
    """.stripMargin
  }
}
