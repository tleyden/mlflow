package org.mlflow.spark.autologging2

import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Implementation of the SparkListener interface used to detect Spark datasource reads.
 * and notify subscribers. Used in REPL-ID aware environments (e.g. Databricks)
 */
class ReplAwareSparkDataSourceListener(
    publisher: MlflowAutologEventPublisherImpl = MlflowAutologEventPublisher)
  extends SparkDataSourceListener(publisher) {
  private val executionIdToReplId = mutable.Map[Long, String]()

  override protected def getDatasourceAttributeExtractor: DatasourceAttributeExtractorBase = {
    ReplAwareDatasourceAttributeExtractor
  }

  private[autologging2] def getProperties(event: SparkListenerJobStart): Map[String, String] = {
    Option(event.properties).map(_.asScala.toMap).getOrElse(Map.empty)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    println(f"ReplAwareSparkDataSourceListener onJobStart called")
    val properties = getProperties(event)
    // too much output println(f"event properties: ${properties}. SparkListenerJobStart event: ${event}")
    val executionIdOpt = properties.get(SQLExecution.EXECUTION_ID_KEY).map(_.toLong)
    println(f"executionIdOpt: ${executionIdOpt}")
    if (executionIdOpt.isEmpty) {
      try {
        println(f"warning, executionIdOpt is empty for event (first 10k chars) ${event.toString.take(10000)}")
      } catch {
        case e: _ => println("Got exception printing event.")
      }
      logger.warn(s"Unable to find execution ID of current Spark Job, " +
        s"refusing to autolog datasource reads performed within current Spark job")
      return
    }
    val executionId = executionIdOpt.get
    val replIdOpt = properties.get("spark.databricks.replId")
    if (replIdOpt.isEmpty) {
      println(f"replIdOpt: ${replIdOpt} is empty, not putting in ${executionIdToReplId}")
      logger.warn(s"Unable to find ID of REPL that triggered current Spark Job (execution ID " +
        s"$executionId), " +
        s"refusing to autolog datasource reads performed within current Spark job")
      return
    }
    println(f"put replIdOpt: ${replIdOpt} for executionIdOpt ${executionIdOpt}")
    executionIdToReplId.put(executionId, replIdOpt.get)
  }

  override protected def getReplIdOpt(event: SparkListenerSQLExecutionEnd): Option[String] = {
    println(f"remove event.executionId: ${event.executionId} from executionIdToReplId map")
    executionIdToReplId.remove(event.executionId)
  }
}
