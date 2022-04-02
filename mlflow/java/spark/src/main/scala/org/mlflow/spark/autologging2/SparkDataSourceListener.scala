package org.mlflow.spark.autologging2

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.slf4j.LoggerFactory


/**
 * Implementation of the SparkListener interface used to detect Spark datasource reads.
 * and notify subscribers.
 */
class SparkDataSourceListener(
    publisher: MlflowAutologEventPublisherImpl = MlflowAutologEventPublisher) extends SparkListener {
  protected val logger = LoggerFactory.getLogger(getClass)

  protected def getDatasourceAttributeExtractor: DatasourceAttributeExtractorBase = {
    DatasourceAttributeExtractor
  }

  protected def getReplIdOpt(event: SparkListenerSQLExecutionEnd): Option[String] = {
    println(f"SparkDataSourceListener.getReplIdOpt returning None")
    None
  }

  // Exposed for testing
  private[autologging2] def onSQLExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    println(f"SparkDataSourceListener.onSQLExecutionEnd called with event: ${event}")
    val replIdOpt = getReplIdOpt(event)  // Workaround attempt - if there are multiple tableInfos, then this will cause issues since getReplIdOpt removes it from the map
    println(f"SparkDataSourceListener.getReplIdOpt: ${replIdOpt} for event: ${event}")
    val extractor = getDatasourceAttributeExtractor
    val tableInfos = extractor.getTableInfos(event)
    tableInfos.foreach { tableInfo =>
      println(f"SparkDataSourceListener.publishEvent replIdOpt: ${replIdOpt} tableInfo: ${tableInfo}")
      publisher.publishEvent(replIdOpt, tableInfo)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionEnd =>
        // Defensively catch exceptions while attempting to extract datasource read information
        // from the SparkListenerSQLExecutionEnd event. In particular, we do this to defend
        // against changes in the internal APIs we access (e.g. changes in Delta table classnames
        // or removal of the QueryExecution field from SparkListenerSQLExecutionEnd) in future
        // Spark versions. As of the time of writing, Spark seems to also catch these exceptions,
        // but we defensively catch here to be safe & give the user a better error message.
        ExceptionUtils.tryAndLogUnexpectedError(
          logger, "when attempting to handle SparkListenerSQLExecutionEnd event", {
          onSQLExecutionEnd(e)
        })
      case _ =>
    }
  }
}
