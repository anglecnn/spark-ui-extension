package org.apache.spark.ui

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.ui.ExtendedUIServer._

/**
 * Created by KPrajapati on 5/21/2017.
 */
class ExtendedUIServer(sparkContext: SparkContext)
  extends SparkUITab(getSparkUI(sparkContext), "dataframeschema")
    with Logging {

  override val name = "Dataframe Schema"

  val parent: SparkUI = getSparkUI(sparkContext)

  attachPage(new DataFrameSchemaUIPage(this))
  parent.attachTab(this)

  def detach() {
    getSparkUI(sparkContext).detachTab(this)
  }
}

object ExtendedUIServer {
  def getSparkUI(sparkContext: SparkContext): SparkUI = {
    sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
