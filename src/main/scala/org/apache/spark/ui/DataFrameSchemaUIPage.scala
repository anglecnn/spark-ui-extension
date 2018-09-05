package org.apache.spark.ui

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import scala.xml.Node

import javax.servlet.http.HttpServletRequest
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset

/**
 * Created by KPrajapati on 5/21/2017.
 */
class DataFrameSchemaUIPage(parent: ExtendedUIServer) extends WebUIPage("") with Logging {

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    import scala.collection.JavaConversions._

    val content = <h4>The below table shows registered dataframes on the left, with there schemas on the
        right.</h4>
        <br/>
      <div>
        <table class="table table-bordered table-condensed" id="task-summary-table">
          <thead>
            <tr style="background-color: rgb(255, 255, 255);">
              <th width="50%" class="">DataFrame</th>
              <th width="50%" class="">Schema</th>
            </tr>
          </thead>
          <tbody>
            {Utility.schemas.map(x =>
            <tr style="background-color: rgb(249, 249, 249);">
              <td>{s"${x._1}"}</td>
              <td><pre>{s"${x._2}"}</pre></td>
            </tr>)}
          </tbody>
          <tfoot></tfoot>
        </table>
      </div>

    UIUtils.headerSparkPage(
      "This is the extension to Spark UI to display custom information about your application.",
      content, parent)
  }
}

object Utility {
  val schemas: ConcurrentMap[String, String] = new ConcurrentHashMap[String, String]()

  implicit class DataFrameSchema[T](df: Dataset[T]) {
    def registerSchema: Dataset[T] = {
        schemas.put(Thread.currentThread().getStackTrace.slice(2,4).mkString("\n"), df.schema
          .treeString)
      df
    }
  }

}