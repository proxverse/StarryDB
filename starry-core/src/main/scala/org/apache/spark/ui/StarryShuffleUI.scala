package org.apache.spark.ui

import org.apache.spark.StarryEnv
import org.apache.spark.util.Utils

import javax.servlet.http.HttpServletRequest
import scala.xml.{Elem, Node}
class StarryMemoryUI(parent: SparkUI) extends SparkUITab(parent, "StarryShuffleMemory") {
  attachPage(new StarryMemoryPage(this))
}

private[ui] class StarryMemoryPage(parent: SparkUITab) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val statics = StarryEnv.get.shuffleManagerMaster.fetchAllMemory()

    val spans = statics.map(tp =>
      createSpan(s"${tp._1}-shuffle", tp._2.map(tp => (tp._1.toString, tp._2.toString))))
    val context =
      <span>
        <div class="form-inline">
          <div class="bs-example" data-example-id="simple-form-inline">
            <div class="form-group">
              <div class="input-group">
                <label class="mr-2" for="search">Search:</label>
                <input type="text" class="form-control" id="search" oninput="onSearchStringChange()"></input>
              </div>
            </div>
          </div>
        </div>
        {spans}
      </span>
    UIUtils.headerSparkPage(request, "Environment", context, parent)
  }

  private def createSpan(title: String, info: Map[String, String]): Elem = {
    val nodes = UIUtils.listingTable(
      propertyHeader,
      jvmRow,
      info.toSeq.sorted,
      fixedWidth = false,
      headerClasses = headerClasses)
    val headClassString = headClass(title)
    val headFunctionString = headClick(title)
    val contextString = contextClass(title)
    <span>
      <span class={headClassString}
            onClick={headFunctionString}>
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>
            {title}</a>
        </h4>
      </span>
      <div class={contextString}>
        {nodes}
      </div>
    </span>

  }

  private def headClass(name: String) = s"collapse-${name} collapse-table"
  private def headClick(name: String) = s"collapseTable('collapse-${name}','aggregated-${name}')"
  private def contextClass(name: String) = s"aggregated-${name} collapsible-table"

  private def resourceProfileHeader = Seq("Resource Profile Id", "Resource Profile Contents")
  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeader = Seq("Resource", "Source")
  private def headerClasses = Seq("sorttable_alpha", "sorttable_alpha")
  private def headerClassesNoSortValues = Seq("sorttable_numeric", "sorttable_nosort")

  private def jvmRowDataPre(kv: (String, String)) =
    <tr><td>{kv._1}</td><td><pre>{kv._2}</pre></td></tr>
  private def jvmRow(kv: (String, String)) =
    <tr id= "thread_1_tr" ><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}
