package org.ndp.port_scan_ns

import org.ndp.port_scan_ns.bean.*
import org.ndp.port_scan_ns.utils.RedisHandler
import org.w3c.dom.Node
import org.w3c.dom.NodeList
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathFactory

object Main {

    private lateinit var ports: String
    private val xPath = XPathFactory.newInstance().newXPath()
    private val task = RedisHandler.consumeTaskParam(
        RedisHandler.generateNonce(5)
    )

    private fun parseParam() {

        val param = task!!.param.split(";")
        val input = File("/input_file")
        input.writeText(param[0].replace(",", "\n"))
        ports = param[1]
        Log.debug("params: ")
        Log.debug(param[0])
        Log.debug(ports)
    }

    private fun execute() {
        Log.info("nmap start")
        val nmapBuilder =
            ProcessBuilder("nmap -Pn -n -sS -T5 --open -vv -oX /result.xml -p $ports -iL /input_file".split(" "))
        nmapBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT)
        nmapBuilder.redirectError(ProcessBuilder.Redirect.INHERIT)
        nmapBuilder.directory(File("/"))
        val nmap = nmapBuilder.start()
        nmap.waitFor()
        Log.info("nmap end")
    }

    private fun parseMidResult(): List<Host> {
        Log.info("parsing the result of nmap")
        val xml = File("/result.xml")
        val doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xml)
        val hostNodes = xPath.evaluate("//host", doc, XPathConstants.NODESET) as NodeList
        val hosts = ArrayList<Host>()
        for (i in 1..hostNodes.length) {
            val addr = (xPath.evaluate("//host[$i]//@addr", doc, XPathConstants.NODE) as Node).textContent
            val portNodes = xPath.evaluate("//host[$i]//port", doc, XPathConstants.NODESET) as NodeList
            val ports = ArrayList<Port>()
            for (j in 1..portNodes.length) {
                val state = (xPath.evaluate(
                    "//host[$i]/ports/port[$j]/state/@state",
                    doc,
                    XPathConstants.NODE
                ) as Node).textContent
                if (state != "open")
                    continue
                val protocol = (xPath.evaluate(
                    "//host[$i]/ports/port[$j]/@protocol",
                    doc,
                    XPathConstants.NODE
                ) as Node).textContent
                val portID = (xPath.evaluate(
                    "//host[$i]/ports/port[$j]/@portid",
                    doc,
                    XPathConstants.NODE
                ) as Node).textContent
                val service = (xPath.evaluate(
                    "//host[$i]/ports/port[$j]/service/@name",
                    doc,
                    XPathConstants.NODE
                ) as Node).textContent
                val product = (xPath.evaluate(
                    "//host[$i]/ports/port[$j]/service/@product",
                    doc,
                    XPathConstants.NODE
                ) as? Node)?.textContent ?: "unknown"
                ports.add(Port(protocol, portID, state, service, product))
            }
            if (ports.size > 0) {
                hosts.add(Host(addr, ports))
            }
        }
        Log.info("finished parsing")
        return hosts
    }

    @JvmStatic
    fun main(args: Array<String>) {
        Log.info("port-scan start")
        if (task == null || task.taskID == 0) {
            Log.info("no task, exiting...")
            return
        }
        try {
            // 获取配置
            parseParam()
            // 执行
            execute()
            // 解析中间文件，写结果
            RedisHandler.produceResult(
                MQResult(task.taskID, parseMidResult(), 0, "")
            )
        } catch (e: Exception) {
            Log.error(e.toString())
            val stringWriter = StringWriter()
            e.printStackTrace(PrintWriter(stringWriter))
            RedisHandler.produceResult(
                MQResult(task.taskID, ArrayList(), 1, stringWriter.buffer.toString())
            )
        }
        // 结束
        Log.info("port-scan end successfully")
    }
}
