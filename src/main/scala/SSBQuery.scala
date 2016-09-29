package main.scala

import java.io.File
import org.apache.spark.sql._

// SSB table schemas
case class Customer(
    c_customerkey: Int,
    c_name: String,
    c_address: String,
    c_city: String,
    c_nation: String,
    c_region: String,
    c_phone: String,
    c_mktsegment: String)

case class Part(
    p_partkey: Int,
    p_name: String,
    p_mfgr: String,
    p_category: String,
    p_brand: String,
    p_colour: String,
    p_type: String,
    p_size: Int,
    p_container: String)

case class Supplier(
    s_suppkey: Int,
    s_name: String,
    s_address: String,
    s_city: String,
    s_nation: String,
    s_region: String,
    s_phone: String)

case class Date(
    d_datekey: Int,
    d_date: String,
    d_dayofweek: String,
    d_month: String,
    d_year: Int,
    d_yearmonthnum: Int,
    d_yearmonth: String,
    d_daynuminweek: Int,
    d_daynuminmonth: Int,
    d_daynuminyear: Int,
    d_monthnuminyear: Int,
    d_weeknuminyear: Int,
    d_sellingseason: String,
    d_lastdayinweekfl: Int,
    d_lastdayInmonthfl: Int,
    d_holidayfl: Int,
    d_weekdayfl: Int)

case class LineOrder(
    lo_orderkey: Int,
    lo_linenumber: Int,
    lo_custley: Int,
    lo_partkey: Int,
    lo_suppkey: Int,
    lo_orderdatekey: Int,
    lo_orderpriority: String,
    lo_shippriority: String,
    lo_quantity: Int,
    lo_extendedprice: Double,
    lo_ordTotalprice: Double,
    lo_discount: Double,
    lo_revenue: Double,
    lo_supplycost: Double,
    lo_tax: Int,
    lo_commitdatekey: Int,
    lo_shipmode: String)

/**
 * Parent class for SSB queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Inspired by - Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class SSBQuery {

    // read files from local FS
    val INPUT_DIR = "file://" + new File(".").getAbsolutePath() + "/ssb-dbgen"

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    val OUTPUT_DIR: String = null

    // get the name of the class excluding dollar signs and package
    val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")

    val spark = SparkSession.builder().appName("SSB " + className).getOrCreate()
    import spark.implicits._

    // customer table
    val customer = spark.sparkContext.textFile(INPUT_DIR + "/customer.tbl").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim, p(7).trim)).toDF()
    // register the DataFrame as a temporary view
    customer.createOrReplaceTempView("customer")

    // part table
    val part = spark.sparkContext.textFile(INPUT_DIR + "/part.tbl").map(_.split('|')).map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)).toDF()
    part.createOrReplaceTempView("part")

    // supplier table
    val supplier = spark.sparkContext.textFile(INPUT_DIR + "/supplier.tbl").map(_.split('|')).map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).trim)).toDF()
    supplier.createOrReplaceTempView("supplier")

    // date table
    val date = spark.sparkContext.textFile(INPUT_DIR + "/date.tbl").map(_.split('|')).map(p => Date(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim.toInt, p(5).trim.toInt, p(6).trim, p(7).trim.toInt, p(8).trim.toInt, p(9).trim.toInt, p(10).trim.toInt, p(11).trim.toInt,p(12).trim, p(13).trim.toInt, p(14).trim.toInt, p(15).trim.toInt, p(16).trim.toInt )).toDF()
    date.createOrReplaceTempView("date")

    // lineorder table
   val lineorder = spark.sparkContext.textFile(INPUT_DIR + "/lineorder.tbl").map(_.split('|')).map(p => LineOrder(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toInt, p(5).trim.toInt, p(6).trim, p(7).trim, p(8).trim.toInt, p(9).trim.toDouble, p(10).trim.toDouble, p(11).trim.toDouble, p(12).trim.toDouble, p(13).trim.toDouble, p(14).trim.toInt, p(15).trim.toInt, p(16).trim)).toDF()
   lineorder.createOrReplaceTempView("lineorder")

    /**
     *  implemented in children classes and hold the actual query
     */
    def execute(): Unit

    def outputDF(df: DataFrame): Unit = {

        if (OUTPUT_DIR == null || OUTPUT_DIR == "")
            df.collect().foreach(println)
        else
            df.write.mode("overwrite").json(OUTPUT_DIR + "/" + className + ".out") // json to avoid alias
    }
}

object SSBQuery {

    /**
     * Execute query reflectively
     */
  def executeQuery(queryNo: Int): Unit = {
    assert(queryNo >= 11 && queryNo <= 43, "Invalid query number")
    Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[{ def execute }].execute
  }

    def main(args: Array[String]): Unit = {
        if (args.length == 1)
            executeQuery(args(0).toInt)
        else
            throw new RuntimeException("Invalid number of arguments")
    }
}
