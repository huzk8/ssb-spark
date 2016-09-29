package main.scala

/**
 * SSB Query 1.1
 */
class Q11 extends SSBQuery {

    import spark.implicits._

    override def execute(): Unit = {

        var sql = """select sum(lo_extendedprice*lo_discount) as revenue
        from lineorder join date on lo_orderdatekey = d_datekey
        where
        d_year = 1993
        and lo_discount between 1 and 3
        and lo_quantity < 25"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
