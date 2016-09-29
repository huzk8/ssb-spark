package main.scala

/**
 * SSB Query 1.2
 */
class Q12 extends SSBQuery {

    import spark.implicits._

    override def execute(): Unit = {

        var sql = """select sum(lo_extendedprice*lo_discount) as
	revenue
	from lineorder join date on lo_orderdatekey = d_datekey
	where 
	d_yearmonthnum = 199401
	and lo_discount between 4 and 6
	and lo_quantity between 26 and 35"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
