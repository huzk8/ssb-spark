package main.scala

/**
 * SSB Query 2.2
 */
class Q22 extends SSBQuery {

    import spark.implicits._

    override def execute(): Unit = {

        var sql = """select sum(lo_revenue), d_year, p_brand
	from lineorder
	join date
	  on lo_orderdatekey = d_datekey
	join part
	  on lo_partkey = p_partkey
	join supplier
	  on lo_suppkey = s_suppkey
	where 
	p_brand between 'MFGR#2221' and 'MFGR#2228'
	and s_region = 'ASIA'
	group by d_year, p_brand
	order by d_year, p_brand"""

        val res = spark.sql(sql)
        outputDF(res)
    }

}
