package it.banzaicommerce.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.file.Path
import java.net.URL
import java.nio.file.Paths
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameReader
import java.util.Date

object App {

  def main(args: Array[String]) {
    
    val resource = this.getClass.getResource("/TD_OD_Vincenzo20170413.csv")

    println(resource.toString());
    val path = Paths.get(resource.toURI());

    println(path);
    
    val conf = new SparkConf()
      .setMaster("local[8]")
      .setAppName("lorenzo-app")
    //  .set("mode", "cluster")
    //  .set("spark.executor.memory", "4g")
    //  .set("spark.num.executors", "4")
    //  .set("spark.cores.max","4")
    val sc = new SparkContext(conf)
    
    val ss = SparkSession.builder().config(conf).getOrCreate()
   
 
//    SQLContext sqlContext = SQLContext.getOrCreate(sc);
    val df = ss.sqlContext.read.option("header","true").csv(path.toString())
    
    df.createOrReplaceTempView("TD_OD_Vincenzo20170413")
  
    df.show()

    val sqlText = "SELECT " +
                  " case when a.marketplace_order_flag=1 and a.order_date != null then year(a.order_date) else year(a.shipping_date) end as group_date, " +
                  " order_date, shipping_date, flag_safety, order_status, order_line_status, revenue, marketplace_order_flag, ordine_cliente_hk, ordine_cliente_riga_hk, cliente_hk, quantity, shipping_cost, cost_of_goods \n" +
                  " FROM TD_OD_Vincenzo20170413 a" +
                  " WHERE (a.flag_safety = 1 OR (a.marketplace_order_flag = 1 AND a.order_status = 'Evaso' AND a.order_line_status = 'Spedito')) "

                  
    val df2 = ss.sqlContext.sql(sqlText)
    
    df2.createOrReplaceTempView("TD_OD_Vincenzo")
                  //
    df2.show();
    System.out.println(df2.count());
    //    
    val sqlText2 = "" +
         "SELECT \n" +
//         "DATE_PART('yy', case when a.marketplace_order_flag=1 then a.order_date else a.shipping_date end) as gvm_date" +
     " group_date \n" +
     ", MAX(shipping_date) as AL \n" +
     ", SUM(revenue) AS revenue_goods \n" +
     ", SUM( case flag_safety when 1 then revenue end) AS site_revenue_goods \n" +
     ", SUM( case marketplace_order_flag when 1 then revenue end) AS marketplace_revenue_goods \n" +
     ", COUNT(DISTINCT ordine_cliente_hk) AS orders \n" +
     ", COUNT(DISTINCT case flag_safety when 1 then ordine_cliente_hk end) AS site_orders \n" +
     ", COUNT(DISTINCT case marketplace_order_flag when 1 then ordine_cliente_hk end) AS marketplace_orders \n" +
     ", COUNT(DISTINCT cliente_hk) AS buyers\n" +
     ", COUNT(DISTINCT case when a.flag_safety =1 then cliente_hk end) AS site_buyers \n" +
     ", COUNT(DISTINCT case when a.marketplace_order_flag=1 then cliente_hk end) AS marketplace_buyers \n" +
     ", SUM( quantity) AS quantity \n" +
     ", SUM( case when a.flag_safety =1 then quantity end) AS site_quantity \n" +
     ", SUM( case when a.marketplace_order_flag=1 then quantity end) AS marketplace_quantity \n" +
     ", SUM( shipping_cost) AS shipping_revenue \n" +
     ", SUM( case when a.flag_safety =1 then shipping_cost end) AS site_shipping_revenue \n" +
     ", SUM( case when a.marketplace_order_flag=1 then shipping_cost end) AS marketplace_shipping_revenue \n" +
     ", SUM( case when cost_of_goods>0 and revenue>0 then revenue end) AS revenue_goods_per_margin \n" +
     ", SUM( case when cost_of_goods>0 and revenue>0 and a.flag_safety =1 then revenue end) AS site_revenue_goods_per_margin \n" +
     ", SUM( case when cost_of_goods>0 and revenue>0 and a.marketplace_order_flag =1 then revenue end) AS marketplace_revenue_goods_per_margin \n" +
     ", SUM( case when cost_of_goods>0 and revenue>0 then cost_of_goods end) AS cost_of_goods \n" +
     ", SUM( case when cost_of_goods>0 and revenue>0 and a.flag_safety =1 then cost_of_goods end) AS site_cost_of_goods \n" +
     ", SUM( case when cost_of_goods>0 and revenue>0 and a.marketplace_order_flag =1 then cost_of_goods end) AS marketplace_cost_of_goods \n" +
     ", COUNT(DISTINCT ordine_cliente_riga_hk) AS order_lines \n" +
     " FROM TD_OD_Vincenzo a" +
     " WHERE (a.flag_safety =1 OR (a.marketplace_order_flag=1 and a.order_status='Evaso' and a.order_line_status='Spedito'))" +
     " GROUP BY\n" + 
        " group_date\n" +
     " ORDER BY group_date DESC"
//
////    sqlContext.functionRegistry().registerFunction(arg0, arg1);
     
     def myFunc: (String => String) = { s => s.take(4) }

     import org.apache.spark.sql.functions.udf
     val myUDF = udf(myFunc)

     val output = ss.sqlContext.sql(sqlText2)
     
//     output.withColumn("newCol", myUDF(output("order_date")))

     output.createOrReplaceTempView("TD_OUTPUT")
     output.show();

     println(output.count());

  }

}