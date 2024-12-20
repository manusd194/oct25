import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, current_date, datediff, initcap, lag, lead, length, max, min, rank, stddev, sum, to_date, when}
import org.apache.spark.sql.types.DateType

import scala.util.control.Exception.noCatch.desc

object coding_set_groupby {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name", "lead_and_lag")
    conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf = conf)
      .getOrCreate()

    import spark.implicits._

    //    1. Count Items per Category
    //    Question: Count the number of products in each category.
    val items = List(("Electronics", "Laptop"), ("Electronics", "Phone"), ("Clothing", "T-Shirt"), ("Clothing", "Jeans"), ("Furniture", "Chair")
    ).toDF("category", "product")
    //items.groupBy(col("category")).agg(count("product")).show()

    //spark-sql
//    items.createOrReplaceTempView("q1")
//    spark.sql(
//      """
//         select category, count(product) from q1 group by category
//    """
//    ).show()

    //    2. Find Minimum, Maximum, and Average Price per Product
    //    Sample Data:
       val items_1 = List(("Laptop",1000), ("Phone",500), ("T-Shirt",20), ("Jeans",50), ("Chair",150))toDF("product","price")
//    items_1.groupBy(col("product")).agg(min(col("price"))).show()
//    items_1.groupBy(col("product")).agg(max(col("price"))).show()
//    items_1.groupBy(col("product")).agg(avg(col("price"))).show()

    //spark-sql
//    items_1.createOrReplaceTempView("q2")
//    spark.sql(
//      """
//        select product, min(price), max(price), avg(price) from q2 group by product
//        """).show()

    //    3. Group Sales by Month and Year
        val sales= List(("2023-01-01","New York",100), ("2023-02-15","London",200), ("2023-03-10","Paris",300), ("2023-04-20","Berlin",400),
          ("2023-05-05","Tokyo",500)).toDF("order_date","city","amount")
        //sales.groupBy(col("order_date")).agg(sum(col("amount"))).show()
        sales.groupBy(col("order_date")).agg(sum(col("amount")), avg(col("amount")), min(col("amount"))).show()

    
    //spark-sql
//    sales.createOrReplaceTempView("salessql")
//    spark.sql(
//      """
//        select order_date, sum(amount) from salessql group by order_date
//        """).show()

    //    4. Find Top N Products by Sales
        val order = List(("Laptop","order_1",2), ("Phone","order_2",1), ("T-Shirt","order_1",3), ("Jeans","order_3",4), ("Chair","order_2",2)
        )toDF("product","order_id","quantity")
      //order.orderBy(col("quantity").desc).show(5)

    //spark-sql
//    order.createOrReplaceTempView("ordersql")
//    spark.sql(
//      """
//        select product, quantity from ordersql order by quantity desc limit 4
//        """).show()

    //    5. Calculate Average Rating per User
        val rating = List((1,1,4), (1,2,5), (2,1,3), (2,3,4), (3,2,5))toDF("user_id","product_id","rating")
        //rating.groupBy(col("user_id")).agg(avg(col("rating"))).show()

    //spark-sql
//    rating.createOrReplaceTempView("ratingsql")
//    spark.sql(
//      """
//        select user_id, avg(rating) from ratingsql group by user_id
//        """).show()


    // 6. Group Customers by Country and Calculate Total Spend
        val spend = List((1,"USA","order_1",100), (1,"USA","order_2",200), (2,"UK","order_3",150), (3,"France","order_4",250),
          (3,"France","order_5",300))toDF("customer_id","country","order_id","amount")
        //spend.groupBy(col("country")).agg(sum(col("amount"))).show()

    //spark-sql
//    spend.createOrReplaceTempView("spendsql")
//    spark.sql(
//      """
//        select country, sum(amount) from spendsql group by country
//        """).show()

    //    7. Find Products with No Sales in a Specific Time Period
    //    Question: Identify products that had no sales between 2023-02-01 and 2023-03-31
        val order_date = List(("Laptop","2023-01-01"), ("Phone","2023-02-15"), ("T-Shirt","2023-03-10"), ("Jeans","2023-04-20")
        )toDF("product",("order_date"))
        val order_date_1 = order_date.withColumn("order_date",to_date(col("order_date")))
        //order_date_1.printSchema()
        //order_date_1.filter(col("order_date")<="2023-02-01" and col("order_date")>="2023-03-31").show()   /// CHECK LATER ///

    //    8. Calculate Order Count per Customer and City
    val customer = List((1,"New York","order1"), (1,"New York","order"),(1,"York","order2"),(2,"New York","order3"),(3,"CA","order4"),
      (4,"DT","order5"))toDF("customer_id","city","order_id")
    //customer.groupBy(col("customer_id"),col("city")).agg(count(col("order_id"))).show()

    //spark-sql
//    customer.createOrReplaceTempView("customersql")
//    spark.sql(
//      """
//        select customer_id,city, count(order_id) from customersql group by customer_id,city order by customer_id
//        """).show()

3
    //      9. Group Orders by Weekday and Calculate Average Order Value (when-otherwise)
    //    Question: Group orders by weekday (use dayofweek) and calculate the average order value for weekdays and weekends using when and otherwise.
          val order_value = List(("2023-04-10",1,100), ("2023-04-11",2,200), ("2023-04-12",3,300), ("2023-04-13",1,400), ("2023-04-14",2,500)
          )toDF("order_date","customer_id","amount")

    //spark-sql



    //    10. Filter Products Starting with "T" and Group by Category with Average Price
    //      Question: Filter products starting with "T" and group them by category, calculating the average price for each category.
    val clothes = List(("T-Shirt","Clothing",20),("Table","Furniture",150),("Jeans","Clothing",50),("Chair","Furniture",100)
    )toDF("product","category","price")
    val clothes_1=clothes.filter(col("product").startsWith("T"))
    //clothes_1.groupBy(col("category")).agg(avg(col("price"))).show()

    //    11. Find Customers Who Spent More Than $200 in Total
    //    Question: Group customers by customer ID and calculate the total amount spent. Filter customers who spent more than $200 in total.
    val customer_200 = List((1,"order_1",100),(1,"order_2",150),(2,"order_3",250),(3,"order_4",100),(3,"order_5",120)
    )toDF("customer_id","order_id","amount")
    //customer_200.groupBy(col("customer_id")).agg(sum(col("amount")) as "total").filter(col("total") >200).show()

    //    12. Create a New Column with Order Status ("High" for > $100, "Low" Otherwise)
    //    Question: Group orders by order ID and create a new column named "order_status" with values
    //    "High" for orders with an amount greater than $100, and "Low" otherwise, using withColumn.
    val order_status = List(("order_1",150),("order_2",80),("order_3",220),("order_4",50))toDF("order_id","amount")
    val order_status_1 = order_status.withColumn("order_status", when(col("amount")>100, "High").otherwise("Low"))
    //order_status_1.orderBy(col("order_id")).show()


    //    13. Select Specific Columns and Apply GroupBy with Average
    //    Question: Select only "product" and "price" columns, then group by "product" and calculate the average price.
    val product_2 = List(("Laptop","Electronics",1000,2),("Phone","Electronics",500,1),("T-Shirt","Clothing",20,3),("Jeans","Clothing",50,4)
    )toDF("product","category","price","quantity")
    val product_3 = product_2.select(col("product"),col("price"))
    //product_3.groupBy(col("product")).agg(avg(col("price"))).show()

    //    14. Count Orders by Year and Month with Aggregation Functions (count, sum)
    //    Question: Group orders by year and month, and calculate the total number of orders and total amount for each month-year combination

    val order_by_date = List(("2023-01-01",1,100),("2023-02-15",2,200),("2023-03-10",3,300),("2023-04-20",1,400),("2023-05-05",2,500)
    )toDF("order_date","customer_id","amount")


    //    15. Find Products with Highest and Lowest Sales in Each Category (Top N)
       // Question: Group by category and find the top 2 products (by total quantity sold) within each category.
    val product_category = List(("Laptop","Electronics",2),("Phone","Electronics",1),("T-Shirt","Clothing",3),("Jeans","Clothing",4),
      ("Chair","Furniture",2),("Sofa","Furniture",1))toDF("product","category","quantity")
    val product_category_1 = product_category.groupBy(col("category"),col("product")).agg(sum(col("quantity")) as "sum_total")
    //product_category_1.orderBy(col("sum_total").desc).limit(2).show()

    //    16. Calculate Average Rating per Product, Weighted by Quantity Sold
    //    Question: Group by product ID, calculate the average rating, weighted by the quantity sold for each order.
    val quantity_sold = List((1,"order_1",4,2),(1,"order_2",5,1),(2,"order_3",3,4),(2,"order_4",4,3),(3,"order_5",5,1)
    )toDF("product_id","order_id","rating","quantity")


    //    17. Find Customers Who Placed Orders in More Than Two Different Months
    //      Question: Group by customer ID and count the distinct number of months in which they placed
    //    orders. Filter customers who placed orders in more than two months.
    val customer_order = List((1,"2023-01-01"),(1,"2023-02-15"),(2,"2023-03-10"),(2,"2023-03-20"),(3,"2023-04-20"),(3,"2023-05-05")
    )toDF("customerID","date")


    // 18. Group by Country and Calculate Total Sales, Excluding Orders Below $50
    // Question: Group by country, calculate the total sales amount, excluding orders with an amount less than $50.
    val order_country = List(("USA","order_1",100),("USA","order_2",40),("UK","order_3",150),("France","order_4",250),("France","order_5",30)
    )toDF("country","order_id","amount")
    val order_country_1 = order_country.filter(col("amount")>50)
    //order_country_1.groupBy(col("country")).agg(sum(col("amount"))).show()


    //    19. Find Products Never Ordered Together (Pairwise Co-occurrence)
    //    Question: Identify product pairs that never appeared together in any order. This may require self-joins or other techniques for
    //    pairwise comparisons. (Repeat order to avoid self-joins)
    val never_order = List(("order_1",1,2),("order_2",1,3),("order_3",2,4),("order_4",3,1))toDF("order_id","product_id1","product_id2")



    //  20. Group by Category and Calculate Standard Deviation of Price
    //  Question: Group by category and calculate the standard deviation of price for each category.
    val category_deviation = List(("Laptop","Electronics",1000),("Phone","Electronics",500),("T-Shirt","Clothing",20),("Jeans","Clothing",50),
      ("Chair","Furniture",150),("Sofa","Furniture",200))toDF("product","category","price")
    //category_deviation.groupBy(col("category")).agg(stddev("price")).show()


    //  21. Find Most Frequent Customer City Combinations
    //  Question: Group by customer_id and city, and find the most frequent city for each customer.
    val customer_city = List((1,"New York"),(1,"New York"),(2,"London"),(2,"Paris"),(3,"Paris"),(3,"Paris"))toDF("customer_id","city")
    //customer_city.groupBy(col("customer_id"),col("city")).agg(count(col("city"))).show()

//22. Calculate Customer Lifetime Value (CLTV) by Year.
//Group by customer_id and year (use year), calculate the total amount spent for each customer in each year.This can be used to calculate CLTV.
    val CLTV = List((1,"2022-01-01",100),(1,"2023-02-15",200),(2,"2022-03-10",300),(2,"2023-04-20",400),(3,"2022-05-05",500),(3,"2023-06-06",600)
    )toDF("customer_id","order_date","amount")

    //CLTV.groupBy(col("customer_id"),col(""))

//    23. Find Products with a Decline in Average Rating Compared to Previous Month
// Question: Group by product_id and month (use month), calculate the average rating for each product in each month. Identify products
// with a decrease in average rating compared to the previous month.
    val avg_rating = List(
      (1,"2023-01-01",4), (1,"2023-02-15",3), (2,"2023-01-10",5), (2,"2023-02-20",4), (3,"2023-01-20",4), (3,"2023-02-25",5)
    )toDF("product_id","order_date","rating")
    val window_avg_rating = Window.partitionBy(col("product_id")).orderBy(col("product_id"))
    //val avg_rating_1= avg_rating.withColumn("lag", )
    avg_rating.groupBy(col(""))

//    24. Group Orders by Weekday and Find Peak Hour for Orders
//    Question: Group by weekday (use dayofweek) and hour, and find the hour with the most orders for each weekday.

    val order_weekday = List(("order_1","2023-04-10",10), ("order_2","2023-04-11",15), ("order_3","2023-04-12",12), ("order_4","2023-04-13",11),
      ("order_5","2023-04-14",18))toDF("order_id","order_date","hour")



//    25. Calculate Average Order Value by Country, Excluding Cancelled Orders
//    Question: Group by country, calculate the average order value, excluding orders with a "Cancelled" status.
    val order_cancel = List(("USA","order_1",100,"Shipped"), ("USA","order_2",40,"Cancelled"), ("UK","order_3",150,"Completed"),
      ("France","order_4",250,"Pending"), ("France","order_5",30,"Shipped"))toDF("country","order_id","amount","status")
    val order_cancel_1 = order_cancel.where(col("status") =!= "Cancelled")
    //order_cancel_1.groupBy(col("country")).agg(avg("amount")).show()


  }
}