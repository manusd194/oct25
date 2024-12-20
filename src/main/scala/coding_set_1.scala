import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{avg, col, count, current_date, datediff, initcap, sum,when,max,min,length}

import scala.util.control.Exception.noCatch.desc


object coding_set_1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name","coding_set_1")
    conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf = conf)
      .getOrCreate()

    import spark.implicits._

    //  Question 1: Employee Status Check
    //  Create a DataFrame that lists employees with names and their work status. For each employee,
    //  determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
    //  within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
    //    letter of each name is capitalized.
    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")).toDF("name", "last_checkin")

    //scala-spark
    //val df = employees.withColumn("last_checkin",current_date() - col("last_checkin")).show()
    val df1 = employees.withColumn("diff", datediff(current_date(), employees("last_checkin")))
    val df2 = df1.withColumn("status", when((col("diff") > 7), "Inactive").otherwise("Active"))
      .withColumn("name",initcap(col("name")))
    //df2.show()

    //sql-spark
    employees.createOrReplaceTempView("employees_sql")

//    Question 2: Sales Performance by Agent
//      Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
//      based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
//      50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
//    sales aggregated by performance status.
    val sales = List(
      ("karthik", 60000),
      ("neha", 48000),
      ("priya", 30000),
      ("mohan", 24000),
      ("ajay", 52000),
      ("vijay", 45000),
      ("veer", 70000),
      ("aatish", 23000),
      ("animesh", 15000),
      ("nishad", 8000),
      ("varun", 29000),
      ("aadil", 32000)
    ).toDF("name", "total_sales")

//    sales.withColumn("performance status",when(col("total_sales")>50000,"excellent").when(col("total_sales")<= 50000 && col("total_sales") >=25000,"Good").otherwise("Needs Improvement"))
//      .withColumn("name",initcap(col("name"))).show()

    //spark-sql
//    sales.createOrReplaceTempView("sales_sql")
//    spark.sql(
//      """
//         select concat(upper(substring(name,1,1)), substring(name,2,length(name))) as name,initcap(name),total_sales, case
//         when total_sales > 50000 then "excellent"
//         when total_sales between 50000 and 25000 then "Good"
//         else "Needs Improvement"
//         end as performance_status
//         from sales_sql
//        """).show()

//    Question 3: Project Allocation and Workload Analysis
//    Given a DataFrame with project allocation data for multiple employees, determine each employee's workload level based on their hours worked
//    in a month across various projects. Categorize employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
//    and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated workload status count by category.
    val workload = List(
      ("karthik", "ProjectA", 120),
      ("karthik", "ProjectB", 100),
    ("neha", "ProjectC", 80),
    ("neha", "ProjectD", 30),
    ("priya", "ProjectE", 110),
    ("mohan", "ProjectF", 40),
    ("ajay", "ProjectG", 70),
    ("vijay", "ProjectH", 150),
    ("veer", "ProjectI", 190),
    ("aatish", "ProjectJ", 60),
    ("animesh", "ProjectK", 95),
    ("nishad", "ProjectL", 210),
    ("varun", "ProjectM", 50),
    ("aadil", "ProjectN", 90)
    ).toDF("name", "project", "hours")

    val workload_1 = workload.groupBy(col("name")).agg(sum(col("hours")).as("total_hours"))
    //workload_1.orderBy(col("total_hours").desc).show()
//    workload_1.withColumn("Utilization",when(col("total_hours")>200,"Overloaded").when(col("total_hours") <= 200 && col("total_hours") >=100, "Balanced")
//      .otherwise("Underutilized"))
//      .withColumn("name", initcap(col("name"))).show()'\\\\\\

//    workload.createOrReplaceTempView("workload_sql_1")
//    spark.sql(
//      """
//         select initcap(name), project, hours, case
//         when hours > 200 then "Overloaded"
//         when hours between 100 and 200 then "Balanced"
//         else "Underutilized"
//         end as Utilization
//         from workload_sql_1
//
//        """).show()



//    5. Overtime Calculation for Employees
//    Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
//    "Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
//      name and group by overtime status.
    val employees_overtime = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")

    val employees_overtime_1 =employees_overtime.withColumn("overtime",when(col("hours_worked") > 60, "Excessive Overtime").when(col("hours_worked") >=60 && col("hours_worked") <= 45, "Standard Overtime").otherwise("No Overtime"))
    //employees_overtime_1.show()
    val a = employees_overtime_1.orderBy(col("overtime"))
    //a.show()

    //spark-sql
//    employees_overtime.createOrReplaceTempView("employees_overtime_1")
//    spark.sql(
//      """
//        select initcap(name), hours_worked, case
//        when hours_worked > 60 then "Excessive Overtime"
//        when hours_worked between 45 and 60 then "Standard Overtime"
//        else "No Overtime"
//        end overtime
//        from employees_overtime_1 order by overtime
//        """).show()

//    6. Customer Age Grouping
//      Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
//      names and show total customers in each group.
//
    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")
//
    val customers_1 = customers.withColumn("age_group", when(col("age") < 25,"Youth").when(col("age") >= 25 && col("age") <= 45, "Adult").otherwise("Senior"))
      .withColumn("name",initcap(col("name")))
    //customers_1.groupBy(col("age_group")).agg(count(col("name"))).show()
    //print(customers_1.count())

    customers.createOrReplaceTempView("customers_view")
    val customers_2 = spark.sql(
      """
        select initcap(name) as name, age, case
        when age > 45 then "Senior"
        when age between 25 and 45 then "Adult"
        else "Youth"
        end as age_group
        from customers_view
        """)
    //customers_2.groupBy(col("age_group")).agg(count(col("name"))).show()


//    7. Vehicle Mileage Analysis
//      Classify each vehicle’s mileage as "High Efficiency" if mileage is above 25 MPG, "Moderate Efficiency"
//    if between 15-25 MPG, and "Low Efficiency" if below 15 MPG.
    val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "mileage")

    //vehicles.withColumn("Efficiency", when(col("mileage") > 25, "High Efficiency").when(col("mileage") >= 15 && col("mileage") <= 25, "Moderate Efficiency").otherwise("Less Efficiency")).show()

    //spark-sql
    vehicles.createOrReplaceTempView("vehicles_view")
    spark.sql(
      """select vehicle_name,mileage, case
        when mileage > 25 then "High Efficiency"
        when mileage between 15 and 25 then "Moderate Efficiency"
        else "Low Efficiency"
        end as Efficiency
        from vehicles_view order by Efficiency
        """).show()

//    8. Student Grade Classification
//      Classify students based on their scores as "Excellent" if score is 90 or above, "Good" if between 75-
//      89, and "Needs Improvement" if below 75. Count students in each category.
//     Scala Spark Data
    val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")

    val students_2 = students.withColumn("Classification", when(col("score") >=90, "Excellent").when(col("score") <= 89 && col("score") >= 75, "Good").otherwise("Needs Improvement"))
    //students_2.groupBy(col("Classification")).agg(count(col("Classification"))).show()

    //students_2.groupBy(col("Classification"),col("name")).agg(count(col("Classification"))).orderBy("name").show()
//
//    9. Product Inventory Check
//      Classify inventory stock levels as "Overstocked" if stock exceeds 100, "Normal" if between 50-100,
//    and "Low Stock" if below 50. Aggregate total stock in each category.
//     Scala Spark Data
    val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")

//    val inventory_1 = inventory.withColumn("Inventory_status",when(col("stock_quantity") > 100,"Overstocked").when(col("stock_quantity") <= 100 && col("stock_quantity") >= 50, "Normal").otherwise("Low Stock"))
//    inventory_1.groupBy(col("Inventory_status")).agg(count(col("Inventory_status"))).show()


//
//    10. Employee Bonus Calculation Based on Performance and Department
//    Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
//    performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
//      employees receive no bonus. Group by department and calculate total bonus allocation.
//     Scala Spark Data
    val employees_performance = List(
      ("karthik", "Sales", 85),
      ("neha", "Marketing", 78),
      ("priya", "IT", 90),
      ("mohan", "Finance", 65),
      ("ajay", "Sales", 55),
      ("vijay", "Marketing", 82),
      ("veer", "HR", 72),
      ("aatish", "Sales", 88),
      ("animesh", "Finance", 95),
      ("nishad", "IT", 60)
      ).toDF("name", "department", "performance_score")

    val employees_performance_1 = employees_performance.withColumn("bonus(in %)", when(col("performance_score")>80, 20).when(col("performance_score")>=70 && col("performance_score") <= 80,15).otherwise(0))
    //employees_performance_1.groupBy(col("department")).agg(sum(col("bonus(in %)"))).show()

//    11. Product Return Analysis with Multi-Level Classification
//      For each product, classify return reasons as "High Return Rate" if return count exceeds 100 and
//      satisfaction score below 50, "Moderate Return Rate" if return count is between 50-100 with a score
//      between 50-70, and "Low Return Rate" otherwise. Group by category to count product return rates.
//     Scala Spark Data
    val products = List(
      ("Laptop", "Electronics", 120, 45),
      ("Smartphone", "Electronics", 80, 60),
      ("Tablet", "Electronics", 50, 72),
      ("Headphones", "Accessories", 110, 47),
      ("Shoes", "Clothing", 90, 55),
      ("Jacket", "Clothing", 30, 80),
      ("TV", "Electronics", 150, 40),
      ("Watch", "Accessories", 60, 65),
      ("Pants", "Clothing", 25, 75),
      ("Camera", "Electronics", 95, 58)
      ).toDF("product_name", "category", "return_count", "satisfaction_score")

      val products_1 = products.withColumn("classification", when(col("return_count") > 100 && col("satisfaction_score") <50, "High Return Rate")
        .when(col("return_count") >= 50 && col("return_count") <= 100 && col("satisfaction_score") <= 70 && col("satisfaction_score") >=50, "Moderate Return Rate")
        .otherwise("Low Return Rate"))

      //products_1.show()
      //products_1.groupBy(col("category")).agg(count("classification")).show()

    // spark-sql
    products.createOrReplaceTempView("products_view")
    val products_sql_1 = spark.sql(
      """
        select product_name, category, return_count, satisfaction_score, case
        when return_count > 100 and satisfaction_score < 50 then "High Return Rate"
        when return_count between 100 and 50 and satisfaction_score between 70 and 50 then "Moderate Return Rate"
        else "Low Return Rate"
        end as classification
        from products_view
     """)
    products_sql_1.groupBy(col("category")).agg(count(col("satisfaction_score"))).show()
//
//    12. Customer Spending Pattern Based on Age and Membership Level
//      Classify customers' spending as "High Spender" if spending exceeds $1000 with "Premium"
//    membership, "Average Spender" if spending between $500-$1000 and membership is "Standard",
//    and "Low Spender" otherwise. Group by membership and calculate average spending.
    val customers_membership = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")

    val customers_membership_1=customers_membership.withColumn("spend_category", when(col("membership") === "Premium" && col("spending") > 1000 , "High Spender")
      .when(col("membership") === "Standard" && col("spending") >= 500 && col("spending") <= 1000, "Average Spender").otherwise("Low Spender"))

    //customers_membership_1.groupBy(col("membership")).agg(avg(col("spending"))).show()

//    13. E-commerce Order Fulfillment Timeliness Based on Product Type and Location
//      Classify orders as "Delayed" if delivery time exceeds 7 days and origin location is "International",
//    "On-Time" if between 3-7 days, and "Fast" if below 3 days. Group by product type to see the count of
//      each delivery speed category.
//     Scala Spark Data
    val orders = List(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Smartphone", "Domestic", 3),
      ("Order4", "Tablet", "International", 5),
      ("Order5", "Watch", "Domestic", 7),
      ("Order6", "Headphones", "International", 10),
      ("Order7", "Camera", "Domestic", 1),
      ("Order8", "Shoes", "International", 9),
      ("Order9", "Laptop", "Domestic", 6),
      ("Order10", "Tablet", "International", 4)
    ).toDF("order_id", "product_type", "origin", "delivery_days")

    val orders_1 = orders.withColumn("delivery_speed", when(col("origin") === "International" && col("delivery_days") > 7, "Delayed")
    .when(col("delivery_days") <=7 && col("delivery_days") >= 3, "On-Time").otherwise("Fast"))

    //orders_1.groupBy(col("product_type"), col("delivery_speed")).agg(count(col("delivery_speed"))).orderBy("product_type").show()

//    Scenario 14: Financial Risk Level Classification for Loan Applicants
//      Question Set:
//      1. Classify loan applicants as "High Risk" if the loan amount exceeds twice their income and
//      credit score is below 600, "Moderate Risk" if the loan amount is between 1-2 times their
//    income and credit score between 600-700, and "Low Risk" otherwise. Find the total count of
//    each risk level.
//    2. For applicants classified as "High Risk," calculate the average loan amount by income range
//    (e.g., < 50k, 50-100k, >100k).
//    3. Group by income brackets (<50k, 50-100k, >100k) and calculate the average credit score for
//      each risk level. Filter for groups where average credit score is below 650.
    val loanApplicants = List(
      ("karthik", 60000, 120000, 590),
      ("neha", 90000, 180000, 610),
      ("priya", 50000, 75000, 680),
      ("mohan", 120000, 240000, 560),
      ("ajay", 45000, 60000, 620),
      ("vijay", 100000, 100000, 700),
      ("veer", 30000, 90000, 580),
      ("aatish", 85000, 85000, 710),
      ("animesh", 50000, 100000, 650),
      ("nishad", 75000, 200000, 540)
    ).toDF("name", "income", "loan_amount", "credit_score")


//    Scenario 16: Electricity Consumption and Rate Assignment
//    Question Set: 7. Classify households into "High Usage" if kWh exceeds 500 and bill exceeds $200,"Medium Usage" for kWh between 200-500
    //and bill between $100-$200, and "Low Usage" otherwise. Calculate the total number of households in each usage category.
//    8. Find the maximum bill amount for "High Usage" households and calculate the average kWh for "Medium Usage" households.
//    9. Identify households with "Low Usage" but kWh usage exceeding 300. Count such households.

    val electricityUsage = List(
      ("House1", 550, 250),
    ("House2", 400, 180),
    ("House3", 150, 50),
    ("House4", 500, 200),
    ("House5", 600, 220),
    ("House6", 350, 120),
    ("House7", 100, 30),
    ("House8", 480, 190),
    ("House9", 220, 105),
    ("House10", 150, 60)
    ).toDF("household", "kwh_usage", "total_bill")

    val electricityUsage_1 = electricityUsage.withColumn("usage_category", when(col("kwh_usage")>500 && col("total_bill")>200,"High Usage").when(col("kwh_usage")>=200 &&
    col("kwh_usage")<=500 && col("total_bill")>=100 && col("total_bill")<=200,"Medium Usage").otherwise("Low Usage"))
    //electricityUsage_1.groupBy(col("usage_category")).agg(count(col("usage_category"))).show()
    //electricityUsage_1.groupBy(col("usage_category")).agg(max(col("total_bill"))).filter(col("usage_category")==="High Usage").show()
    //electricityUsage_1.groupBy(col("usage_category")).agg(avg(col("kwh_usage"))).filter(col("usage_category")==="Medium Usage").show()
    //electricityUsage_1.groupBy(col("usage_category")).agg(count(col("household"))).filter(col("usage_category") === "Low Usage").show()

      //col("kwh_usage") > 300).show()
    //electricityUsage_1.filter(col("usage_category") === "Low Usage" && col("kwh_usage") > 100).agg(count(col("household"))).show()

//    Scenario 17: Employee Salary Band and Performance Classification
//      Question Set: 10. Classify employees into salary bands: "Senior" if salary > 100k and experience > 10
//    years, "Mid-level" if salary between 50-100k and experience 5-10 years, and "Junior" otherwise.
//    Group by department to find count of each salary band.
//    11. For each salary band, calculate the average performance score. Filter for bands where
//      average performance exceeds 80.
//    12. Find employees in "Mid-level" band with performance above 85 and experience over 7 years.

    val employees_17 = List(
      ("karthik", "IT", 110000, 12, 88),
      ("neha", "Finance", 75000, 8, 70),
      ("priya", "IT", 50000, 5, 65),
      ("mohan", "HR", 120000, 15, 92),
      ("ajay", "IT", 45000, 3, 50),
      ("vijay", "Finance", 80000, 7, 78),
      ("veer", "Marketing", 95000, 6, 85),
      ("aatish", "HR", 100000, 9, 82),
      ("animesh", "Finance", 105000, 11, 88),
      ("nishad", "IT", 30000, 2, 55)
    ).toDF("name", "department", "salary", "experience", "performance_score")

    val employees_17_1 = employees_17.withColumn("band", when(col("salary")>100000 && col("experience")>10,"Senior")
      .when(col("salary")>=50000 && col("salary")<=100000 && col("experience")>=5 && col("experience")<=10,"Mid-level").otherwise("Junior"))
    //employees_17_1.groupBy(col("band")).agg(count(col("band"))).show()
    //employees_17_1.groupBy(col("band")).agg(avg(col("performance_score"))).filter(avg(col("performance_score")) > 50).show()
    //employees_17_1.filter(col("band") === "Mid-level" && col("performance_score") > 85 && col("experience")>7).show()

//    Scenario 18: Product Sales Analysis
//    1. Classify products as "Top Seller" if total sales exceed 200,000 and discount offered is less than 10%, "Moderate Seller" if total sales
//    are between 100,000 and 200,000, and "Low Seller" otherwise. Count the total number of products in each classification.
//    2. Find the maximum sales value among "Top Seller" products and the minimum discount rate among "Moderate Seller" products.
//    3. Identify products from the "Low Seller" category with a total sales value below 50,000 and discount offered above 15%.
    val productSales = List(
      ("Product1", 250000, 5),
      ("Product2", 150000, 8),
      ("Product3", 50000, 20),
      ("Product4", 120000, 10),
      ("Product5", 300000, 7),
      ("Product6", 60000, 18),
      ("Product7", 180000, 9),
      ("Product8", 45000, 25),
      ("Product9", 70000, 15),
      ("Product10", 10000, 30)
    ).toDF("product_name", "total_sales", "discount")

    val productSales_1 = productSales.withColumn("seller_status", when(col("total_sales")>200000 && col("discount")<10,"Top Seller")
      .when(col("total_sales")>=100000 && col("total_sales")<=200000,"Moderate Seller").otherwise("Low Seller"))
    //productSales_1.groupBy(col("seller_status")).agg(count(col("product_name"))).show()
    //productSales_1.filter(col("seller_status")==="Top Seller").agg(max("total_sales")).show()
    //productSales_1.groupBy(col("seller_status")).agg(max("total_sales")).filter(col("seller_status")==="Top Seller").show()
    //productSales_1.groupBy(col("seller_status")).agg(min("discount")).filter(col("seller_status")==="Moderate Seller").show()
   //productSales_1.filter(col("seller_status")==="Low Seller" && col("total_sales") < 50000 && col("discount") >15).show()
//
//    Scenario 19: Customer Loyalty Analysis
//    Question Set: 4. Classify customers as "Highly Loyal" if purchase frequency is greater than 20 times and average spending is above 500,
//    "Moderately Loyal" if frequency is between 10-20 times, and "Low Loyalty" otherwise. Count customers in each classification.
//    5. Calculate the average spending of "Highly Loyal" customers and the minimum spending for "Moderately Loyal" customers.
//    6. Identify "Low Loyalty" customers with an average spending less than 100 and purchase frequency under 5.
    val customerLoyalty = List(
      ("Customer1", 25, 700),
      ("Customer2", 15, 400),
      ("Customer3", 5, 50),
      ("Customer4", 18, 450),
      ("Customer5", 22, 600),
      ("Customer6", 2, 80),
      ("Customer7", 12, 300),
      ("Customer8", 6, 150),
      ("Customer9", 10, 200),
      ("Customer10", 1, 90)
    ).toDF("customer_name", "purchase_frequency", "average_spending")

    val customerLoyalty_1 = customerLoyalty.withColumn("Loyalty",when(col("purchase_frequency")>20 && col("average_spending")>500,"Highly Loyal")
    .when(col("purchase_frequency")>=10 && col("purchase_frequency")<=20,"Moderately Loyal").otherwise("Low Loyalty"))
    //customerLoyalty_1.groupBy(col("Loyalty")).agg(count(col("customer_name"))).show()
    //customerLoyalty_1.groupBy(col("Loyalty")).agg(avg(col("average_spending"))).filter(col("Loyalty")==="Highly Loyal").show()
    //customerLoyalty_1.groupBy(col("Loyalty")).agg(min(col("average_spending"))).filter(col("Loyalty")==="Moderately Loyal").show()
    //customerLoyalty_1.filter(col("Loyalty")==="Low Loyalty" && col("purchase_frequency")<5 && col("average_spending")<100).show()

//    Scenario 20: E-commerce Return Rate Analysis
//      Question Set: 7. Classify products by return rate: "High Return" if return rate is over 20%, "Medium Return" if return rate is between 10%
//      and 20%, and "Low Return" otherwise. Count products in each classification.
//    8. Calculate the average sale price for "High Return" products and the maximum return rate for "Medium Return" products.
//    9. Identify "Low Return" products with a sale price under 50 and return rate less than 5%.
    val ecommerceReturn = List(
      ("Product1", 75, 25),
      ("Product2", 40, 15),
      ("Product3", 30, 5),
      ("Product4", 60, 18),
      ("Product5", 100, 30),
      ("Product6", 45, 10),
      ("Product7", 80, 22),
      ("Product8", 35, 8),
      ("Product9", 25, 3),
      ("Product10", 90, 12)
      ).toDF("product_name", "sale_price", "return_rate")

      val ecommerceReturn_1 = ecommerceReturn.withColumn("return_class", when(col("return_rate")>20,"High Return")
        .when(col("return_rate")>=10 && col("return_rate")<=20,"Medium Return").otherwise("Low Return"))
      //ecommerceReturn_1.groupBy(col("return_class")).agg(count(col("product_name"))).show()
      //ecommerceReturn_1.groupBy(col("return_class")).agg(avg(col("sale_price"))).filter(col("return_class")==="High Return").show()
      //ecommerceReturn_1.groupBy(col("return_class")).agg(max(col("return_rate"))).filter(col("return_class")==="Medium Return").show()
      //ecommerceReturn_1.filter(col("return_class")==="Low Return" && col("sale_price")<50 && col("return_rate")<5).show()

//    Scenario 21: Employee Productivity Scoring
//    Question Set: 10. Classify employees as "High Performer" if productivity score > 80 and project count is greater than 5, "Average Performer"
//    if productivity score is between 60 and 80, and "Low Performer" otherwise. Count employees in each classification.
//    11. Calculate the average productivity score for "High Performer" employees and the minimum score for "Average Performers."
//    12. Identify "Low Performers" with a productivity score below 50 and project count under 2.
    val employeeProductivity = List(
      ("Emp1", 85, 6),
      ("Emp2", 75, 4),
      ("Emp3", 40, 1),
      ("Emp4", 78, 5),
      ("Emp5", 90, 7),
      ("Emp6", 55, 3),
      ("Emp7", 80, 5),
      ("Emp8", 42, 2),
      ("Emp9", 30, 1),
      ("Emp10", 68, 4)
      ).toDF("employee_id", "productivity_score", "project_count")

    val employeeProductivity_1 = employeeProductivity.withColumn("productivity_class", when(col("productivity_score")>80 && col("project_count")>5,"High Performer")
    .when(col("productivity_score")>=60 && col("productivity_score")<=80,"Average Performer").otherwise("Low Performer"))
    //employeeProductivity_1.groupBy(col("productivity_class")).agg(count(col("employee_id"))).show()
    //employeeProductivity_1.groupBy("productivity_class").agg(avg(col("productivity_score"))).filter(col("productivity_class")==="High Performer").show()
    //employeeProductivity_1.groupBy("productivity_class").agg(min(col("productivity_score"))).filter(col("productivity_class")==="Average Performer").show()
    //employeeProductivity_1.filter(col("productivity_class")==="Low Performer" && col("project_count")<2 && col("productivity_score")<50).show()

//    Scenario 22: Banking Fraud Detection
//    Question Set:
//      1. Classify transactions as "High Risk" if the transaction amount is above 10,000 and frequency of transactions from the same account within
//      a day exceeds 5, "Moderate Risk" if the amount is between 5,000 and 10,000 and frequency is between 2 and 5, and "Low Risk" otherwise.
//      Calculate the total number of transactions in each risk level.
//    2. Identify accounts with at least one "High Risk" transaction and the total amount transacted by those accounts.
//    3. Find all "Moderate Risk" transactions where the account type is "Savings" and the amount is above 7,500.
    val transactions = List(
      ("Account1", "2024-11-01", 12000, 6, "Savings"),
      ("Account2", "2024-11-01", 8000, 3, "Current"),
      ("Account3", "2024-11-02", 2000, 1, "Savings"),
      ("Account4", "2024-11-02", 15000, 7, "Savings"),
      ("Account5", "2024-11-03", 9000, 4, "Current"),
      ("Account6", "2024-11-03", 3000, 1, "Current"),
      ("Account7", "2024-11-04", 13000, 5, "Savings"),
      ("Account8", "2024-11-04", 6000, 2, "Current"),
      ("Account9", "2024-11-05", 20000, 8, "Savings"),
      ("Account10", "2024-11-05", 7000, 3, "Savings")
    ).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")







  }

}
