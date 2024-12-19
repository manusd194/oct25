import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, current_date, datediff, initcap, lag, lead, length, max, min, rank, sum, when}

import scala.util.control.Exception.noCatch.desc

object lead_and_lag {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name","lead_and_lag")
    conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(conf=conf)
      .getOrCreate()

    import spark.implicits._

    //1. we want to find the difference between the price on each day with its previous day.

    val product_price = List(
      (1, "KitKat", 1000.0, "2021-01-01"),
      (1, "KitKat", 2000.0, "2021-01-02"),
      (1, "KitKat", 1000.0, "2021-01-03"),
      (1, "KitKat", 2000.0, "2021-01-04"),
      (1, "KitKat", 3000.0, "2021-01-05"),
      (1, "KitKat", 1000.0, "2021-01-06")
    ).toDF("IT_ID","IT_Name", "Price", "PriceDate")

    val window = Window.orderBy(col("PriceDate"))

    val product_price_scala = product_price.withColumn("diff_column", col("Price")-lag(col("Price"),1).over(window))
      .withColumn("lag_column",lag(col("Price"),1).over(window))
    //product_price_scala.show()
    //sql-spark


    //2. If salary is less than previous month we will mark it as "DOWN", if salary has increased then "UP"
    val salary = List(
      (1,"John",1000,"01/01/2016"),
      (1,"John",2000,"02/01/2016"),
      (1,"John",1000,"03/01/2016"),
      (1,"John",2000,"04/01/2016"),
      (1,"John",3000,"05/01/2016"),
      (1,"John",1000,"06/01/2016")
    )toDF("ID","NAME","SALARY","DATE")

    val salary_window = Window.orderBy(col("DATE"))
    val salary_scala = salary.withColumn("diff_column", when(col("SALARY") > lag(col("SALARY"),1).over(salary_window), "UP")
      .when(col("SALARY") < lag(col("SALARY"),1).over(salary_window), "DOWN").otherwise("NULL"))
      .withColumn("lag_column", lag(col("SALARY"),1).over(salary_window))
    //salary_scala.show()

    //3. Calculate the lead and lag of the salary column ordered by id
    val salary_other = List(
      (1,"karthik",1000),
      (2,"mohan",2000),
      (3,"vinay",1500),
      (4,"Deva",3000)
    )toDF("id1","name","salary")

    val salary_other_window = Window.orderBy(col("id1"))
    val salary_other_lead = salary_other.withColumn("salary_lead", lead(col("salary"),1).over(salary_other_window))
    //salary_other_lead.show()

    val salary_other_lag = salary_other.withColumn("salary_lag", lag(col("salary"),1).over(salary_other_window))
    //salary_other_lag.show()

    //4. Calculate the percentage change in salary from the previous row to the current row, ordered by id. (using the same sample data)
    val salary_other_per = salary_other.withColumn("diff_per", (col("salary")-lag(col("salary"),1).over(salary_other_window))/col("salary")*100)
    //salary_other_per.show()

    //5. Calculate the rolling sum of salary for the current row and the previous two rows, ordered by id.
    val window_rolling_sum = Window.orderBy("id1").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    //salary_other.withColumn("Running_Total", sum("salary").over(window_rolling_sum)).show()

    //6. Calculate the difference between the current salary and the minimum salary within the last three rows, ordered by id.

    //7. Calculate the lead and lag of salary within each group of employees (grouped by name) ordered by id.
    val window_partition_name = Window.partitionBy(col("name")).orderBy(col("id1"))
    //Lead
    //salary_other.withColumn("lead_by_name",lead(col("salary"),1).over(window_partition_name)).show()
    //Lag
    //salary_other.withColumn("lead_by_name",lag(col("salary"),1).over(window_partition_name)).show()

    //8. Calculate the lead and lag of the salary column for each employee ordered by id, but only for the employees who have a salary greater than 1500.
    //salary_other.withColumn("lead_1500", lead(col("salary"),1).over(window_partition_name)).filter(col("salary")>1500).show()
    //salary_other.withColumn("lag_1500",lag(col("salary"),1).over(window_partition_name)).filter(col("salary")>1500).show()

    //9. Calculate the lead and lag of the salary column for each employee, ordered by id, but only for the employees who have a change in salary
    // greater than 500 from the previous row.

    val salary_other_1 = List(
      (1,"karthik",1000),
      (1,"karthik",1500),
      (1,"karthik",2000),
      (2,"mohan",2000),
      (2,"mohan",2500),
      (2,"mohan",3000),
      (3,"vinay",1500),
      (4,"Deva",3000)
    )toDF("id1","name","salary")
    val window_other_1 = Window.partitionBy(col("name")).orderBy(col("id1"))
    //salary_other_1.withColumn("lead", lead(col("salary"),1).over(window_other_1)-col("salary")).filter(col("lead")>=500).show()

    //10. Calculate the cumulative count of employees, ordered by id, and reset the count when the name changes.



    //11. Calculate the running total of salary for each employee ordered by id.
    val window_running_total = Window.partitionBy("name").orderBy("id1").rowsBetween(Window.unboundedPreceding,Window.currentRow)
    //salary_other_1.withColumn("running_total", sum(col("salary")).over(window_running_total)).show()


    //12. Find the maximum salary for each employee’s group (partitioned by name) and display it for each row.
    val salary_other_2 = salary_other_1.withColumn("max_salary", max(col("salary")).over(window_other_1))
    //salary_other_2.orderBy(col("id1")).show()

    //13. Calculate the difference between the current salary and the average salary for each employee’s group(partitioned by name) ordered by id.
    val salary_other_3 = salary_other_1.withColumn("diff_avg_curr", col("salary") - avg(col("salary")).over(window_other_1))
    //salary_other_3.orderBy(col("id1")).show()

    //14. Calculate the rank of each employee based on their salary, ordered by salary in descending order.
    val window_salary_rank = Window.orderBy(col("salary").desc)
    val salary_other_rank_1 = salary_other.withColumn("salary_rank", rank().over(window_salary_rank))
    //salary_other_rank_1.orderBy(col("id1")).show()

    //15. Calculate the lead and lag of the salary column, ordered by id, but only for the employees whose salaries are strictly increasing
    // (i.e., each employee’s salary is greater than the previous employee’s salary).



    //16. Calculate the lead and lag of the salary column ordered by id, but reset the lead and lag values when the employee’s name changes.

    //17. Calculate the percentage change in salary from the previous row to the current row, ordered by id, but group the percentage changes by name.









  }

}
