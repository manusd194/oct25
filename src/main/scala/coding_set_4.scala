import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, avg, col, count, current_date, date_add, date_format, date_sub, date_trunc, datediff, dayofmonth, dayofweek, dense_rank, hour, initcap, lag, last_day, lead, length, max, min, month, months_between, next_day, rank, round, row_number, stddev, sum, to_date, to_timestamp, when, year}
import org.apache.spark.sql.types.DateType
import scala.util.control.Exception.noCatch.desc

object coding_set_4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name", "coding-set-4")
    conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf = conf)
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("Karthik", "Sales", 2023, 1200), ("Ajay", "Marketing", 2022, 2000), ("Vijay", "Sales", 2023, 1500), ("Mohan", "Marketing", 2022, 1500),
      ("Veer", "Sales", 2021, 2500), ("Ajay", "Finance", 2023, 1800), ("Kiran", "Sales", 2023, 1200), ("Priya", "Finance", 2023, 2200),
      ("Karthik", "Sales", 2022, 1300), ("Ajay", "Marketing", 2023, 2100), ("Vijay", "Finance", 2022, 2100), ("Kiran", "Marketing", 2023, 2400),
      ("Mohan", "Sales", 2022, 1000)) toDF("Name", "Department", "Year", "Salary")


    //  1. For each department, assign ranks to employees based on their salary in descending order for each year.
    val window1 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary").desc)
    //data.withColumn("salary_rank", rank().over(window1)).show()

    //  2. In the "Sales" department, rank employees based on their salary. If two employees have the
    //  same salary, they should share the same rank.
    val window2 = Window.partitionBy(col("Department")).orderBy(col("Salary"))
    //data.withColumn("rank", rank().over(window2)).filter(col("Department")=== "Sales").show()

    //  3. For each department, assign a row number to each employee based on the year, ordered by salary in descending order.
    val window3 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary").desc)
    //data.withColumn("rank", row_number().over(window3)).orderBy(col("Department")).show()

    //  4. Rank employees across all departments based on their salary within each year.
    val window4 = Window.partitionBy(col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", rank().over(window4)).show()

    //  5. Rank employees by their salary in descending order. If multiple employees have the same
    //    salary, ensure they receive unique rankings without gaps.
    val window5 = Window.orderBy(col("Salary").desc)
    //data.withColumn("rank", dense_rank().over(window5)).show()

    //  6. For each year, rank employees in the "Marketing" department based on salary, but without
    //    any gaps in ranks if salaries are tied.
    val q6 = data.filter(col("Department") === "Marketing")
    val window6 = Window.partitionBy(col("Year")).orderBy(col("Salary"))
    //q6.withColumn("rank", dense_rank().over(window6)).show()

    //  7. For each year, assign row numbers to employees based on salary in ascending order within each department.
    val window7 = Window.partitionBy(col("Department")).orderBy(col("Salary"))
    //data.withColumn("rank", row_number().over(window7)).show()

    //  8. Within each department, assign a dense rank to employees based on their salary for each year.
    val window8 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", dense_rank().over(window8)).show()

    //  9. Identify the top 3 highest-paid employees in each department for the year 2023 using any ranking function.
    val q9 = data.filter(col("Year") === 2023)
    val window9 = Window.partitionBy(col("Department")).orderBy(col("Salary").desc)
    //q9.withColumn("highest_paid", rank().over(window9)).where(col("highest_paid") <= 3).show()

    //  10. For the "Finance" department, list employees in descending order of salary, showing their relative ranks without any gaps.
    val window10 = Window.partitionBy(col("Department")).orderBy(col("Salary").desc)
    //data.withColumn("rank", dense_rank().over(window10)).filter(col("Department")=== "Finance").show()

    //  11. Rank employees across departments based on their salary, grouping by year, and sort by department as the secondary sorting criteria.
    val window11 = Window.partitionBy(col("Year"),col("Department")).orderBy(col("Salary"))
    //data.withColumn("rank", rank().over(window11)).orderBy(col("Year")).show()

    //  12. In each department, assign a rank to employees based on their salaries within each year. If
    //  two employees have the same salary, they should get the same rank without any gaps.
    val window12 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", dense_rank().over(window12)).show()

    //  13. For each department, assign row numbers to employees based on year, ordering by salary in descending order.
    val window13 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary").desc)
    //data.withColumn("rank", row_number().over(window13)).show()

    //  14. Find the lowest-ranked employees in each department for the year 2022 based on salary.
    val window14 = Window.partitionBy(col("Department"), col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", rank().over(window14)).filter(col("rank")===1 && col("Year")===2022).show()

    //  15. In each department, rank employees based on salary and year. If employees have the same salary, they should share the same rank.
    val window15 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", rank().over(window15)).show()

    //  16. Assign row numbers to employees across all departments, ordered by salary, with ties in
    //    salary broken by alphabetical order of employee names.
    val window16 = Window.partitionBy(col("Department")).orderBy(col("Salary"),col("Name"))
    //data.withColumn("rank", row_number().over(window16)).show()

    //  17. For each department, assign ranks to employees by year. Use a ranking function that assigns the same rank for ties and has no gaps.
    val window17 = Window.partitionBy(col("Department")).orderBy(col("Year"))
    //data.withColumn("rank", dense_rank().over(window17)).show()

    //  18. List employees ranked in descending order of their salaries within each department. If
    //  employees have the same salary, they should receive consecutive ranks without gaps.
    val window18 = Window.partitionBy(col("Department")).orderBy(col("Salary").desc)
    //data.withColumn("rank", row_number().over(window18)).show()

    //  19. Assign a dense rank to employees in the "Sales" department based on salary across all years.
    val window19 = Window.partitionBy(col("Department")).orderBy(col("Salary"))
    //data.withColumn("rank", dense_rank().over(window19)).filter(col("Department")==="Sales").show()

    //  20. For each department and year, assign a row number to employees ordered by salary,
    //  showing only the top 2 in each department and year.
    val window20 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", row_number().over(window20)).where(col("rank") <=2).show()

    //  21. In each department, rank employees based on their salary. Show ranks as consecutive numbers even if salaries are tied.
    val window21 = Window.partitionBy(col("Department")).orderBy(col("Salary"))
    //data.withColumn("rank", row_number().over(window21)).show()

    //  22. List employees with the top 2 highest salaries in the "Finance" department for each year.
    val window22 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary").desc)
    //data.withColumn("rank", dense_rank().over(window22)).filter(col("rank")<=2 && col("Department") === "Finance").show()

    //  23. For each department, rank employees based on their salary within each year. Display only
    //    employees ranked in the top 3 for each department and year.
    val window23 = Window.partitionBy(col("Department"),col("Year")).orderBy(col("Salary"))
    //data.withColumn("rank", dense_rank().over(window23)).where(col("rank")<=3).show()

    //  24. Within the "Marketing" department, assign a row number to employees based on salary in descending order across all years.
    val window24 = Window.partitionBy(col("Department")).orderBy(col("Salary").desc)
    //data.withColumn("rank", row_number().over(window24)).show()

    //  25. Rank all employees based on their salaries in descending order across departments. Handle ties in a way that avoids gaps between ranks
    val window25 = Window.orderBy(col("Salary").desc)
    //data.withColumn("rank", dense_rank().over(window25)).show()


  }
}