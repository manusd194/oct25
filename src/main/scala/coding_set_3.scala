import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{add_months, avg, col, count, current_date, date_add, date_format, date_sub, date_trunc, datediff, dayofmonth, dayofweek, hour, initcap, lag, last_day, lead, length, max, min, month, months_between, next_day, rank, round, stddev, sum, to_date, to_timestamp, when, year}
import org.apache.spark.sql.types.DateType

import scala.util.control.Exception.noCatch.desc

object coding_set_3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name", "date_manipulation")
    conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf = conf)
      .getOrCreate()

    import spark.implicits._


//      1. Extract the day of the month from a date column.
    val q1 = Seq(("2024-01-15"), ("2024-02-20"), ("2024-03-25")).toDF("date")
    //q1.withColumn("date", dayofmonth(col("date"))).show()

    //spark-sql
    //q1.select(dayofmonth().alias('day'))
//    q1.createOrReplaceTempView("qq1")
//    spark.sql(
//      """
//        select date, dayofmonth(date) as day from qq1
//        """).show()

//    2. Get the weekday name (e.g., Monday, Tuesday) from a date column.
    val q2 = Seq(("2024-04-02"), ("2024-04-03"), ("2024-04-04")).toDF("date")
    val q2_1 = q2.withColumn("day", dayofweek(col("date")))
      .withColumn("date_str", date_format(col("date"), "EEEE"))
    //q2_1.printSchema()

    //spark-sql
//    q2.createOrReplaceTempView("qq2")
//    spark.sql(
//      """
//        select date, date_format(date, "EEEE") as weekday from qq2
//        """).show()


//    3. Calculate the number of days between two dates.
      val q3 = Seq(("2024-01-01", "2024-01-10"), ("2024-02-01", "2024-02-20")).toDF("start_date", "end_date")
      //q3.withColumn("days_between", datediff(col("start_date"), col("end_date"))).show()

    //spark-sql
//    q3.createOrReplaceTempView("qq3")
//    spark.sql(
//      """
//        select start_date, end_date, datediff(start_date, end_date) from qq3
//        """).show()

//    4. Add 10 days to a given date column.
      val q4 = Seq(("2024-05-01"), ("2024-05-15")).toDF("date")
      //q4.withColumn("add_date", date_add(col("date"),10)).show()

    //spark-sql
//    q4.createOrReplaceTempView("qq4")
//    spark.sql(
//      """
//        select date, date_add(date, 10) as future from qq4
//        """).show()


//    5. Subtract 7 days from a given date column.
      val q5 =  Seq(("2024-06-10"), ("2024-06-20")).toDF("date")
      //q5.withColumn("sub_date", date_sub(col("date"),7)).show()

    //spark-sql
//    q5.createOrReplaceTempView("qq5")
//    spark.sql(
//      """
//        select date, date_sub(date, 7) from qq5
//        """).show()

//    6.Filter rows where the year in a date column is 2023.
      val q6 = Seq(("2023-08-12"), ("2024-08-15")).toDF("date")
      //q6.withColumn("year_filter", year(col("date"))).filter(col("year_filter") =!= 2023).show()

    //spark-sql
//    q6.createOrReplaceTempView("qq6")
//    spark.sql(
//      """
//        select date from qq6 where year(date) = 2023
//        """).show()

//    6. Get the last day of the month for a given date column.
      val q6_1 = Seq(("2024-07-10"), ("2024-07-25")).toDF("date")
      //q6_1.withColumn("last", last_day(col("date"))).show()

    //spark-sql
//    q6_1.createOrReplaceTempView("qq6_1")
//    spark.sql(
//      """
//        select last_day(date) from qq6_1_1
//        """).show()

//    7. Extract only the year from a date column.
      val q7 = Seq(("2024-01-01"), ("2025-02-01")).toDF("date")
      //q7.withColumn("year", year(col("date"))).show()

    //spark-sql
//    q7.createOrReplaceTempView("qq7")
//    spark.sql(
//      """
//        select year(date) as year from qq7
//        """).show()

//    8. Calculate the number of months between two dates.
      val q8 = Seq(("2024-01-01", "2024-04-01"), ("2024-05-01", "2024-08-01")).toDF("start_date", "end_date")
      //q8.withColumn("months_between", months_between(col("end_date"),col("start_date"))).show()

    //spark-sql
//    q8.createOrReplaceTempView("qq8")
//    spark.sql(
//      """
//        select start_date, end_date, months_between(start_date, end_date) as diff from qq8
//        """).show()

//    9. Get the week number from a date column.
      val q9 = Seq(("2024-08-15"), ("2024-08-21")).toDF("date")
      //q9.withColumn("week_num", date_format(col("date"), "W")).show()

    //spark-sql
//    q9.createOrReplaceTempView("qq9")
//    spark.sql(
//      """
//        select date, date_format(date, "W") as week_num from qq9
//        """).show()

//    10. Format a date column to "dd-MM-yyyy" format.
      val q10 = Seq(("2024-09-01"), ("2024-09-10")).toDF("date")
      //q10.withColumn("date_format", date_format(col("date"),"dd-MM-yyyy")).show()

    //spark-sql
//    q10.createOrReplaceTempView("qq10")
//    spark.sql(
//      """
//        select date, date_format(date, "dd-MM-yyyy") as new_date from qq10
//        """).show()

    //11. Find if a given date falls on a weekend.
    val q11 = Seq(("2024-10-11"), ("2024-10-13")).toDF("date")
    val q11_df = q11.withColumn("weekend", dayofweek(col("date")))
    //q11_df.withColumn("w", when(col("weekend") ===1 || col("weekend") ===7,"weekend").otherwise("not weekend")).show()

    //spark-sql
//    q11.createOrReplaceTempView("qq11")
//    spark.sql(
//      """
//        select date from (select  date, case
//        when dayofweek(date) == 1 or dayofweek(date) == 7 then "weekend"
//        else "not weekend"
//        end as week from qq11) where week = "weekend"
//        """).show()

    //11. Check if the date is in a leap year.
    val q11_1 = Seq(("2024-02-29"), ("2023-02-28")).toDF("date")
    //q11_1.withColumn("d", )

//    q11_1.withColumn("leap", when(year(col("date")) % 400 === 0 && year(col("date")) % 100 === 0, "leap_year")
//      .when(year(col("date")) % 4 === 0 && year(col("date")) % 100 =!= 0, "leap_year").otherwise("not_leap_year")).show()

//    //spark-sql
//    q11_1.createOrReplaceTempView("qq11_1")
//    spark.sql(
//      """
//        select date, case
//        when year(date) % 400 == 0 and year(date) % 100 == 0 then "leap_year"
//        when year(date) % 4 == 0 and year(date) % 100 != 0 then "leap_year"
//        else "not_leap_year"
//        end as leap from qq11_1
//        """).show()

    //12. Extract the quarter (1-4) from a date column.
    val q12 = Seq(("2024-03-15"), ("2024-06-20")).toDF("date")
//    q12.withColumn("quarter", month(col("date")))
//      .withColumn("num", when(col("quarter") >= 1 && col("quarter") <= 3,"1").when(col("quarter") >= 4 && col("quarter") <= 6,"2")
//        .when(col("quarter") >= 7 && col("quarter") <= 9,"3").otherwise("4")).show()

    //spark-sql
//    q12.createOrReplaceTempView("qq12")
//    spark.sql(
//      """
//         select date, case
//         when month(date) between 1 and 3 then 1
//         when month(date) between 4 and 6 then 2
//         when month(date) between 7 and 9 then 3
//         else 4
//         end as abc from qq12
//        """).show()

    //13. Display the month as an abbreviation (e.g., "Mar" for March).
    val q13 = Seq(("2024-03-10"), ("2024-04-15")).toDF("date")
    //q13.withColumn("month", date_format(to_date(col("date"), "yyyy-MM-dd"), "MMMM")).show()

    //spark-sql
//    q13.createOrReplaceTempView("qq13")
//    spark.sql(
//      """
//        select date, date_format(date, "MMMM") as month from qq13
//        """).show()

    //14. Add 5 months to a date column.
    val q14 = Seq(("2024-01-01"), ("2024-02-15")).toDF("date")
    //q14.withColumn("add_months", add_months(col("date"),5)).show()

    //spark-sql
//    q14.createOrReplaceTempView("qq14")
//    spark.sql(
//      """
//        select date, add_months(date,5) from qq14
//        """).show()

    //15. Subtract 1 year from a date column.
    val q15 = Seq(("2024-08-10"), ("2025-10-20")).toDF("date")
    //q15.withColumn("add_months", add_months(col("date"),-12)).show()

    //saprk-sql
//    q15.createOrReplaceTempView("qq15")
//    spark.sql(
//      """
//        select date, add_months(date, -12) from qq15
//        """).show()

    //16. Round a timestamp column to the nearest hour.
    val q16 = Seq(("2024-03-10 12:25:30"), ("2024-03-10 12:55:45")).toDF("timestamp")
    //q16.withColumn("round_hour", date_trunc("Hour",col("timestamp"))).show()

    //spark-sql
//    q16.createOrReplaceTempView("qq16")
//    spark.sql(
//      """
//        select timestamp, date_trunc("Hour", timestamp) as round from qq16
//        """).show()

    //17. Calculate the date 100 days after a given date.
    val q17 = Seq(("2024-04-01"), ("2024-05-10")).toDF("date")
    //q17.withColumn("add_date", date_add(col("date"),100)).show()

    //spark-sql
//    q17.createOrReplaceTempView("qq17")
//    spark.sql(
//      """
//        select date, date_add(date, 100) from qq17
//        """).show()

    //18. Calculate the difference in weeks between two dates.
    val q18 = Seq(("2024-01-01", "2024-02-01"), ("2024-03-01", "2024-04-01")).toDF("start_date", "end_date")

    //spark-sql
//    q18.createOrReplaceTempView("qq18")
//    spark.sql(
//      """
//        select start_date, end_date, datediff(start_date, end_date) from qq18
//        """).show()

    //19. Check if a date falls in the current year.
    val q19 = Seq(("2024-01-15"), ("2023-12-25")).toDF("date")
    //q19.withColumn("year", when(year(col("date")) === year(current_date()), "current year").otherwise("other")).filter(col("year") === "current year").show()

    //spar-sql
//    q19.createOrReplaceTempView("qq19")
//    spark.sql(
//      """
//        select date from qq19 where year(date) == year(current_date())
//        """).show()

    //20. Convert a date to timestamp and format it as "yyyy-MM-dd HH:mm".
    val q20 = Seq(("2024-09-30"), ("2024-10-01")).toDF("date")
    //q20.withColumn("timestamp", date_format(to_timestamp(col("date")),"yyyy-MM-dd HH:mm")).show()

    //spark-sql
//    q20.createOrReplaceTempView("qq20")
//    spark.sql(
//      """
//        select date, date_format(to_timestamp(date), "yyyy-MM-dd HH:mm") from qq20
//        """).show()

    //21. Find the date of the next Sunday for a given date.
    val q21 = Seq(("2024-10-10"), ("2024-10-15")).toDF("date")
    //q21.withColumn("next_sunday", next_day(col("date"),"Sunday")).show()

    //spark-sql
//    q21.createOrReplaceTempView("qq21")
//    spark.sql(
//      """
//        select date, next_day(date, "Sunday") from qq21
//        """).show()

    //22. Display the date as "MMM dd, yyyy" format.
    val q22 = Seq(("2024-11-15"), ("2024-12-20")).toDF("date")
    //q22.withColumn("date_format", date_format(to_date(col("date"), "yyyy-MM-dd"),"MMM dd,yyyy")).show()

    //spark-sql
//    q22.createOrReplaceTempView("qq22")
//    spark.sql(
//      """
//        select date, date_format(to_date(date, "yyyy-MM-dd"), "MMM dd, yyyy") from qq22
//        """).show()

    //23. Calculate the date 30 business days after a given date.
    val q23 = Seq(("2024-01-01"), ("2024-01-07")).toDF("date")
    //q23.withColumn("30days", date_add(col("date"),40)).show()
    //q23.withColumn("day", dayofweek(col("date"))).show()

    //spark-sql
//    q23.createOrReplaceTempView("qq23")
//    spark.sql(
//      """
//        select date , date_add(date, 40) from qq23
//        """).show()

  }

}