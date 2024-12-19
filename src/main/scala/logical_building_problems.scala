import scala.{:+, ::}
import scala.math.pow

object logical_building_problems {


  def main(args:Array[String]):Unit= {

//    1. Write a program to convert kg to g. (Input 56kg print in grams)

//    def kg_to_gram(a: Int): Unit = {
//      var gram = a * 1000
//      print("Kg to gram:"+gram)
//
//    }
    //kg_to_gram(56)

//    2. Write a program to covert temperature from degree C to F. (Input 80C)
//    (80°C × 9/5) + 32 = 176°F
//      def f_to_c(a : Double): Unit = {
//        var f = (a * 9/5) + 32
//        print("In Fahrenheit: "+f)
//
//      }
//      f_to_c(85.55)

//    3. Declare and initialize 3 three variable and print the biggest number.

//    def biggest(a:Int, b:Int, c:Int): Unit = {
//      var n = 0
//      if (a > b){
//        n = a
//      }
//      else{
//        n = b
//      }
//
//      if (n > c){
//        n = n
//      }
//      else{
//        n= c
//      }
//      print("biggest is:"+n)
//    }
//    biggest(1,2,3)

//    4. Write a java program that performs the following tasks.
//    a. Store a number in a variable
//      b. If value is not in range (100-1000) prints wrong number else follows
//    the steps
//      c. Check even or odd
//      d. If even divide the number by 3 and print the remainder
//    e. If odd divide the number by 2 and print the remainder.
//    def fiveprograms(a:Int): Unit = {
//      var n = a
//      if (n <= 1000 && n >= 100) {
//        if (n % 2 == 0) {
//          var remainder = n % 3
//          print(remainder)
//        }
//        else {
//          var remainder = n % 2
//          print(remainder)
//        }
//      }
//      else {
//        print("wrong number")
//      }
//    }
//    fiveprograms(1000)
//    fiveprograms(2000)
//    fiveprograms(105)

//    5. Declare & initialize a number. Check whether the number is in range 0-100 or not. If not in range print
//    invalid input. Else – if the number is in range 90-100 then print Super Smart, 80-90 print Smart,70-80 print
//    smart enough, 60-70 print just smart, 35-60 print no smart, 0-35 print dump.
//    def intelligence(a:Int): Unit = {
//      if (a > 100 || a < 0){
//        print("invalid input")
//      }
//      else if(a <= 100 && a >= 90 ){
//        print("Super Smart")
//      }
//      else if(a <90 && a >= 80){
//        print("Smart")
//      }
//      else if(a <80 && a >=70){
//        print("Smart Enough")
//      }
//      else if(a <70 && a >= 60){
//        print("Just Smart")
//      }
//      else if(a < 60 && a >=35){
//        print("No Smart")
//      }
//      else{
//        print("Dump")
//      }
//    }
//    intelligence(101)

//    6. Write a program to perform simple math based on the user inputs by using Switch condition.(+ , - , * , /)
//  def matchTest(x:Int, y:Int, z:String): {
//
//    case "add" => x+y
//    case "sub" => x-y
//    case "multiply" => x*y
//    case "division" => x/y
//
//  }

  //matchTest(3, 4, "add")

//    7. Write a program to print “SEEKHO BIGDATA”for 60 times.
//      def q7(a:String,b:Int): Unit = {
//
//        for(i <- 1 to b){
//          println(a)
//        }
//      }
//      q7("SEEKHO BIGDATA",60)

//    8. Write a program to print all numbers which are divisible by 11 from 250 to 550.
//      for (i <- 250 to 550){
//        if (i%11 == 0){
//          println(i)
//        }
//      }


//    9. Write a program to sum all the numbers from 56 to 153.
//    var sum1 = 0
//    for (i <- 56 to 153){
//
//        sum1 = sum1 + i
//      }
//      print(sum1)

//    10. Write a program to print all even numbers in range 700 to 900.
//      for(i <- 700 to 900){
//        if (i % 2 == 0){
//          print(i)
//        }
//      }

//    11. Write a program to print all odd numbers from 251 to 51. like (251,249,...51)
//      for (i <- 251 to 51 by -1){
//        if (i % 2 == 0){}
//        else{
//          print(i+",")
//        }
//      }


//    12. Write a Program to print the count of the even numbers between the given range?
//    var count = 0
//    for (i <- 10 to 20){
//        if (i % 2 == 0){
//          count = count + 1
//        }
//      }
//    print(count)

//    13. Write a program to print alternate even numbers from 20 to 140. Like (20,24,28...)
//      var a = Array(0)
//      for (i <- 10 to 20){
//              if (i % 2 == 0){
//                a = a :+ i
//              }
//            }
//    for ( x <- a )
//    {
//      print( x )
//    }
//    for ( x <- a){
//
//    }


    //    14. Write a program to print alternate even numbers from 20 to 140. Like (22,26,30...)
//      var x = List(2)
//      //x: List[Int] = List(2)
//
//      x = 1 :: x
//      print(x)

//    15. Print following series 2*3,3*4,4*5,......16*17 (Print in two ways – patter & multiplied value)


//    16. Write a program to sum all even numbers between 382 and 582.



//    17. Write a Program to print the all alphabets by using character Variable?
//    for (i <- 65 to 90) {
//      // Integer i with chr() will be converted to character
//      // before printing. chr() will take its equivalent
//      // character value
//      //print(i).toChar
//      var result = (i).toChar
//      print(result)
//    }


//    18. Write a program to find the average of 24,26,28,.....100.
//    var n18 = 0
//    var n18count = 0
//    for (i <- 24 to 100){
//        n18 = n18 + i
//        n18count = n18count + 1
//      }
//    println(n18)
//    print(n18/n18count)


//    19. Write programs to sum of the following Series. 52 + 62 + 72 +..........+1022.
//    var sum19 = 0
//    for(i <- 52 to 1022 by 10){
//        //print(i+",")
//        sum19 = sum19 + i
//      }
//    println(sum19)

//    20. Write a program to print A, B alternatively for 100 times. Ex: (A, B, A, B, A,B...)
//    for (i <- 1 to 100){
//
//      print("A, B, ")
//    }

//    21. Write a program to print the series : 10@9,9@8,8@7.......-5@-6
//      for (i <- 10 to -5 by -1){
//        for ( j <- 9 to -6 by -1){
//          if (i == j+1 ){
//            print(i + "@" + j + ",")
//          }
//          }
//        }

//    22. Write programs to print the following series. 100,200,300........10000
//      for (i <- 100 to 10000 by 100){
//        print(i+",")
//      }

//    23. Write programs to print the following series. 5^2, 7^2,9^2.....25^2
//      for (i <- 5 to 25 by 2){
//        print(i+"^2, ")
//      }

//    24. Write programs to print the following series. 5,10,5,10,5,10,5 for 7 times


//    25. Write programs to print the following series. 5*4,5*3,5*2,......5*(-12) (Print in two ways – patter & multiplied value)
//      for (i <- 4 to -12 by -1){
//        print("5*"+i+",")
//      }
//
//      for (i <- 4 to -12 by -1){
//        print(5*i+",")
//      }

//    26. Write programs to print the following series. 1,even,3,even,5,even,.......35,even
//      for (i <- 1 to 36){
//        if (i % 2 == 0){
//          print("even,")
//
//        }
//        else{print(i+",")}
//      }

//    27. Write programs to print the following series. 1,2,factor of three,4,5,factor
//    of three, 7,8,factor of three,..........22,23,factor of three.
//      for (i <- 1 to 24){
//        if (i % 3 == 0){
//          print("factor of three,")
//        }
//        else{print(i+",")}
//      }

//    28. Write programs to print the following series. 1,3, divisible by five, 7,9,
//    11,13, divisible by five,.....21,23, divisible by five
//      for (i <- 1 to 25){
//        if (i % 2 == 0){
//        }
//        else if (i % 5 == 0){
//          print("divisible by five,")
//        }
//        else{print(i+",")}
//      }

//    29. Write programs to print the following series. 0.5^2, 0.7^2,0.9^2....5.1^2
//      def q29(a: Double, b: Int): Unit = {
//        var exponent = pow(a,b)
//        print(exponent)
//      }
//      q29(0.5,2)
//      var n = 0.2
//      for( i <- 1 to 10 ){
//        n = n + 0.2
//        println(n)
//      }

//    30. Write a for loop that never ends?


//    var a = 2
//    for ( i <- 1 to ){
//
//      print(i)
//
//    }

//    31. Write a Loop inside other loop and observe the execution flow?


  }

}
