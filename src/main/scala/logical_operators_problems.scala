

object logical_operators_problems {

  def main(args: Array[String]): Unit = {


//    1)Check for Even AND Positive Number:
//      Write a Scala function to check if a given number is both even and positive.
//    Sample Data: Number: 14
//    val n = 14
//    if (n > 0) {
//      if (n % 2 == 0){
//      println(s"The number $n is positive and even")}
//    }
//    else{}


//    2)Range Check with OR:
//    Create a Scala program to determine if a given value is either less than -10 or greater than 10.
//    Sample Data: Value: -15
//    if ( -10 to 10 contains(-15)){print("In range")}
//    else{print("not in range")}

//    3)Odd Number Check with AND:
//    Implement a Scala function to check if a given number is odd and not divisible by 3.
//    Sample Data: Number: 27
//      val n = 27
//      if (n % 2 != 0 && n % 3 != 0){print("done")}
//      else{print("not done")}

//
//    4)Divisibility by 4 OR 6:
//    Write a Scala program to check if a given number is divisible by either 4 or 6.
//    Sample Data: Number: 18
//      val n = 18
//      if ( n% 4 ==0 || n%6 == 0 ){print("ok")}
//      else{print("not ok")}
//
//    5)Eligibility for Voting OR Driving:
//      Create a Scala program to check if a person is eligible to vote (age greater than or equal to 18) or
//      eligible to drive (age greater than or equal to 16).
//      Sample Data: Age: 20
//      val age = 15
//      if (age >= 16 ) {
//        if (age >= 18) {
//          print("person is eligible to vote")
//        } else {
//          print("eligible to drive")
//        }
//      }
//      else{
//        print("not eligible to drive and vote")}

//    6)Multiple Range Check:
//    Write a Scala function to check if a given number is in the range [1, 10] or [20, 30].
//    Sample Data: Number: 25
//      val i = 20
//      if (i >= 1 && i <= 20){
//        print("between 1 and 20")
//      }
//      else if (i >= 20 && i <= 30){
//        print("between 20 and 30")
//      }
//      else(print("not in range"))

//    7)Check for Negative AND Odd Number:
//    Implement a Scala program to check if a given number is both negative and odd.
//    Sample Data: Number: -7
//      val i = -7
//      if (i < 0 && i%2 != 0){
//        print("negative and odd")
//      }
//      else{print("either not negative or num even")}

//    8)Eligibility for Senior Discount OR Student Discount:
//    Create a Scala program to check if a person is eligible for a senior citizen discount (age greater than
//    60) or a student discount (age less than 25).
//    Sample Data: Age: 63
//      val age = 60
//      if (age > 60){
//        print("senior citizen discount")
//      }
//      else if ( age < 25){
//        print("student discount")
//      }
//      else{print("not eligible for discount")}

//    9)Divisibility by 5 AND 7:
//    Write a Scala function to check if a given number is divisible by both 5 and 7.
//    Sample Data: Number: 35
//      val i = 40
//      if ( i % 5 == 0 && i % 7 == 0){
//        print("divisible by 5 n 7")
//      }
//      else{
//        print("not divisible")
//      }

//    10)Check for Non-Negative OR Even Number:
//      Create a Scala program to check if a given number is either non-negative or even.
//      Sample Data: Number: -8
//      val i = 0
//      if ( i > 0 || i % 2 == 0 ){
//        print("non-negative or even")
//      }

//    11)Check for Prime AND Odd Number:
//    Write a Scala function to check if a given number is both a prime number and an odd number.
//    Sample Data: Number: 17

//    var isPrime = true
//    val num = 17
//    if (num <= 1) {
//      isPrime = false
//    } else {
//      for (i <- 2 to Math.sqrt(num).toInt if isPrime) {
//        if (num % i == 0) {
//          isPrime = false
//        }
//      }
//    }
//
//    if (isPrime) {
//      println(s"$num is a prime number.")
//    } else {
//      println(s"$num is not a prime number.")
//    }
    //if isPrime: This is a guard condition (a filter) that prevents unnecessary iterations. The loop will only run if isPrime is still true.
    //If a divisor is found and isPrime becomes false, the loop stops evaluating further iterations.


//    12)Eligibility for Discount OR Free Shipping:
//      Create a Scala program to check if a customer is eligible for a discount (purchase amount greater
//      than 150) or qualifies for free shipping (purchase amount greater than 100).
//      Sample Data: Purchase Amount: 120
//      val i = 120
//      if ( i > 150 ){
//        print("eligible for a discount and delivery")
//      }
//      else if ( i > 100 && i <= 150){
//        print("eligible for a discount")
//      }
//      else{print("not eligible")}

//    13)Divisibility by 3 OR 8:
//    Write a Scala function to check if a given number is divisible by either 3 or 8.
//    Sample Data: Number: 24
//      val i = 24
//      if(i % 3 == 0 || i%8 == 0){
//        print("divisible by 3 n 8")
//      }
//      else{print("not divisible")}

//    14)Check for Non-Positive AND Even Number:
//      Implement a Scala program to check if a given number is both non-positive and even.
//      Sample Data: Number: -6
//      val i = -7
//      if (i < 0 && i % 2 == 0){
//        print("Non-Positive AND Even Number")
//      }
//      else{print("not eligible")}

//    15)Age Group Classification with AND:
//    Create a Scala program to classify a person&#39;s age group. Classify them as a child (less than 13),
//    teenager (between 13 and 19), and an adult (20 and above) using both logical AND and OR.
//    Sample Data: Age: 15
//      val age = 20
//      if( age < 13){
//        print("child")
//      }
//      else if(age >= 13 && age <= 19){
//        print("teenager")
//      }
//      else{print("adult")}

//    16)Check for Divisibility by 2 OR 5:
//    Write a Scala function to check if a given number is divisible by either 2 or 5.
//    Sample Data: Number: 25
//      val n = 27
//      if ( n % 2 == 0 || n % 5 == 0){
//        print("Divisibility by 2 OR 5")
//      }
//      else{print("not divisible")

//    17)Check for Multiple of 3 AND 7:
//    Implement a Scala function to check if a given number is both a multiple of 3 and 7.
//    Sample Data: Number: 21
//      val n = 21
//      if (n % 3 == 0 && n % 7 == 0){
//        print("Multiple of 3 AND 7")
//      }
//      else{print("not multiple")}

//    18)Divisibility by 5 OR 9:
//    Write a Scala program to check if a given number is divisible by either 5 or 9.
//    Sample Data: Number: 45
//      val n = 45
//      if ( n % 5 == 0 || n % 9 == 0){
//        print("divisible by either 5 or 9")
//      }
//      else{
//        print("not divisible")
//      }

//    19)Check for Odd AND Not Divisible by 4:
//    Create a Scala program to check if a given number is both odd and not divisible by 4.
//    Sample Data: Number: 15
//      val n = 15
//      if (n % 2 != 0 && n % 4 != 0){
//        print("Odd AND Not Divisible by 4")
//      }
//      else{
//        print("divisible")
//      }

//    22)Check for Divisibility by 3 AND 5:
//    Write a Scala function to check if a given number is divisible by both 3 and 5.
//    Sample Data: Number: 15
//      val n = 15
//      if ( n % 3 == 0 && n % 5 == 0){
//        print("Divisibility by 3 AND 5")
//      }
//      else{print("not divisible")}

//    23)Eligibility for Discount OR Membership Benefits:
//      Create a Scala program to check if a customer is eligible for a discount (purchase amount greater
//      than 200) or qualifies for membership benefits (loyalty card available).
//    Sample Data: Purchase Amount: 180 Loyalty Card: true
//      val PurchaseAmount = 180
//      val LoyaltyCard = true
//      if (PurchaseAmount > 200 && LoyaltyCard == true){
//        print("eligible for a discount and membership benefits")
//      }
//      else if (PurchaseAmount  <= 200 && LoyaltyCard == true) {
//        print("eligible for membership benefits only")
//      }
//      else if (PurchaseAmount  > 200 && LoyaltyCard == false) {
//        print("eligible for discount only")
//      }
//      else{
//        print("not eligible for discount and membership benefits")
//      }

//    24)Divisibility by 2 OR 3:
//    Write a Scala function to check if a given number is divisible by either 2 or 3.
//    Sample Data: Number: 9
//      val n = 9
//      if ( n % 2 == 0 ){
//        print("divisible by 2")
//      }
//      else if ( n % 3 == 0){
//        print("divisible by 3")
//      }

//    25)Check for Positive AND Not Divisible by 3:
//    Implement a Scala program to check if a given number is positive and not divisible by 3.
//    Sample Data: Number: 7
//      val n = 7
//      if ( n > 0 && n % 3 == 0){
//        print("+ve and div by 3")
//      }
//      else{
//        print("not div")
//      }

//    26)Eligibility for Senior Discount AND Not a New Customer:
//    Create a Scala program to check if a person is eligible for a senior citizen discount (age greater than 65) and is not a new customer.
//    Sample Data: Age: 70 New Customer: false


//    27)Check for Odd OR Prime Number:
//    Write a Scala function to check if a given number is either odd or a prime number.
//    Sample Data: Number: 11

//    28)Eligibility for Discount AND Free Shipping:
//      Create a Scala program to check if a customer is eligible for a discount (purchase amount greater than 150)
//      and qualifies for free shipping (purchase amount greater than 100).
//      Sample Data: Purchase Amount: 120
//      val PurchaseAmount = 100
//      if ( PurchaseAmount > 150 ){
//        print("eligible for a discount and free shipping")
//      }
//      else if (PurchaseAmount >100 && PurchaseAmount <= 150 ) {
//        print("eligible for free shipping")
//      }
//      else{
//        print("not eligible for a discount and free shipping")
//      }

//    29)Check for Non-Negative AND Not Divisible by 7:
//    Implement a Scala program to check if a given number is non-negative and not divisible by 7.
//    Sample Data: Number: 14


//    30)Eligibility for Student Discount OR Free Trial:
//      Write a Scala program to check if a person is eligible for a student discount (age less than 25) or is eligible for a free trial.
//      Sample Data: Age: 22 Free Trial: true


//    31)Check for Divisibility by 4 OR 6:
//    Create a Scala function to check if a given number is divisible by either 4 or 6.
//    Sample Data: Number: 24

  }
}