package spark_cube

import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source

object cube {

  def get_12_months(yyyymm: String) : Array[String] = {
    val months = new Array[String](12)
    var yyyy = yyyymm.substring(0,4).toInt
    var mm = yyyymm.substring(4,6).toInt

    months{0} = yyyymm
    for ( a <- 1 to 11){
      mm = mm - 1
      if (mm == 0 ){
        mm = 12
        yyyy = yyyy - 1
      }

      if ( mm < 10) {
        months{a} = yyyy.toString.concat("0".concat(mm.toString))
      }else{
        months{a} = yyyy.toString.concat(mm.toString)
      }
    }

    return months
  }

  def stripChars(s:String, ch:String) = s filterNot(ch contains _)

  def read_combo_file(combo_file:String):Array[String]={
    val lines = Source.fromFile(combo_file).getLines()

    val ret_val = new Array[String](lines.length-1)

    var i = 0
    for (line <- Source.fromFile(combo_file).getLines()) {
      if (i >= 1) {
        ret_val.update(i-1, line)
      }
      i = i + 1
    }

    return ret_val
  }

  def main (args: Array[String]){
    val file = args{0}
    val combo_file = args{1}
    val yyyymm = args{2}

    // Read all combos need to computer
    val combos = read_combo_file(combo_file)
    // Find out all the 12 months
    val all_months = get_12_months(yyyymm)

    val conf = new SparkConf().setAppName("Spark_cube").setMaster("local")

    val sc = new SparkContext(conf)

    // Load the file in Spark
    val data = sc.textFile(file)

    // Read the header
    val header = data.first()

    // Get all the data within 12 months
    val rdd = data.filter(_!=header).filter(all_months contains  _.split(","){6})

    // Compute the sum of 12 months' wage for each person
    val personal_sum = rdd.map(line => {
      val fields = line.split(",")
      // Person + job + ind + state + rate_type
      val key = fields{0} + "," +fields{1} + "," + fields{2} + "," + fields{3} + "," + fields{4}
      val value = fields{5}.toDouble
      (key,value)
    }).reduceByKey(_ + _)

    // Iterate all combos
    for(combo <- combos){
      val vars = combo.split(",")
      val combo_num = vars{0}

      val rdd = personal_sum.map(line =>{
        val fields = stripChars(line.toString(),"()").split(",")
        var key = ""+combo_num
        for(i <- 1 to vars.length-1){
          if(vars{i} == "1"){
            key = key + "," + fields{i}
          }else{
            key = key + ","
          }
        }
        val value = fields{5}.toDouble
        (key,value)
      }).combineByKey(
        (x:Double)=>(x,1),
        (acc:(Double,Int),x:Double) => (acc._1 + x,acc._2 + 1),
        (acc1:(Double, Int), acc2:(Double, Int)) => (acc1._1 + acc2._2, acc1._2 + acc2._2)
      ).map{case (key, value) => (key, value._1 / value._2.toFloat)}
      rdd.foreach(println)
    }
  }
 }
