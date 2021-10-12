import scala.io.Source
object Filter2{
  def main(args: Array[String]) {
      val fname = "../files/loudacre.log"
      var flines = Source.fromFile(fname).getLines.filter(_ contains "4000")
      var device_ID = " "
      var Model = " "
      for(line <- flines) {
          // println("flines is:" + flines.getClass)                       // flines is:class scala.io.BufferedSource$BufferedLineIterator
          // println("line is:" + line.getClass)                           // line is:class java.lang.String
          var line_string: String = line      				// Use line to populate a String
          if( line_string.contains("4000") ) { 
	  // println("line_string is: " + line_string.getClass)            // line_string is: class java.lang.String
              var line_split = line_string.split(",")      			// Buffer the results of split
	  // println("line_split is: " + line_split.getClass)              // line_split is: class [Ljava.lang.String;
              device_ID = (line_split(2))             			// Print out field 2, the Unique ID
	  // println("device_ID is: " + device_ID.getClass)                // device_ID is: class java.lang.String 
              println("Device ID: " + device_ID)                            // Device ID: 1882b564-c7e0-4315-aa24-228c0155ee1b
              Model = (line_split(1))             				// Print out field 1, Model name and number
              println("Model: " + Model)         
          }                          
     }
  }
}

