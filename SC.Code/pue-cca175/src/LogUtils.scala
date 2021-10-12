
package com.loudacre.utilslib

class LogUtils(val line: String) {
// A function getDevId that, given a line from a device log file, returns the Device ID string
      def getDevId ( line: String ) {
          val line_split = line.split (",")                       // Buffer the results of split
          val device_ID = ( line_split (2) )                       // Print out field 2, the Unique ID
      }

// A function getModel that, given a line from a device log file, returns the Model name
      def getModel ( line: String ) {
          val line_split = line.split(",")                       // Buffer the results of split
          val Model = ( line_split (1) )                                       // Print out field 1, the Model
      }
}
