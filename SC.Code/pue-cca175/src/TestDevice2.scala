package com.cloudera.training
import com.loudacre.phonelib.Device2

object TestDevice2{
       def main(args: Array[String]) {
           val a = new Device2("iFruit 3000")
           println(a.display)
       }
}
