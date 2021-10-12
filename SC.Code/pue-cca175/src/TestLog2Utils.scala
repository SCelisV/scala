object TestLog2Utils{
       def main(args: Array[String]) {
           val record = "2014-03-15:10:10:31, Titanic 4000, 1882b564-c7e0-4315-aa24-228c0155ee1b, 58, 36, 39, 31, 15, 0, TRUE, enabled, enabled, 37.819722, -122.478611"
           // println(record)
           // println(record.getClass)		        // class java.lang.String
           val L2U = new Log2Utils(record)
           // println(L2U.getClass)			// class Log2Utils
           println("test :" + L2U.getDevId(record))
           println("test :" + L2U.getModel(record))
	}
}
