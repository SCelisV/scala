object Model3ToBrand {
  def main(args: Array[String]) {
      val mod2b = Map( ("1","iFruit"),("2","iFruit"),("3","iFruit"),
                       ("3A","iFruit"),("4","iFruit"),("4A","iFruit"),
                       ("5","iFruit"), ("S1","Ronin"), ("S2","Ronin"),
                       ("S3", "Ronin"), ("F01L","Sorrento"), ("F11L","Sorrento"),
                       ("F21L","Sorrento"), ("F23L","Sorrento"), ("F33LL","Sorrento"), 
                       ("F41L","Sorrento")  )
 
      if(mod2b.contains(args(0) )) {  
         println(" Brand is: " + mod2b(args(0)) ) 
      } else { 
         println(" Record not found ") 
      }
  }
}
