class Device(val phoneName: String) {
      def display = s"Phone is $phoneName"
      override def toString = s"$phoneName"
}
