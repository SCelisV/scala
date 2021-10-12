package com.loudacre.phonelib

class Device2(val phoneName: String) {
      def display = s"Phone is $phoneName"
      override def toString = s"$phoneName"
}
