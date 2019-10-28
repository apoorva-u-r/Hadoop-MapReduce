package com.abarag4.hw2.helpers

import scala.collection.mutable.ListBuffer

object StatisticsHelper {

  /**
   *
   * @param array Sorted array on which to perform the median computation
   * @return median value
   */
  def computeMedian(array: ListBuffer[Int]): Double = {

    if (array.length % 2 == 0) { //If array has even number of elements
      val median = (array(array.length/2).toDouble + array((array.length/2)-1).toDouble) / 2.0
      return median
    } else { //Else array has odd number of elements, so we just get the element in the middle
      val median = array(array.length/2).toDouble
      return median
    }
  }

  def computeAvg(array: ListBuffer[Int]): Double = {
    val sum = array.foldLeft(0.0) { (t,i) => t + i }
    val avg: Double = sum/array.size.toDouble
    return avg
  }

}
