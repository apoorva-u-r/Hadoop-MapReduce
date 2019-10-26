package com.abarag4.hw2.helpers

object BinHelper {

  //Start: x,0,1,1
  private def getBinFromCountImpl(count: Int, currentPass: Int, currentMin: Int, currentMax: Int): String = {

    //Override for output format reasons
    if (count <= 1) {
      return "1";
    }

    //If count is inside, then return bin defined by min and max
    if (count>=currentMin && count<=currentMax) {
      return s"$currentMin-$currentMax";
    }

    getBinFromCountImpl(count, currentPass+1, currentMin+1+currentPass, currentMax+2+currentPass)
  }

  /*
   1
   2,3
   4,5,6
   7,8,9,10
   11,12,13,14,15
    */

  /*
  * This function implements the bin generation policy for task 1.
  * Bin size grows incrementally by 1; in this way we counter-balance the increased sparsity of the dataset when the number of authors increases.
   */
  def getBinFromCount(count: Int) : String = {
    return getBinFromCountImpl(count, 0, 1, 1)
  }

  private def getBinForYearsImpl(year: Int, currentMin: Int, currentMax: Int) : String = {

    if (year>=currentMin && year<=currentMax) {
      return s"$currentMin-$currentMax";
    }

    return getBinForYearsImpl(year, currentMin+10, currentMax+10)
  }

  /*
  1901-1910
  1911-1920
  1921-1930
   */
  def getBinForYears(year: Int) : String = {
    return getBinForYearsImpl(year, 1901, 1910)
  }
}
