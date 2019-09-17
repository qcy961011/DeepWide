package com.qiao

import com.intel.analytics.zoo.examples.recommendation.WNDParams

object MlDiver {
  def main(args: Array[String]): Unit = {
    val params = new WNDParams(inputDir="data/ml-1m")
    Ml1mWideAndDeep.run(params)
  }
}
