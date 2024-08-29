package org.apache.spark.sql.shuffle

import com.prx.starry.Starry
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StarryConf

class StreamingShuffleManagerSuite extends ShuffleManagerSuite {

  override protected def spark: SparkSession = {
    val conf1 = new SparkConf()
    conf1.set("spark.sql.starry.columnar.columnarShuffleEnabled", "true")
    conf1.set(StarryConf.STREAMING_SHUFFLE_ENABLED.key, "true")
    conf1.set("spark.sql.shuffle.partitions", "2")
    conf1.setMaster("local[4]")
    Starry.starrySession(conf1)
  }
}
