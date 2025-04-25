package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Spark01_Operate {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO RDD的方法
        jsc.parallelize(
                Arrays.asList(1, 2, 3)
        );

        // RDD的方法会有很多，主要讲解核心，重要的方法
        // 学习的重点：
        //      1.名字
        //      2.

        jsc.close();

    }
}
