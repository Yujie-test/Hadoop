package spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory_partition_Data {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("spark");
        conf.set("spark.default.parallelism", "4");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        final List<Integer> names = Arrays.asList(1, 2, 3, 4, 5);

        // TODO Spark分区数据的存储基本原则：平均分

        final JavaRDD<Integer> rdd = jsc.parallelize(names, 3);

        rdd.saveAsTextFile("output");

        jsc.close();

    }
}
