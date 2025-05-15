package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Spark13_Operate_Transform_KV_sortByKey_2 {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("a", 3),
                        new Tuple2<>("a", 2),
                        new Tuple2<>("a", 4)
                )
        );
        // (a, 1) => (1, (a, 1))
        // (a, 3) => (3, (a, 3))
        // (a, 2) => (2, (a, 2))
        // (a, 4) => (4, (a, 4))
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(t -> t);
        JavaPairRDD<Integer, Tuple2<String, Integer>> mapRDD1 = mapRDD.mapToPair(kv -> new Tuple2<>(kv._2, kv));
        mapRDD1.sortByKey().map(t -> t._2).collect().forEach(System.out::println);
        jsc.close();
    }
}
