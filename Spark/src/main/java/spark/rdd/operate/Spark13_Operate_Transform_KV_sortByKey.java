package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Spark13_Operate_Transform_KV_sortByKey {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(
                Arrays.asList(
                        new Tuple2<>("a", 1),
                        new Tuple2<>("b", 2),
                        new Tuple2<>("a", 3),
                        new Tuple2<>("b", 4)
                )
        );
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(t -> t);

        // TODO sortByKey方法
        //      groupByKey：按照K对V进行分组
        //      reduceByKey：按照K对V进行两两聚合
        //      sortByKey：按照K排序
        JavaPairRDD<String, Integer> sortRDD = mapRDD.sortByKey(false);
        sortRDD.collect().forEach(System.out::println);
        jsc.close();

    }
}
