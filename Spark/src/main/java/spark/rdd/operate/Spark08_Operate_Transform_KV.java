package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Spark08_Operate_Transform_KV {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO KV类型一般表示2元组
        //      Spark RDD会整体数据的处理称之为单值类型的数据处理
        //      Spark RDD对KV数据个体的处理称之为KV类型的数据处理：K和V不作为整体使用
        Tuple2<String, Integer> a = new Tuple2<>("a", 1);
        Tuple2<String, Integer> a1 = new Tuple2<>("a", 2);
        Tuple2<String, Integer> a2 = new Tuple2<>("a", 3);

        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(a, a1, a2);
//        JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(tuple2s);
//
//        rdd.map(
//                t -> new Tuple2<>(t._1, t._2 * 2)
//        ).collect().forEach(System.out::println);

        // TODO 上面的代码不是对KV类型数据进行处理，是将2元组当做一个整体来使用的
        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(tuple2s);
        pairRDD.mapValues(
                num -> num * 2
        ).collect().forEach(System.out::println);

        jsc.close();

    }
}
