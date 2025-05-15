package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark12_Operate_Transform_KV_wordCount {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO groupByKey方法作用是KV类型的数据直接按照K进行分组
        //      (a,[1, 3])
        //      (b,[2, 4])
        List<Tuple2<String, Integer>> tuple2s = Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("a", 3),
                new Tuple2<>("b", 4)
        );
        JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(tuple2s);
        JavaPairRDD<String, Integer> mapRDD = rdd.mapToPair(t -> t);

        // TODO 将分组聚合功能进行简化操作
        //      reduceByKey方法的作用：将KV类型的数据按照K对V进行reduce（将多个值聚合成一个值）操作
        //          [1, 3] => 4
        //          [2, 4] => 6
        //      计算的基本思想：两两计算
        //      (i1, i2) => i3
        //JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(Integer::sum);
//        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(
//                new Function2<Integer, Integer, Integer>() {
//                    @Override
//                    public Integer call(Integer v1, Integer v2) throws Exception {
//                        return v1 + v2;
//                    }
//                }
//        );
//        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(
//                (i1, i2) -> i1 + i2
//        );
//        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(
//                SumTest::sum
//        );
        JavaPairRDD<String, Integer> wordCountRDD = mapRDD.reduceByKey(Integer::max);

        wordCountRDD.collect().forEach(System.out::println);
        jsc.close();

    }
}
class SumTest {
    public static int sum(Integer v1, Integer v2) {
        return v1 + v2;
    }
}