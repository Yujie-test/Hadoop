package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Spark11_Operate_Transform_KV_wordCount {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // groupBy方法底层实现时，调用了groupByKey
//        JavaRDD<Tuple2<String, Integer>> rdd = jsc.parallelize(
//                Arrays.asList(
//                        new Tuple2<>("a", 1),
//                        new Tuple2<>("b", 2),
//                        new Tuple2<>("a", 3),
//                        new Tuple2<>("b", 4)
//                )
//        );
        // TODO 将数据的一个值用于分组
        //      (a, [(a, 1), (a, 3)])
        //      (b, [(b, 2), (b, 4)])
//        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = rdd.groupBy(
//                t -> t._1
//        );
        // TODO groupByKey方法作用是KV类型的数据直接按照K进行分组
        //      (a,[1, 3])
        //      (b,[2, 4])
        jsc.parallelizePairs(
                        Arrays.asList(
                                new Tuple2<>("a", 1),
                                new Tuple2<>("b", 2),
                                new Tuple2<>("a", 3),
                                new Tuple2<>("b", 4)
                        )
                ).groupByKey()
                .mapValues(
                        iter -> {
                            int sum = 0;
                            Iterator<Integer> iterator = (Iterator<Integer>)iter.iterator();
                            while (iterator.hasNext()) {
                                int num = iterator.next();
                                sum += num;
                            }
                            return sum;
                        }
                )
                .collect().forEach(System.out::println);
        jsc.close();

    }
}
