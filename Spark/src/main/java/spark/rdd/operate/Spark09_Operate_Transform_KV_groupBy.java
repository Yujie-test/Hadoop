package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark09_Operate_Transform_KV_groupBy {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = jsc.parallelize(nums, 2);

        // TODO groupBy:按照指定的规则对数据进行分组
        //      给每一个数据增加一个标记，相同的标记的数据会放置在一个组中，这个标记就是组名
        //      groupBy结果就是KV类型的数据 (0, [2, 4]), (1, [1, 3])

        JavaPairRDD<Integer, Iterable<Integer>> groupRDD = rdd.groupBy(
                num -> num % 2
        );
        groupRDD.mapValues(
                iter -> {
                    int sum = 0;
                    Iterator<Integer> iterator = iter.iterator();
                    while (iterator.hasNext()) {
                        Integer num = iterator.next();
                        sum = sum + num;
                    }
                    return sum;
                }
        ).collect().forEach(System.out::println);

        jsc.close();

    }
}
