package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark05_Operate_Transform_groupBy_1 {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = jsc.parallelize(nums, 2);

        // TODO RDD的方法：groupBy，按照指定的规则分组
//        rdd.groupBy(new Function<Integer, Object>() {
//            @Override
//            public Object call(Integer num) throws Exception {
//                if (num % 2 == 0) {
//                    return "even";
//                } else {
//                    return "odd";
//                }
//                return num % 2 == 0;
//            }
//        })
        rdd.groupBy(num -> num % 2 == 0)
                .collect().forEach(System.out::println);


        jsc.close();

    }
}
