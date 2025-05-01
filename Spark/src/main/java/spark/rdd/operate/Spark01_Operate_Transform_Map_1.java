package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark01_Operate_Transform_Map_1 {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Integer> nums = Arrays.asList(1, 2, 3, 4);

        // TODO RDD的方法
        JavaRDD<Integer> rdd = jsc.parallelize(
                nums, 2
        );

        // TODO map方法的作用就是将传入的A转换为B返回，但并没有限制A和B的关系
        JavaRDD<Integer> newRDD = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer in) throws Exception {
                return in * 2;
            }
        });

        JavaRDD<String> newRDD1 = rdd.map(new Function<Integer, String>() {
            @Override
            public String call(Integer in) throws Exception {
                return in + "abc";
            }
        });
        newRDD.collect().forEach(System.out::println);
        newRDD1.collect().forEach(System.out::println);
        System.out.println("****************************************");
        System.out.println(nums);


        jsc.close();

    }
}
