package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark07_Operate_Transform_sortBy {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 3, 2, 4, 33, 11);
        JavaRDD<Integer> rdd = jsc.parallelize(nums, 3);

        // TODO RDD的方法：sortBy：按照指定的排序规则对数据进行排序
        //      sortBy方可以传递三个参数
        //          第一个参数表示排序规则
        //              Spark会为每一个数据加一个标记，然后按照标记对数据进行排序
        //          第二个参数表示排序的方式：升序（true），降序（false）
        //          第三个参数表示分区数量
        /*
        数据：1, 33, 3, 2, 4, 11
        -------------------
        标记：1, 33, 3, 2, 4, 11
        =>
        标记：1, 2, 3, 4, 11, 33
        -------------------
        数据：1, 2, 3, 4, 11, 33

        数据：1, 33, 3, 2, 4, 11
        -------------------
        标记："1", "33", "3", "2", "4", "11"
        =>
        标记："1", "11", "2", "3", "33", 4
        -------------------
        数据：1, 11, 2, 3, 33, 4

         */
        //rdd.saveAsTextFile("Spark/output1");
//        rdd.sortBy(
//                num -> num, true, 2
//        )
        rdd.sortBy(
                        num -> "" + num, true, 2
                )
                .collect().forEach(System.out::println);
                        //.saveAsTextFile("Spark/output");


        jsc.close();

    }
}
