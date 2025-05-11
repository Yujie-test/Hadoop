package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark05_Operate_Transform_groupBy_2 {
    public static void main(String[] args) throws Exception {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> rdd = jsc.parallelize(nums, 3);

        // TODO RDD的方法：groupBy，按照指定的规则分组

        // 含有shuffle操作的方法都具有改变分区的能力，可以设定分区参数
        rdd.groupBy(num -> num % 2 == 0, 2)
                .collect().forEach(System.out::println);
        System.out.println("计算完毕");
        // 监控页面：http://localhost:4040
        Thread.sleep(10000000L);


        jsc.close();

    }
}
