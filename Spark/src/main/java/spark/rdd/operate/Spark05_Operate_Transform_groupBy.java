package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Spark05_Operate_Transform_groupBy {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = jsc.parallelize(nums, 2);

        // TODO RDD的方法：groupBy，按照指定的规则分组
        rdd.groupBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer num) throws Exception {
                // 返回的值其实就是数据对应组的名称，相同组的名称的数据会放置在一个组中
                // 当前的逻辑就是给数据增加标记
                // 1 -> B
                // 2 -> B
                // 3 -> B
                // 4 -> B
                return "a"; // 组的名称，此处需要实现分组逻辑
            }
        })
                // 逻辑执行完毕后，打印的结果只有一行(a,[1, 2, 3, 4])，
                // 一行数据就表示一个组，组的名称是a，组内的数据就是1, 2, 3, 4
                .collect().forEach(System.out::println);


        jsc.close();

    }
}
