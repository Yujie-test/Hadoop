package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.List;

public class Spark04_Operate_Transform_FlatMap {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<List<Integer>> lists = Arrays.asList(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4)
        );
        JavaRDD<List<Integer>> rdd = jsc.parallelize(lists, 2);

        // TODO RDD的转换方法：FlatMap
        //      flat(数据扁平化) + Map(映射)
        JavaRDD<Integer> flatMapRDD = rdd.flatMap(new FlatMapFunction<List<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(List<Integer> list) throws Exception {
                return list.iterator();
            }
        });


        jsc.close();

    }
}
