package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark04_Operate_Transform_FlatMap_1 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("Spark/data/test1.txt");

        // TODO map方法只负责转换数据(A -> B[B1, B2, B3])，不能将数据拆分后独立使用
        //  line => Hadoop hive flume
        //  sting[] => [Hadoop, hive, flume]
        //  flatmap方法可以将数据拆分后单独使用(A -> B1, B2, B3)
//        JavaRDD<String[]> mapRDD = rdd.map(
//                line -> line.split(" ")
//        );
        JavaRDD<String> flatMapRDD = rdd.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );
        flatMapRDD.collect().forEach(System.out::println);


        jsc.close();

    }
}
