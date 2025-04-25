package spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Spark03_RDD_Disk {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO 构建RDD数据处理模型
        //  利用环境对象对接磁盘（文件）数据源，构建RDD对象

        // textFile可以传递一个参数：文件路径
        JavaRDD<String> rdd = jsc.textFile("/Users/chenyujie/Desktop/Hadoop/Spark/data");
        List<String> collect = rdd.collect();
        collect.forEach(System.out::println);

        jsc.close();

    }
}
