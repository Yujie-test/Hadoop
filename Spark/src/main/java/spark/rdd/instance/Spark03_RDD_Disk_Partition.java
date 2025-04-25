package spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark03_RDD_Disk_Partition {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO Spark读取文件可以传递路径，这个路径可以是相对路径也可以是绝对路径
        //      IDEA默认的相对路径是以项目的跟路径为基准，不是以模块的跟路径为基准

        // TODO 文件数据源分区设定也存在多个位置
        //      1. textFile可以传递第二个参数：minPartition（最小分区数）
        //          参数可以不需要传递的，那么Spark会采用默认值
        //              minPartition = math.min(defaultParallelism, 2)
        //      2.使用配置参数：spark.default.parallelism => 1 => math.min(参数, 2)
        //      3.采用环境默认总核数 => math.min(总核数, 2)


        // TODO Spark框架基于MR开发的
        //      Spark框架文件的操作没有自己的实现。采用MR库（Hadoop）来实现
        //      当读取文件的切片数量不是由Spark决定的，而是Hadoop决定的

        // Hadoop切片规则
        //      totalsize：3byte
        //      goalsize：3 / 2 => 1 byte
        //      partition：3 / 1 = 3
        JavaRDD<String> rdd = jsc.textFile("Spark/data/test.txt", 3);
        rdd.saveAsTextFile("Spark/output");

        jsc.close();

    }
}
