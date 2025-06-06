package spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Memory_partition {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("spark");
        conf.set("spark.default.parallelism", "4");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO Kafka可以将数据进行切片（减小规模），也称之为分区，分区操作是底层完成的
        //  local环境中，分区数量和环境核数相关，但是一般不推荐
        //  分区数量需要手动设定
        //      Spark在读取集合数据时，分区存在3种不同场合
        //          1. 优先使用方法参数
        //          2. 使用配置参数：spark.default.parallelism
        //          3. 采用环境默认值
        final List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu");
        // parallelize方法可以传递两个参数
        //  第一个参数表示对接的数据源集合
        //  第二个参数表示切片（分区）数量
        //      可以不需要制定，spark会采用默认值进行分区（切片）
        //          numSlices = scheduler.conf.getInt("spark.default.parallelism", totalCores)
        //          从配置对象中获取配置参数：spark.default.parallelism（默认并行度）
        //              如果配置参数不存在，那么默认取值为totalCores（当前环境总的虚拟核数）
        final JavaRDD<String> rdd = jsc.parallelize(names, 3);

        // TODO 将数据模型分区后的数据保存到磁盘文件中
        //  saveAsTextFile方法可以传递一个参数，表示输出文件的路径，路径可以是绝对路径，也可以是相对路径
        //  idea默认的相对路径是以项目（Spark）的根路径为准的
        rdd.saveAsTextFile("output");

        jsc.close();

    }
}
