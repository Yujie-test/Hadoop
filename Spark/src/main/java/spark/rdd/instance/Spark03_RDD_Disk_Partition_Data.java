package spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark03_RDD_Disk_Partition_Data {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO Spark进行分区处理时，需要对每个分区的数据尽快的能平均分配
        //      totalsize = 7
        //      goalsize = totalsize / minpartnum = 7 / 2 = 3
        //      partnum = totalsize / goalsize = 7 / 3 = 2...1 => 2 + 1 = 3

        // TODO Spark不支持文件操作，文件操作都是由Hadoop完成的
        //      Hadoop进行文件切片数量的计算和文件数据存储的计算规则不一样
        //      1.分区数量计算的时候，考虑的是尽可能的平均：按字节来计算
        //      2.分区数量的存储是考虑业务数据的完成性：按照行来读取
        //          读取数据时，还需要考虑数据偏移量，偏移量从0开始
        //          读取数据时，相同的偏移量不能重复读取
        JavaRDD<String> rdd = jsc.textFile("Spark/data/test.txt", 2);
        rdd.saveAsTextFile("Spark/output");

        jsc.close();

    }
}
