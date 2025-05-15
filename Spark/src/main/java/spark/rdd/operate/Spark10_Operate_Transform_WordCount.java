package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Spark10_Operate_Transform_WordCount {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        final JavaSparkContext jsc = new JavaSparkContext(conf);

        // TODO 分组聚合
        //      1. 读取文件
        JavaRDD<String> lineRDD = jsc.textFile("Spark/data/word.txt");
        //      2. 将文件数据进行分解
        JavaRDD<String> wordRDD = lineRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //      3. 将相同的单词分到一个组中
        //          Hello, Hello, Hello
        //          (Hello, [Hello, Hello, Hello])
        JavaPairRDD<String, Iterable<String>> wordGroupRDD = wordRDD.groupBy(word -> word);
        //      4. 计算每个单词的组中的数量即可
        JavaPairRDD<String, Integer> wordCountRDD = wordGroupRDD.mapValues(
                iter -> {
                    int cnt = 0;
                    for (String s : iter) {
                        cnt++;
                    }
                    return cnt;
                }
        );
        wordCountRDD.collect().forEach(System.out::println);
        jsc.close();

    }
}
