package spark.rdd.instance;

import org.apache.spark.api.java.JavaSparkContext;

public class Spark01_Env_1 {
    public static void main(String[] args) {

        // TODO 构建Spark的运行环境

        // TODO 创建Spark配置对象

        //  SparkException: A master URL must be set in your configuration
        final JavaSparkContext javaSparkContext = new JavaSparkContext("local", "spark");

        // TODO 释放资源
        javaSparkContext.close();

    }
}
