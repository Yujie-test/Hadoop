package spark.rdd.operate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;

public class Spark13_Operate_Transform_KV_sortByKey_1 {
    public static void main(String[] args) {

        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        User user1 = new User();
        user1.age = 30;
        user1.amount = 2000;

        User user2 = new User();
        user2.age = 40;
        user2.amount = 3000;

        User user3 = new User();
        user3.age = 30;
        user3.amount = 3000;

        User user4 = new User();
        user4.age = 40;
        user1.amount = 2500;

        final JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Tuple2<User, Integer>> rdd = jsc.parallelize(
                Arrays.asList(
                        new Tuple2<>(user1, 1),
                        new Tuple2<>(user2, 2),
                        new Tuple2<>(user3, 3),
                        new Tuple2<>(user4, 4)
                )
        );
        JavaPairRDD<User, Integer> mapRDD = rdd.mapToPair(t -> t);

        // TODO sortByKey方法
        //      groupByKey：按照K对V进行分组
        //      reduceByKey：按照K对V进行两两聚合
        //      sortByKey：按照K排序

        // TODO java.lang.ClassCastException: spark.rdd.operate.User cannot be cast to java.lang.Comparable

        // sortByKey方法要求数据中的K必须可以进行比较，实现Comparable接口
        JavaPairRDD<User, Integer> sortRDD = mapRDD.sortByKey();
        sortRDD.collect().forEach(System.out::println);
        jsc.close();

    }
}
class User implements Serializable, Comparable<User> {
    public int age = 0;
    public int amount = 0;

    @Override
    // 方法返回值为整型数据，表示数据比较结果（状态）
    // 如果为大于0的整数，那么表示当前对象比其他的对象大
    // 如果为小于0的整数，那么表示当前对象比其他的对象小
    // 如果为等于0的整数，那么表示当前对象和其他的对象一样大
    public int compareTo(User other) {
//        if (this.age < other.age) {
//            return -1;
//        } else if (this.age > other.age) {
//            return 1;
//        } else {
//            return 0;
//        }
//        return this.age - other.age;
        if (this.age == other.age) {
            return this.amount - other.amount;
        } else {
            return other.age - this.age;
        }
    }

    @Override
    public String toString() {
        return "User{" +
                "age=" + age +
                ", amount=" + amount +
                '}';
    }
}