package spark.test;

import java.util.Arrays;
import java.util.List;

public class Test3 {
    public static void main(String[] args) {
        List<String> strings = Arrays.asList("zhangsan", "lisi", "wangwu");
        for (String name : strings) {
            System.out.println(test(name));
        }
    }
    private static String test(String name) {

        return name.substring(0, 1).toUpperCase() + name.substring(1);
    }
}
