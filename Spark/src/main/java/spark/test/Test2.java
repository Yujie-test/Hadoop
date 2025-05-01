package spark.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Test2 {
    public static void main(String[] args) {
        // TODO 在数据处理过程中，一般不会改变原始数据
        List<Integer> nums = Arrays.asList(1, 2, 3, 4);
//        for (int i = 0; i < nums.size(); i++) {
//            //nums.add(i, nums.get(i) * 2);
//            nums.set(i, nums.get(i) * 2);
//        }
//        System.out.println(nums);
        List<Integer> newNums = new ArrayList<>();
        for (int i = 0; i < nums.size(); i++) {
            //nums.add(i, nums.get(i) * 2);
            newNums.add(i, nums.get(i) * 2);
        }
        System.out.println(newNums);

    }
}
