package spark.test;

public class Test {
    public static void main(String[] args) {
        String s = " a b "; // char[] value = [' ', 'a', ' ', 'b', ' ']
        // trim去掉字符串首尾半角空格
        String s1 = s.trim(); // char[] value = ['a', ' ', 'b']
        String s2 = s.substring(0, 2);
        String s3 = s.replaceAll("a", "c");
        System.out.println("!"+s+"!");
        System.out.println("!"+s1+"!");
        System.out.println(s2);
        System.out.println(s3);

        // TODO String不可变字符串
    }
}
