package Test1;

import org.junit.Test;

public class StrTest {
    @Test
    public void test(){
        String s = "[]";
        System.out.println(s.indexOf(","));
        String s1 = s.substring(0, 1);
        System.out.println(s1);
    }
    @Test
    public void test1(){
        StringBuffer s = new StringBuffer("[{a:fdafdsa},{b:dfsfedsw},");
        s.delete(s.length()-2,s.length());
        System.out.println(s);
    }
    @Test
    public void test2(){
        Integer i =null;
        System.out.println(i.intValue());
    }
}
