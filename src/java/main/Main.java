package main;

import java.util.*;

/**
 * Created by liukun on 18/6/14.
 */
public class Main {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(5);
        boolean excepted = list.remove(new Integer(5));
        System.out.println(excepted);
    }
}
