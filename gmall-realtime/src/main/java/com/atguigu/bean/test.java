package com.atguigu.bean;

import java.util.ArrayList;
import java.util.HashSet;

public class test {
    public static void main(String[] args) {
        System.out.println(String.class);

        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(1);
        integers.add(1);
        integers.add(2);
        HashSet<Integer> set = new HashSet<>();
        set.addAll(integers);
        System.out.println(set.size());
    }
}
