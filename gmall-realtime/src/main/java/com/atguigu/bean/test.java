package com.atguigu.bean;

import java.util.ArrayList;
import java.util.HashSet;

public class test<T> {

    void func(){
        System.out.println("hhhhh");
    }
    static void func2(){
        System.out.println("Xxxxxx");
    }
    public static void main(String[] args) {

        test<String> stringtest = new test<String>(){};
        test<String> stringtest2 = new test<String>();
        System.out.println(stringtest);
        System.out.println(stringtest2);
        stringtest.func();

    }
}
