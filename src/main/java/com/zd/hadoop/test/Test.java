package com.zd.hadoop.test;

import java.util.TreeMap;

/**
 * Copyright (C) zhongda
 *
 * @author zx
 * @date 2020/9/2 0002 09:42
 * @description:
 */

public class Test {
    public static void main(String[] args) {
        System.out.println(12%1);

        TreeMap<Integer,String> map = new TreeMap<>();

        map.put(12,"十二");
        map.put(1,"一");
        map.put(4,"四");

        System.out.println(map.get(map.lastKey()));
    }
}
