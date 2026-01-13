package com.ljj.toshow.demos;

import com.ljj.toshow.demos.pojo.TrafficEventUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;



public class test {
    public static void main(String[] args) throws IOException {
//        TrafficEventUtils.MileageConverter mileageConverter1=new TrafficEventUtils.MileageConverter("D:\\learn\\codes\\a_idea_codes\\toShow\\src\\main\\java\\com\\ljj\\toshow\\demos\\pojo\\data\\sx_json.json");
//        double[] d=mileageConverter1.findCoordinate(stakeToMileage("K1175+133")).getLnglat();
//        System.out.println(Arrays.toString(d));
        Map<Integer,Integer>d=new HashMap<>();
        d.put(1,2);
        int a=d.get(1);
        a=3;
        System.out.println(d.get(1));
//        LinkedList<Integer>as=new LinkedList<>();
//        LinkedList<Integer>bs=as;
//        bs.add(123);
//        System.out.println(as);
    }
}
