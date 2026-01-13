package com.ljj.toshow.demos.service.impl;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.ljj.toshow.demos.pojo.*;
import com.ljj.toshow.demos.service.HBaseService;
import com.ljj.toshow.demos.tools.JsonReader;
import com.ljj.toshow.demos.tools.hbaseTool;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;


@Service
public class HBaseServiceImpl implements HBaseService {



//因为转化的时候是用里程转化为经纬度，所以要区分方向
    @Override
    public List<poiS> getByTimeSpatial() throws IOException {
//
////        // 实现从resList获取数据的逻辑
        List<poiS> points = new ArrayList<>();
////            resMap.forEach((key, value) -> {
//////                System.out.println("value:" + value);
////                double[] d;
////                    System.out.println("sk:"+value.getSkateID()+"  mi:"+stakeToMileage(value.getSkateID())+"  get data:"+mileageConverter1.findCoordinate(stakeToMileage(value.getSkateID())));
////
////                if(value.getDirection() == 1) {
////                    d = mileageConverter1.findCoordinate(stakeToMileage(value.getSkateID())).getLnglat();
////                 System.out.println("entering  value:"+value+"  miconv:"+(mileageConverter1==null));
////
////                }else{
////                    d = mileageConverter2.findCoordinate(stakeToMileage(value.getSkateID())).getLnglat();
////                }
////                poiS point = new poiS();
////                point.setName(value.getName());
////                point.setLongitude(d[0]);
////                point.setLatitude(d[1]);
////                points.add(point);
////            });
////
////            resMap.clear();
//
//                    // 实现从resList获取数据的逻辑
//        List<poiS> points = new ArrayList<>();
//            resMap.forEach((key, value) -> {
////                System.out.println("value:" + value);
////        if(value.getDirection() == 1) {
//
//                poiS point = new poiS();
//                point.setName(value.getName());
//                point.setLongitude(value.getLongitude());
//                point.setLatitude(value.getLatitude());
//            points.add(point);
////        }
//            });
//
//            resMap.clear();
//
////             resMap.forEach((key, value) -> {
////                 System.out.println("entering  value:"+value+"  miconv:"+(mileageConverter1==null));
////                 if(value.getDirection()==1) {
////                poiS point = new poiS();
////                if(mileageConverter1!=null){
////                    System.out.println("sk:"+value.getSkateID()+"  mi:"+stakeToMileage(value.getSkateID())+"  get data:"+mileageConverter1.findCoordinate(stakeToMileage(value.getSkateID())));
////                    double[] d=mileageConverter1.findCoordinate(stakeToMileage(value.getSkateID())).getLnglat();
////                point.setName(value.getName());
////                point.setLongitude(d[0]);
////                point.setLatitude(d[1]);
////                point.setSkateID("1");
////                points.add(point);
////                }
////                 }
////                 if(value.getDirection()==0) {
////                        poiS point = new poiS();
////                        if(mileageConverter2!=null) {
////                            System.out.println("sk:"+value.getSkateID()+"  mi:"+stakeToMileage(value.getSkateID())+"  get data:"+mileageConverter2.findCoordinate(stakeToMileage(value.getSkateID())));
////                            double[] d = mileageConverter2.findCoordinate(stakeToMileage(value.getSkateID())).getLnglat();
////                            point.setName(value.getName());
////                            point.setLongitude(d[0]);
////                            point.setLatitude(d[1]);
////                            point.setSkateID("1");
////                            points.add(point);
////                        }
////                         }
////
////
////            });
////            points.add(new poiS(30,114,"13","123",1));
//
////        System.out.println("points: "+points);
//        resMap.clear();
//
        return points;
    }

    private int stakeToMileage(String stakeId) {
            return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
        }
}
