package com.ljj.toshow.demos.pojo;

import lombok.*;

import java.io.Serializable;
import java.util.List;

public class PTData {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class PathTData implements Serializable {
        private int pathNum;
        private long time;
        private String timeStamp;
        //        private String waySectionId;
//        private String waySectionName;
        private List<PathPoint> pathList;
    }

    /**
     * PathPoint 为 PathData中 pathList 存储的点的简化版本
     * 注意：time是多余的，原本只要求timestamp
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class PathPoint implements Serializable{
        private int direction;
        private long id;
        private int laneNo;
        private int mileage;
        private String plateNo = "";
        private float speed;
        private String timeStamp;
        private Integer plateColor = null;
        private Integer vehicleType = null;
        private double longitude;
        private double latitude;
        private double carAngle;
        // 现在没有桩号
        private String stakeId = "";
        private Integer originalType = null;
        private Integer originalColor = null;
        private String specialFlag = "";
    }
}
