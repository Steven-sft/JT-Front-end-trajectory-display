package com.ljj.toshow.demos.pojo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.io.Serializable;
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class poiS implements Serializable {
    private double longitude;
    private double latitude;
    private String name = "";
    private String stakeId="";
    private Long id;
    private int direction;
    private String timeStampStr;
    private Integer source; // 数据来源
    private Integer specialFlag; // 特殊标识：99表示预测点
}
