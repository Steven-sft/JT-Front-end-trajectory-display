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
    private int direction;
}
