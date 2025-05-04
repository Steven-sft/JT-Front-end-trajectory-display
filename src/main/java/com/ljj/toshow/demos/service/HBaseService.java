package com.ljj.toshow.demos.service;

import com.ljj.toshow.demos.pojo.TimeSpatialResult;
import com.ljj.toshow.demos.pojo.poiS;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.List;

public interface HBaseService {
       List<poiS> getByTimeSpatial() throws IOException;

}