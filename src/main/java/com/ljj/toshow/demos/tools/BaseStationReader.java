package com.ljj.toshow.demos.tools;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * 基站数据读取工具类
 */
public class BaseStationReader {
    
    /**
     * 基站信息类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BaseStation {
        private int stationId;           // 基站ID（第1列，索引0）
        private double longitude;        // 经度（第6列，索引5）
        private double latitude;         // 纬度（第7列，索引6）
        private double coverageRadius;   // 监测范围（米）
    }
    
    /**
     * 从Excel文件读取基站数据
     * @param filePath Excel文件路径（相对于classpath）
     * @return 基站列表
     */
    public static List<BaseStation> loadBaseStations(String filePath) throws IOException {
        List<BaseStation> stations = new ArrayList<>();
        
        // 从classpath读取文件
        ClassPathResource resource = new ClassPathResource(filePath);
        try (InputStream inputStream = resource.getInputStream();
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            
            Sheet sheet = workbook.getSheetAt(0);
            
            // 从第2行开始读取（跳过表头，索引从1开始）
            for (int rowNum = 1; rowNum <= sheet.getLastRowNum(); rowNum++) {
                Row row = sheet.getRow(rowNum);
                if (row == null) continue;
                
                // 检查是否有足够的列（至少需要7列）
                if (row.getLastCellNum() < 7) continue;
                
                try {
                    // 第1列（索引0）是基站ID
                    int stationId = (int) row.getCell(0).getNumericCellValue();
                    
                    // 第6列（索引5）是经度，第7列（索引6）是纬度
                    // 直接使用 getNumericCellValue()，与项目中其他代码保持一致
                    double longitude = row.getCell(5).getNumericCellValue();
                    double latitude = row.getCell(6).getNumericCellValue();
                    
                    // 所有基站的监测范围都是100米
                    double coverageRadius = 100.0;
                    
                    BaseStation station = new BaseStation(stationId, longitude, latitude, coverageRadius);
                    stations.add(station);
                    
                } catch (Exception e) {
                    System.err.println("读取第 " + rowNum + " 行基站数据时出错: " + e.getMessage());
                }
            }
        }
        
        System.out.println("成功加载 " + stations.size() + " 个基站");
        return stations;
    }
}

