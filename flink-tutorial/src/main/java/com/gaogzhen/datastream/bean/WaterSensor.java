package com.gaogzhen.datastream.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 水位传感器
 * @author: Administrator
 * @createTime: 2023/12/03 15:18
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    /**
     * id
     */
    public String id;

    /**
     * 传感器记录时间戳
     */
    public Long ts;

    /**
     * 水位值
     */
    public Integer vc;
}
