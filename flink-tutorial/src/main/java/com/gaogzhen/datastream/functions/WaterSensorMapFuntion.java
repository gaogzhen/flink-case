package com.gaogzhen.datastream.functions;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author gaogzhen
 * @since 2023/12/4 20:48
 */
public class WaterSensorMapFuntion implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String s) throws Exception {
        String[] data = s.split(",");
        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
    }
}
