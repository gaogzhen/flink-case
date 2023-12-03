package com.gaogzhen.datastream.functions;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 20:22
 */
public class FilterFunctionImpl implements FilterFunction<WaterSensor> {

    public String id;

    public FilterFunctionImpl(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return id.equals(value.getId());
    }
}
