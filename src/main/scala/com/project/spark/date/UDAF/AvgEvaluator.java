package com.project.spark.date.UDAF;

import org.apache.hadoop.hive.ql.exec.UDAF;

public class AvgEvaluator extends UDAF {

    private double first;
    //create temporary function fun_sum ''
    /**
     * 初始化
     */
    public void init() {
        first = 0.0;
    }

    /**
     * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return
     */
    public boolean iterate(Double o) {
        if (o != null) {
            first = o + first;
        }
        return true;
    }

    /**
     * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据， * terminatePartial类似于hadoop的Combiner * * @return
     */
    public double terminatePartial() {
        return first == 0 ? null : first;
    }

    /**
     * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o * @return
     */
    public boolean merge(double result) {
        if (result != 0.0) {
            first = first + result;
        }
        return true;
    }

    /**
     * terminate返回最终的聚集函数结果 * * @return
     */
    public Double terminate() {
        return first;
    }
}
