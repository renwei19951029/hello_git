package com.project.spark.date.UDAF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class CustomUDF extends UDF {

    public String evaluate(String text) {
        return text + "哈哈哈";
    }
}
