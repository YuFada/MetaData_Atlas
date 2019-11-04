package com.dc.cloud.json.bean;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
@TableName("hive_demo")
public class HiveDemo extends HiveDynamicTable implements Serializable {

    //table1
    private String name;

    //table2
    private String age;

}
