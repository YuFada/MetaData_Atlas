package com.dc.cloud.json.bean;

import com.baomidou.mybatisplus.annotation.TableField;
import lombok.Data;

import java.io.Serializable;

//动态表名
@Data
public class HiveDynamicTable implements Serializable {

    //动态数据库表名称
    @TableField(exist = false)
    private String tableName;

    @SuppressWarnings("unchecked")
    public <T extends HiveDynamicTable> T tableName(String tableName){
        this.tableName = tableName;
        return (T) this;
    }


}
