package com.dc.cloud.json.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;


/**
 * lineage_details_db
 * lineage_details_table
 */

@Data
@Builder
@TableName("lineage_%s_detail")
public class HiveLineageDetail extends HiveDynamicTable{

    @TableId(type = IdType.AUTO)
    private String id;

    private String name;

    private String guid;

    private String tbGuid;

    private String dbGuid;

    private String des;

    private String location;

    private String  owner;

    private String createBy;

    private String updateBy;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private String encoding;

    private  String  comment; //对库、表、列的描述

    private String type; // 描述列的数据类型


}
