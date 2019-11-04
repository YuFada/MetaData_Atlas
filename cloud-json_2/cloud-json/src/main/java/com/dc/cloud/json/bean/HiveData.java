package com.dc.cloud.json.bean;

import com.baomidou.mybatisplus.annotation.*;

import com.dc.cloud.json.support.json.HiveDataJsonHandler;
import lombok.Data;
import reactor.util.function.Tuple3;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@TableName("lineage_")
public class HiveData extends HiveDynamicTable implements Serializable {

    //当数据库不存在这个字段时
    @TableField(exist = false)
    public static HiveData NULL_INSTANCE = new HiveData();

    @TableField(exist = false)
    private Tuple3<String,String,Class<? extends HiveDataJsonHandler>> requestJsonTuple;

    @TableId(type = IdType.AUTO)
    private String id;

    private String guid;

    private String encoding;

    private String des;

    private String owner;

    private String updater;

    //属性名称和数据库字段不对应的时候
    @TableField("createTime")
    private LocalDateTime createTime;

    //createtime  //create_time

    @TableField("updateTime")
    private LocalDateTime updateTime;

    private String origion;

    private String name;

    private HiveData() {
    }

    public String toString() {
        return "HiveData(id=" + this.getId() + ", guid=" + this.getGuid() + ", encoding=" + this.getEncoding() + ", des=" + this.getDes() + ", ownerFiledName=" + this.getOwner() + ", updater=" + this.getUpdater() + ", createTime=" + this.getCreateTime() + ", updateTime=" + this.getUpdateTime() + ", origion=" + this.getOrigion() + ", name=" + this.getName() + ", tableName=" + this.getTableName() + ")";
    }

    public static HiveData.HiveDataBuilder builder() {
        return new HiveData.HiveDataBuilder();
    }


    private HiveData(String id, String guid, String encoding, String des, String owner, String updater, LocalDateTime createTime, LocalDateTime updateTime, String origion, String name, String tableName,Tuple3<String,String,Class<? extends HiveDataJsonHandler>> requestJsonTuple) {
        this.id = id;
        this.guid = guid;
        this.encoding = encoding;
        this.des = des;
        this.owner = owner;
        this.updater = updater;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.origion = origion;
        this.name = name;
        this.setTableName(tableName);
        this.requestJsonTuple = requestJsonTuple;
    }

    public static class HiveDataBuilder {
        private String id;
        private String guid;
        private String encoding;
        private String des;
        private String owner;
        private String updater;
        private LocalDateTime createTime;
        private LocalDateTime updateTime;
        private String origion;
        private String name;
        private String tableName;
        private Tuple3<String,String,Class<? extends HiveDataJsonHandler>> requestJsonTuple;


        HiveDataBuilder() {
        }

        public HiveData.HiveDataBuilder id(String id) {
            this.id = id;
            return this;
        }

        public HiveData.HiveDataBuilder guid(String guid) {
            this.guid = guid;
            return this;
        }

        public HiveData.HiveDataBuilder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public HiveData.HiveDataBuilder des(String des) {
            this.des = des;
            return this;
        }

        public HiveData.HiveDataBuilder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public HiveData.HiveDataBuilder updater(String updater) {
            this.updater = updater;
            return this;
        }

        public HiveData.HiveDataBuilder createTime(LocalDateTime createTime) {
            this.createTime = createTime;
            return this;
        }

        public HiveData.HiveDataBuilder updateTime(LocalDateTime updateTime) {
            this.updateTime = updateTime;
            return this;
        }

        public HiveData.HiveDataBuilder origion(String origion) {
            this.origion = origion;
            return this;
        }

        public HiveData.HiveDataBuilder name(String name) {
            this.name = name;
            return this;
        }

        public HiveData.HiveDataBuilder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public HiveData.HiveDataBuilder requestJsonTuple(Tuple3<String,String,Class<? extends HiveDataJsonHandler>> requestJsonTuple) {
            this.requestJsonTuple = requestJsonTuple;
            return this;
        }


        public HiveData build() {
            return new HiveData(this.id, this.guid, this.encoding, this.des, this.owner, this.updater, this.createTime, this.updateTime, this.origion, this.name, this.tableName,requestJsonTuple);
        }

        @Override
        public String toString() {
            return "HiveDataBuilder{" +
                    "id='" + id + '\'' +
                    ", guid='" + guid + '\'' +
                    ", encoding='" + encoding + '\'' +
                    ", des='" + des + '\'' +
                    ", ownerFiledName='" + owner + '\'' +
                    ", updater='" + updater + '\'' +
                    ", createTime=" + createTime +
                    ", updateTime=" + updateTime +
                    ", origion='" + origion + '\'' +
                    ", name='" + name + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }
    }


}
