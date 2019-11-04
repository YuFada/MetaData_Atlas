package com.dc.cloud.json.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.springframework.util.StringUtils;

@Data
@TableName("hive_")
public class HiveLineageTable extends HiveDynamicTable {

    @TableId(type = IdType.AUTO)
    private String id;

    private String guid;

    private String name;

    private String qualifiedName;

    private String baseEntityGuid;

    private String fromEntityId;

    private String toEntityId;



    public static boolean checkEntityId(HiveLineageTable lineageTable) {
        return StringUtils.hasText(lineageTable.fromEntityId) && StringUtils.hasText(lineageTable.toEntityId);
    }

    private HiveLineageTable(String id, String guid, String name, String qualifiedName, String baseEntityGuid, String fromEntityId, String toEntityId,String tableName) {
        this.id = id;
        this.guid = guid;
        this.name = name;
        this.qualifiedName = qualifiedName;
        this.baseEntityGuid = baseEntityGuid;
        this.fromEntityId = fromEntityId;
        this.toEntityId = toEntityId;
        this.setTableName(tableName);
    }

    public static HiveLineageTable.HiveLineageTableBuilder builder() {
        return new HiveLineageTable.HiveLineageTableBuilder();
    }


    public String toString() {
        return "HiveLineageTable(id=" + this.getId() + ", guid=" + this.getGuid() + ", name=" + this.getName() + ", qualifiedName=" + this.getQualifiedName() + ", baseEntityGuid=" + this.getBaseEntityGuid() + ", fromEntityId=" + this.getFromEntityId() + ", toEntityId=" + this.getToEntityId() + ", tableName=" + this.getTableName() + ")";
    }


    public static class HiveLineageTableBuilder {
        private String id;
        private String guid;
        private String name;
        private String qualifiedName;
        private String baseEntityGuid;
        private String fromEntityId;
        private String toEntityId;
        private String tableName;

        HiveLineageTableBuilder() {
        }

        public HiveLineageTable.HiveLineageTableBuilder id(String id) {
            this.id = id;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder guid(String guid) {
            this.guid = guid;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder name(String name) {
            this.name = name;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder qualifiedName(String qualifiedName) {
            this.qualifiedName = qualifiedName;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder baseEntityGuid(String baseEntityGuid) {
            this.baseEntityGuid = baseEntityGuid;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder fromEntityId(String fromEntityId) {
            this.fromEntityId = fromEntityId;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder toEntityId(String toEntityId) {
            this.toEntityId = toEntityId;
            return this;
        }

        public HiveLineageTable.HiveLineageTableBuilder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }


        public HiveLineageTable build() {
            return new HiveLineageTable(this.id, this.guid, this.name, this.qualifiedName, this.baseEntityGuid, this.fromEntityId, this.toEntityId,this.tableName);
        }

        @Override
        public String toString() {
            return "HiveLineageTableBuilder{" +
                    "id='" + id + '\'' +
                    ", guid='" + guid + '\'' +
                    ", name='" + name + '\'' +
                    ", qualifiedName='" + qualifiedName + '\'' +
                    ", baseEntityGuid='" + baseEntityGuid + '\'' +
                    ", fromEntityId='" + fromEntityId + '\'' +
                    ", toEntityId='" + toEntityId + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }
    }


}
