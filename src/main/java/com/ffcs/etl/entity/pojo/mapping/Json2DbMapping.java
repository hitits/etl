package com.ffcs.etl.entity.pojo.mapping;

import com.ffcs.etl.entity.enumerate.FiledTypeEnum;

import java.util.UUID;

/**
 * @className: Json2DbMapping
 * @author: linzy
 * @date: 2022/11/22
 **/
public class Json2DbMapping {

    private String id;

    private String targetTable;
    /***
     *  值
     **/
    private String value;
    /**
     * 目标字段
     * FaceObject.MustacheStyle.FaceStyle
     */
    private String targetField;
    /**
     * 目标字段类型 jsonObject,jsonArray,Integer,String,Double,Date...
     */
    private String targetType;
    /**
     * 来源字段
     */
    private String sourceField;
    /**
     * 来源字段类型 jsonObject,jsonArray,Integer,String,Double,Date...
     */
    private String sourceType;

    /**
     * 是否必须 0:非必须 1:必须 2:特定情况必须
     **/
    private String isDisabled;

    /**
     * 是否必须 0:非必须 1:必须 2:特定情况必须
     **/
    private String isRequired;

    public Json2DbMapping() {

    }

    public Json2DbMapping(String id, String targetTable, String value, String targetField, String targetType, String sourceField, String sourceType) {
        this.id = id;
        this.targetTable = targetTable;
        this.value = value;
        this.targetField = targetField;
        this.targetType = targetType;
        this.sourceField = sourceField;
        this.sourceType = sourceType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getTargetField() {
        return targetField;
    }

    public void setTargetField(String targetField) {
        this.targetField = targetField;
    }

    public String getTargetType() {
        return targetType;
    }

    public void setTargetType(String targetType) {
        this.targetType = targetType;
    }

    public String getSourceField() {
        return sourceField;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getIsDisabled() {
        return isDisabled;
    }

    public void setIsDisabled(String isDisabled) {
        this.isDisabled = isDisabled;
    }

    public String getIsRequired() {
        return isRequired;
    }

    public void setIsRequired(String isRequired) {
        this.isRequired = isRequired;
    }

    @Override
    public String toString() {
        return "Json2DbMapping{" +
                "id='" + id + '\'' +
                ", targetTable='" + targetTable + '\'' +
                ", value='" + value + '\'' +
                ", targetField='" + targetField + '\'' +
                ", targetType='" + targetType + '\'' +
                ", sourceField='" + sourceField + '\'' +
                ", sourceType='" + sourceType + '\'' +
                ", isDisabled='" + isDisabled + '\'' +
                ", isRequired='" + isRequired + '\'' +
                '}';
    }
}
