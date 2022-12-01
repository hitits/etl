package com.ffcs.etl.entity.pojo.json;

import com.ffcs.etl.entity.enumerate.FiledTypeEnum;

import java.util.UUID;

/**
 * @className: Pulsar2HudiMapping
 * @author: linzy
 * @date: 2022/11/22
 **/
public class Pulsar2HudiMapping {

    private String id;

    /**
     * 格式类别[type1400,type1240]
     */
    private String formatType;

    /***
     *  值
     **/
    private String value;
    /**
     * 服务类别 [1.接入  99.其它]
     */
    private String serviceType;
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

    public Pulsar2HudiMapping() {

    }

    public Pulsar2HudiMapping(String formatType, String serviceType, String targetField, FiledTypeEnum targetType, String sourceField, FiledTypeEnum sourceType, String value) {
        this.formatType = formatType;
        this.serviceType = serviceType;
        this.targetField = targetField;
        this.targetType = targetType.name();
        this.sourceField = sourceField;
        this.sourceType = sourceType.name();
        this.id = UUID.randomUUID().toString();
        this.value = value;
        this.isDisabled = "0";
        this.isRequired = "0";

    }

    public Pulsar2HudiMapping(String formatType, String serviceType, String targetField, FiledTypeEnum targetType, String sourceField, FiledTypeEnum sourceType) {
        this.formatType = formatType;
        this.serviceType = serviceType;
        this.targetField = targetField;
        this.targetType = targetType.name();
        this.sourceField = sourceField;
        this.sourceType = sourceType.name();
        this.id = UUID.randomUUID().toString();
        this.isDisabled = "0";
        this.isRequired = "0";

    }

    public Pulsar2HudiMapping(String id, String formatType, String serviceType, String targetField, FiledTypeEnum targetType, String sourceField, FiledTypeEnum sourceType, String isDisabled) {
        this.id = id;
        this.formatType = formatType;
        this.serviceType = serviceType;
        this.targetField = targetField;
        this.targetType = targetType.name();
        this.sourceField = sourceField;
        this.sourceType = sourceType.name();
        this.isDisabled = isDisabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFormatType() {
        return formatType;
    }

    public void setFormatType(String formatType) {
        this.formatType = formatType;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getIsRequired() {
        return isRequired;
    }

    public void setIsRequired(String isRequired) {
        this.isRequired = isRequired;
    }
}
