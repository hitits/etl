package com.ffcs.etl.entity.pojo.pulsar;

/**
 * @className: User_pulsar
 * @author: linzy
 * @date: 2022/11/21
 *
 * hudi与pulsar字段映射
 *  **/

public class UserPulsarType2{
   // insert into User_Pulsar_Hudi values(2,"json_type2","uId","uName","createTime","uAddr","uAge","uSex","uImage","");
    //来源类别
    private String sourceCategory;
    private String uId;
    private String uName;
    private String createTime;
    private String uAddr;
    private Integer uAge;
    private Integer uSex;
    private String uImage;

    public String getSourceCategory() {
        return sourceCategory;
    }

    public void setSourceCategory(String sourceCategory) {
        this.sourceCategory = sourceCategory;
    }

    public String getuId() {
        return uId;
    }

    public void setuId(String uId) {
        this.uId = uId;
    }

    public String getuName() {
        return uName;
    }

    public void setuName(String uName) {
        this.uName = uName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getuAddr() {
        return uAddr;
    }

    public void setuAddr(String uAddr) {
        this.uAddr = uAddr;
    }

    public Integer getuAge() {
        return uAge;
    }

    public void setuAge(Integer uAge) {
        this.uAge = uAge;
    }

    public Integer getuSex() {
        return uSex;
    }

    public void setuSex(Integer uSex) {
        this.uSex = uSex;
    }

    public String getuImage() {
        return uImage;
    }

    public void setuImage(String uImage) {
        this.uImage = uImage;
    }
}
