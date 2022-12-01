package com.ffcs.etl.entity.pojo.pulsar;

/**
 * @className: User_pulsar
 * @author: linzy
 * @date: 2022/11/21
 *
 * hudi与pulsar字段映射
 *  **/

public class UserPulsarType1 {

    //insert into User_Pulsar_Hudi values(1,"json_type1","userId","name","createTime","address","age","sex","image","");
    //来源类别
    private String sourceCategory;
    private String userId;
    private String name;
    private String createTime;
    private String address;
    private Integer age;
    private Integer sex;
    private String image;


    public String getSourceCategory() {
        return sourceCategory;
    }

    public void setSourceCategory(String sourceCategory) {
        this.sourceCategory = sourceCategory;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }
}
