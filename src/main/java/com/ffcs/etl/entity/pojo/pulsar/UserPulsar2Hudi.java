package com.ffcs.etl.entity.pojo.pulsar;

import java.sql.Timestamp;

/**
 * @className: User_pulsar
 * @author: linzy
 * @date: 2022/11/21
 *
 * hudi与pulsar字段映射
 *  **/
/**
 * 例:
 * (1)
 * hudi      pulsar
 * userId     uid
 * name       uname
 * ... ...
 *  (2)
 *  hudi      pulsar
 *  userId     userId
 *  name       username
 *  ... ...
 **/
public class UserPulsar2Hudi {
    //来源类别
    private String sourceCategory;
    private String userId;
    private String name;
    private String createTime;
    private String address;
    private String age;
    private String sex;
    private String image;
    private String partitionDay;

    public UserPulsar2Hudi() {
    }

    public UserPulsar2Hudi(String sourceCategory, String userId, String name, String createTime, String address, String age, String sex, String image, String partitionDay) {
        this.sourceCategory = sourceCategory;
        this.userId = userId;
        this.name = name;
        this.createTime = createTime;
        this.address = address;
        this.age = age;
        this.sex = sex;
        this.image = image;
        this.partitionDay = partitionDay;
    }

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

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getPartitionDay() {
        return partitionDay;
    }

    public void setPartitionDay(String partitionDay) {
        this.partitionDay = partitionDay;
    }

    @Override
    public String toString() {
        return "UserPulsar2Hudi{" +
                "sourceCategory='" + sourceCategory + '\'' +
                ", userId='" + userId + '\'' +
                ", name='" + name + '\'' +
                ", createTime='" + createTime + '\'' +
                ", address='" + address + '\'' +
                ", age='" + age + '\'' +
                ", sex='" + sex + '\'' +
                ", image='" + image + '\'' +
                ", partitionDay='" + partitionDay + '\'' +
                '}';
    }
}
