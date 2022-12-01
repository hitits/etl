package com.ffcs.etl.entity.pojo.hudi;

import java.sql.Timestamp;

/**
 * @className: User_pulsar
 * @author: linzy
 * @date: 2022/11/21
 *
 *
 **/

public class UserHudi {

    private String userId;
    private String name;
    private String createTime;
    private String address;
    private Integer age;
    private Integer sex;
    private Timestamp ts;
    private byte[] image;
    private String partitionDay;

    public byte[] getImage() {
        return image;
    }

    public void setImage(byte[] image) {
        this.image = image;
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

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public String getPartitionDay() {
        return partitionDay;
    }

    public void setPartitionDay(String partitionDay) {
        this.partitionDay = partitionDay;
    }

    @Override
    public String toString() {
        return "UserHudi{" +
                "userId='" + userId + '\'' +
                ", name='" + name + '\'' +
                ", createTime='" + createTime + '\'' +
                ", address='" + address + '\'' +
                ", age=" + age +
                ", sex=" + sex +
                ", ts=" + ts +
                ", image='" + image + '\'' +
                ", partitionDay='" + partitionDay + '\'' +
                '}';
    }
}
