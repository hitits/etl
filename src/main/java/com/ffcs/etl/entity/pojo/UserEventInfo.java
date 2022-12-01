package com.ffcs.etl.entity.pojo;

public class UserEventInfo {
    private String userID;
    private String userName;
    private Integer userAge;
    private String eventTime;
    private String eventType;
    private Integer productID;
    private String partitionDay;

    public UserEventInfo() {
    }

    public UserEventInfo(String userID, String userName, Integer userAge, String eventTime, String eventType, Integer productID, String partitionDay) {
        this.userID = userID;
        this.userName = userName;
        this.userAge = userAge;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.productID = productID;
        this.partitionDay = partitionDay;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getUserAge() {
        return userAge;
    }

    public void setUserAge(Integer userAge) {
        this.userAge = userAge;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Integer getProductID() {
        return productID;
    }

    public void setProductID(Integer productID) {
        this.productID = productID;
    }

    public String getPartitionDay() {
        return partitionDay;
    }

    public void setPartitionDay(String partitionDay) {
        this.partitionDay = partitionDay;
    }
}
