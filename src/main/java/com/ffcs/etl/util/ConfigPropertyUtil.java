package com.ffcs.etl.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author linzy
 * @version 1.0
 * @Company:
 * @description: TODO
 * @date 2021/12/9 17:27
 */
public class ConfigPropertyUtil {

    private static final Logger logger = LoggerFactory.getLogger( ConfigPropertyUtil.class);
    private static Map<String, ConfigPropertyUtil> configPropertyMap = new HashMap<String, ConfigPropertyUtil>();
    private Properties properties = null;

    public static  ConfigPropertyUtil getInstance(String propertyFileName){
        if(!configPropertyMap.containsKey(propertyFileName)){
            synchronized ( ConfigPropertyUtil.class){
                if(!configPropertyMap.containsKey(propertyFileName)){
                     ConfigPropertyUtil configPropertyUtil = new  ConfigPropertyUtil(propertyFileName);
                    configPropertyMap.put(propertyFileName, configPropertyUtil);
                }
            }
        }
        return configPropertyMap.get(propertyFileName);
    }

    private ConfigPropertyUtil(String propertyFileName){
        init(propertyFileName);
    }

    private void init(String propertyFileName){
        properties = new Properties();
        InputStream inputStream =  ConfigPropertyUtil.class.getClassLoader().getResourceAsStream(propertyFileName);
        if(inputStream != null) {
            try {
                properties.load(new InputStreamReader(inputStream,"UTF-8"));
            } catch (IOException e) {
                logger.error(String.format("config.properties file not found"));
                e.printStackTrace();
            }
        }
    }

    public String getPropertyVal(String key){
        return this.properties.getProperty(key);
    }

    public Integer getIntPropertyVal(String key){
        return Integer.parseInt(this.getPropertyVal(key));
    }

    public Properties getProperties() {
        return properties;
    }
}
