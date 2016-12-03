package com.citibike.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class will bind and unbind objects to xml files.
 * 
 **
 * @param <T> is the object type to marshal/unmarshal
 */
public class JSONMarshaller<T> {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(JSONMarshaller.class);

    // JSON mapper to be used for this instance.
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * 
     * @param json
     * @param jsonClass
     * @return
     * @throws Exception
     */
    public T fromJSON(String json, Class<T> jsonClass) throws Exception {
        LOGGER.debug("Converting " + jsonClass.getName());
        //System.out.println("Deserialize: "+json);
        return mapper.readValue(json, jsonClass);
    }

    public String toJSON(Object object) throws Exception {
        LOGGER.debug("Creating " + object.getClass().getName());
        return mapper.writeValueAsString(object);

    }
}
