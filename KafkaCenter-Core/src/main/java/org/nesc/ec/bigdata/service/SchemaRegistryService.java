package org.nesc.ec.bigdata.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class SchemaRegistryService {

    @Autowired
    RestTemplate restTemplate;

    @Value("${schema.registry.url:''}")
    String schemaRegistryUrl;

    private static final String CONTENT_TYPE_SCHEMA = "application/vnd.schemaregistry.v1+json";

    public JSONObject getSchema(String subject){
        String uri = "/subjects/" + subject + "/versions/latest";
        JSONObject responseBody = restTemplate.getForEntity(schemaRegistryUrl + uri, JSONObject.class).getBody();
        return responseBody;
    }

    public JSONObject createSchema(String subject, String schema){
        String uri = "/subjects/" + subject + "/versions";
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, CONTENT_TYPE_SCHEMA);
        JSONObject jsonObject  = new JSONObject();
        jsonObject.put("schema", schema);
        HttpEntity<JSONObject> httpEntity = new HttpEntity<>(jsonObject, headers);
        String responseBody = restTemplate.postForEntity(schemaRegistryUrl + uri, httpEntity ,String.class).getBody();
        return JSONObject.parseObject(responseBody);
    }

    public JSONObject getSchemaVersion(String subject){
        String uri = "/subjects/" + subject + "/versions/";
        JSONArray responseBody = restTemplate.getForEntity(schemaRegistryUrl + uri, JSONArray.class).getBody();
        JSONObject jsonObject  = new JSONObject();
        jsonObject.put("versions", responseBody);
        return jsonObject;
    }
}
