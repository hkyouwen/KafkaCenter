package org.nesc.ec.bigdata.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.nesc.ec.bigdata.common.constant.Constant;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author Truman.P.Du
 * @version 1.0
 * @date 2019年4月17日 下午2:57:37
 */
public class ElasticsearchUtil implements Closeable {
    private RestHighLevelClient client = null;


    public ElasticsearchUtil(String hosts) {
        this(hosts, "", "");
    }

    public ElasticsearchUtil(String hosts, String user, String password) {
        if (!"".equalsIgnoreCase(hosts) && null != hosts) {
            String[] addressArray = hosts.split(",", -1);
            HttpHost[] httpHosts = new HttpHost[addressArray.length];

            for (int i = 0; i < addressArray.length; i++) {
                String[] hostAndPort = addressArray[i].split(":", -1);
                httpHosts[i] = new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1]), "http");
            }
            if (StringUtils.isNotBlank(user) && StringUtils.isNotBlank(password)) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
                client = new RestHighLevelClient(RestClient.builder(httpHosts)
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                httpClientBuilder.disableAuthCaching();
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }));
            } else {
                client = new RestHighLevelClient(RestClient.builder(httpHosts));
            }
        }
    }

    public boolean batchInsertES(List<JSONObject> datas, String index) throws IOException {
        if (client == null) {
            return false;
        }
        BulkRequest request = new BulkRequest();
        if (datas == null || datas.isEmpty()) {
            return true;
        }
        datas.forEach(data -> {
            request.add(new IndexRequest(index).source(data));
        });

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);

        return !bulkResponse.hasFailures();

    }

    public JSONObject searchES(String requestBody, String index) throws IOException {
        if (client == null) {
            return new JSONObject();
        }
        RestClient lowLevelClient = client.getLowLevelClient();
        String endpoint = "/" + index + Constant.Search.SEARCH_;
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(new NStringEntity(requestBody, ContentType.APPLICATION_JSON));
        Response response = lowLevelClient.performRequest(request);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            long len = entity.getContentLength();
            if (len != -1 && len < 2048) {
                return JSON.parseObject(EntityUtils.toString(entity));
            } else {
                return JSON.parseObject(entity.getContent(), JSONObject.class);
            }
        }
        return null;
    }

    public JSONObject scrollSearch(String requestBody, String index) throws Exception {
        if (client == null) {
            return new JSONObject();
        }
        RestClient lowLevelClient = client.getLowLevelClient();
        JSONObject json = new JSONObject();
        JSONArray arrays = new JSONArray();
        JSONObject aggs = null;
        String endpoint = "/" + index + Constant.Search.SEARCH_SCROLL_1M;
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(new NStringEntity(requestBody, ContentType.APPLICATION_JSON));
        Response response = lowLevelClient.performRequest(request);
        HttpEntity entity = response.getEntity();
        JSONObject searchResult = null;
        while (true) {
            searchResult = JSON.parseObject(EntityUtils.toString(entity));
            final JSONArray searchHits = searchResult.getJSONObject(Constant.Elasticsearch.HITS).getJSONArray(Constant.Elasticsearch.HITS);
            if (searchResult.containsKey(Constant.Elasticsearch.AGGREGATIONS)) {
                aggs = searchResult.getJSONObject(Constant.Elasticsearch.AGGREGATIONS);
            }
            if (searchHits.size() == 0) {
                break;
            }
            arrays.add(searchResult.getJSONObject(Constant.Elasticsearch.HITS).getJSONArray(Constant.Elasticsearch.HITS));
            request = new Request(HttpPost.METHOD_NAME, Constant.Search.SEARCH_SCROLL);
            request.setEntity(new NStringEntity(getScrollNextBody(searchResult.getString(Constant.Elasticsearch.SCROLL_ID)), ContentType.APPLICATION_JSON));
            response = lowLevelClient.performRequest(request);
            entity = response.getEntity();
        }
        json.put(Constant.Elasticsearch.HITS, arrays);
        json.put(Constant.Elasticsearch.AGGREGATIONS, aggs);
        return json;
    }

    private static String getScrollNextBody(String scrollId) {
        return "{\"scroll\":\"1m\",\"scroll_id\":\"" + scrollId + "\"}";
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

}
