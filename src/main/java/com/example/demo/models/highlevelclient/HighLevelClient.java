package com.example.demo.models.highlevelclient;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportRequestOptions;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by hadoop on 2018/8/22.
 */
public class HighLevelClient {
    private RestHighLevelClient client;

    public RestHighLevelClient getClient() {
        return client;
    }

    public static class Builder{
        private String hostName = null;
        private RestHighLevelClient client= null;

        public Builder hostName(String hostName){
            this.hostName = hostName;
            return this;
        }

        public Builder client(){
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(this.hostName, 9200, "http")));
            this.client = client;
            return this;
        }

        public HighLevelClient build(){
            return new HighLevelClient().build(this);
        }

    }

    private HighLevelClient build(Builder builder){
        this.client = builder.client;
        return this;
    }

    /**
     *
     * @throws java.io.IOException
     */
    public void CreateRolloverIndexAndMapping() throws java.io.IOException{
        RolloverRequest request = new RolloverRequest("alias", "index-2");
        request.addMaxIndexAgeCondition(new TimeValue(7, TimeUnit.MINUTES));
        request.addMaxIndexDocsCondition(1000);
        request.addMaxIndexSizeCondition(new ByteSizeValue(50, ByteSizeUnit.MB));
        request.dryRun(true);
        //暂不支持6.2.4及以下版本
        request.getCreateIndexRequest().settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        request.getCreateIndexRequest().mapping("type", "field", "type=keyword");
        request.getCreateIndexRequest().alias(new Alias("another_alias"));
        RolloverResponse rolloverResponse = client.indices().rollover(request);
        boolean acknowledged = rolloverResponse.isAcknowledged();
        boolean shardsAcked = rolloverResponse.isShardsAcknowledged();
        String oldIndex = rolloverResponse.getOldIndex();
        String newIndex = rolloverResponse.getNewIndex();
        boolean isRolledOver = rolloverResponse.isRolledOver();
        boolean isDryRun = rolloverResponse.isDryRun();
        Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
        System.out.println(acknowledged);
    }

}
