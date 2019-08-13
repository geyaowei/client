package com.example.demo.models.highlevelclient;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.TransportRequestOptions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
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

    public static class Builder {
        private String hostName = null;
        private RestHighLevelClient client = null;

        public Builder hostName(String hostName) {
            this.hostName = hostName;
            return this;
        }

        public Builder client() {
            RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(this.hostName, 9200, "http")));
            this.client = client;
            return this;
        }

        public HighLevelClient build() {
            return new HighLevelClient().build(this);
        }

    }

    private HighLevelClient build(Builder builder) {
        this.client = builder.client;
        return this;
    }

    /**
     * @throws java.io.IOException
     */
    public void CreateRolloverIndexAndMapping() throws java.io.IOException {
        RolloverRequest request = new RolloverRequest("logs_write", "logs-000004");
        request.addMaxIndexAgeCondition(new TimeValue(10, TimeUnit.MINUTES));
        request.addMaxIndexDocsCondition(1000);
        request.addMaxIndexSizeCondition(new ByteSizeValue(50, ByteSizeUnit.MB));
        request.dryRun(false);
        //暂不支持6.2.4及以下版本
        request.getCreateIndexRequest().settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
        request.getCreateIndexRequest().mapping("type", "field", "type=keyword");
        //request.getCreateIndexRequest().alias(new Alias("another_alias"));
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

    /**
     * 创建日志详情索引
     */
    public void createLogDetailIndex() throws Exception {
        CreateIndexRequest request = new CreateIndexRequest("order_log_detail");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("_doc");
            {
                builder.startObject("properties");
                {
                    builder.startObject("order_no");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("order_source");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("order_paltform_id");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("order_svc_name");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("applicant");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("current_svc");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_start_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_end_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_exec_time");
                    {
                        builder.field("type", "double");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_state");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_repair_time");
                    {
                        builder.field("type", "double");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_back_count");
                    {
                        builder.field("type", "integer");
                    }
                    builder.endObject();
                    builder.startObject("current_svc_reponse_time");
                    {
                        builder.field("type", "double");
                    }
                    builder.endObject();
                    builder.startObject("depend_current_svc_name");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("current_step_name");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("current_step_start_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();
                    builder.startObject("current_step_end_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();
                    builder.startObject("current_step_back_flag");
                    {
                        builder.field("type", "boolean");
                    }
                    builder.endObject();
                    builder.startObject("current_step_restart_flag");
                    {
                        builder.field("type", "boolean");
                    }
                    builder.endObject();
                    builder.startObject("current_step_exec_result");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();
                    builder.startObject("current_step_exec_time");
                    {
                        builder.field("type", "double");
                    }
                    builder.endObject();
                    builder.startObject("currrent_append_info");
                    {
                        builder.field("type", "text");
                    }
                    builder.endObject();
                    builder.startObject("current_operate_object");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("current_process_step_type");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("current_process_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();

                    builder.startObject("current_process_owner");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("order_status");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("order_create_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();

                    builder.startObject("order_end_time");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();

                    builder.startObject("order_exec_time");
                    {
                        builder.field("type", "double");
                    }
                    builder.endObject();

                    builder.startObject("order_state");
                    {
                        builder.field("type", "keyword");
                    }
                    builder.endObject();

                    builder.startObject("order_back_count");
                    {
                        builder.field("type", "integer");
                    }
                    builder.endObject();

                    builder.startObject("order_time_out_flag");
                    {
                        builder.field("type", "boolean");
                    }
                    builder.endObject();

                    builder.startObject("order_breakdown_flag");
                    {
                        builder.field("type", "boolean");
                    }
                    builder.endObject();

                    builder.startObject("order_force_close_flag");
                    {
                        builder.field("type", "boolean");
                    }
                    builder.endObject();

                    builder.startObject("order_score");
                    {
                        builder.field("type", "double");
                    }
                    builder.endObject();

                    builder.startObject("timestamp");
                    {
                        builder.field("type", "date").field("format","yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping("_doc", builder);
        CreateIndexResponse createIndexResponse = client.indices().create(request);
        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        System.out.println("res:"+acknowledged+"---"+shardsAcknowledged);
    }

    public void bulkInsertDocument() throws Exception{
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("order_no", "yunguan&1000000000");
        jsonMap.put("order_paltform_id", "platform-1");
        jsonMap.put("order_score", Double.valueOf("0"));
        jsonMap.put("order_source", "tongyifuwupingtai");
        jsonMap.put("order_state", "执行中");
        jsonMap.put("order_svc_name", "user-1");
        jsonMap.put("order_time_out_flag", Boolean.valueOf(false));
        jsonMap.put("timestamp", System.currentTimeMillis());
        jsonMap.put("order_force_close_flag", Boolean.valueOf(false));
        jsonMap.put("order_exec_time", Double.valueOf(2));
        jsonMap.put("order_end_time", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("order_create_time", LocalDateTime.now().plusDays(-3).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("order_breakdown_flag", Boolean.valueOf(false));
        jsonMap.put("order_back_count", Integer.valueOf(0));
        jsonMap.put("depend_current_svc_name", "");
        jsonMap.put("currrent_append_info", "{'telnet':'test'}");
        jsonMap.put("current_svc_state", "成功");
        jsonMap.put("current_svc_start_time", LocalDateTime.now().plusDays(-2).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("current_svc_reponse_time", Double.valueOf(0.2));
        jsonMap.put("current_svc_repair_time", Double.valueOf(0.0));
        jsonMap.put("current_svc_exec_time", Double.valueOf(0.5));
        jsonMap.put("current_svc_end_time", LocalDateTime.now().plusDays(-1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("current_svc_back_count", Integer.valueOf(0));
        jsonMap.put("current_svc", "user-1");
        jsonMap.put("current_step_start_time", LocalDateTime.now().plusDays(-2).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("current_step_restart_flag", Boolean.valueOf(false));
        jsonMap.put("current_step_name", "step-1");
        jsonMap.put("current_step_end_time", LocalDateTime.now().plusDays(-1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        jsonMap.put("current_step_exec_time", Double.valueOf(1));
        jsonMap.put("current_step_back_flag", Boolean.valueOf(false));
        jsonMap.put("current_step_exec_result", Boolean.valueOf(false));
        jsonMap.put("current_operate_object", "hangxin");
        jsonMap.put("current_process_owner", "");
        jsonMap.put("current_process_step_type", "");
        //jsonMap.put("current_process_time", "");

        IndexRequest indexRequest = new IndexRequest("order_log_detail", "_doc", "1")
                .source(jsonMap);

        IndexResponse indexResponse = client.index(indexRequest);
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();

        System.out.println("index:"+index+" type:"+type+" id:"+id+" version:"+version);
    }
}
