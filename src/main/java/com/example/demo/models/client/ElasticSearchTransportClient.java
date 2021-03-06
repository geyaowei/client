package com.example.demo.models.client;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by hadoop on 2018/8/17.
 */
public class ElasticSearchTransportClient {

    private final static String article="article";
    private final static String content="content";
    private  String clusterName ;
    private  String hostName ;
    private TransportClient client = null;

    public String getClusterName() {
        return clusterName;
    }

    public String getHostName() {
        return hostName;
    }

    public static class Builder{
        private String clusterName = null;
        private String hostName = null;
        private TransportClient client= null;
        public Builder clusterName(String clusterName){
            this.clusterName = clusterName;
            return this;
        }

        public Builder hostName(String hostName){
            this.hostName = hostName;
            return this;
        }
        public  Builder client() throws UnknownHostException {
            Settings settings = Settings.builder().put("cluster.name", this.clusterName).build();
            TransportClient client  = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(this.hostName), 9300));
            this.client = client;
            return this;
        }

        public ElasticSearchTransportClient build(){
            return new ElasticSearchTransportClient().build(this);
        }

    }

    private ElasticSearchTransportClient build(Builder builder) {
        this.clusterName = builder.clusterName;
        this.hostName = builder.hostName;
        this.client = builder.client;
        return this;
    }

    /**
     * 创建索引---方式1
     * @throws java.io.IOException
     */
    public void CreateIndexAndMapping() throws java.io.IOException{
        CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(article);
        //CreateIndexRequest request = new CreateIndexRequest(article);
        //request.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties") //设置之定义字段
                .startObject("author")
                .field("type","keyword") //设置数据类型
                .endObject()
                .startObject("title")
                .field("type","keyword")
                .endObject()
                .startObject("content")
                .field("type","text")
                .endObject()
                .startObject("price")
                .field("type","keyword")
                .endObject()
                .startObject("view")
                .field("type","keyword")
                .endObject()
                .startObject("tag")
                .field("type","keyword")
                .endObject()
                .startObject("date")
                .field("type","date")  //设置Date类型
                .field("format","yyyy-MM-dd'T'HH:mm:ssZ") //设置Date的格式
                .endObject()
                .endObject()
                .endObject();
        cirb.addMapping(content, mapping);
        CreateIndexResponse res=cirb.execute().actionGet();
        System.out.println("----------添加映射成功----------");
    }

    /**
     * 创建索引---方式2(可以设置setting,mapping,alias...)
     * @throws java.io.IOException
     */
    public void CreateIndexAndMapping2() throws java.io.IOException{
        CreateIndexRequest request = new CreateIndexRequest(article+"-test");
        request.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        Map<String,Object> jsonMap = new HashMap<String,Object>();

        //fields
        Map<String,Object> name = new HashMap<String,Object>();
        name.put("type","keyword");
        Map<String,Object> age = new HashMap<String,Object>();
        age.put("type","integer");
        Map<String,Object> sex = new HashMap<String,Object>();
        sex.put("type","keyword");

        //属性
        Map<String,Object> properties = new HashMap<String,Object>();
        properties.put("name",name);
        properties.put("age",age);
        properties.put("sex",sex);

        //type
        Map<String,Object> doc = new HashMap<String,Object>();
        doc.put("properties",properties);
        jsonMap.put("doc",doc);
        //index
        request.mapping("doc",jsonMap);
        CreateIndexResponse createIndexResponse = client.admin().indices().create(request).actionGet();
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("执行后的状态："+acknowledged);
    }

    /**
     * 创建索引---方式3(可以设置setting,mapping,alias...)
     * @throws java.io.IOException
     */
    public void CreateIndexAndMapping3() throws java.io.IOException{
        CreateIndexRequest request = new CreateIndexRequest(article+"-test2");
        request.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        //type
        builder.startObject("doc");
        //属性
        builder.startObject("properties");
        //fields
        builder.startObject("name");
        builder.field("type","keyword");
        builder.endObject();
        builder.startObject("age");
        builder.field("type","integer");
        builder.endObject();
        builder.startObject("sex");
        builder.field("type","keyword");
        builder.endObject();

        builder.endObject();
        builder.endObject();
        builder.endObject();

        request.mapping("doc",builder);
        request.alias(new Alias("alias"));
        CreateIndexResponse createIndexResponse = client.admin().indices().create(request).actionGet();
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("执行后的状态："+acknowledged);
    }
    //进行match查询
    public void getBoolQuery(){
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        mustQuery.must(QueryBuilders.matchAllQuery());
        //mustQuery.must(QueryBuilders.matchQuery("name", "applog"));
        SearchResponse response = client.prepareSearch("article-test").setTypes("doc")
                         .setQuery(mustQuery)
                            .execute().actionGet();
        SearchHits hits = response.getHits();
        System.out.println(response.getHits().getHits().toString());
        for(SearchHit hit : hits){
           System.out.println(hit.docId());
        }
    }

    /**
     * 多字段查询
     */
    public  void multisearch() {
        try {
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch("test").setTypes("doc");
            SearchResponse searchResponse = searchRequestBuilder.
                    setQuery(QueryBuilders.boolQuery()
                            //.must(QueryBuilders.fuzzyQuery("tag","example"))
                            //.must(QueryBuilders.matchQuery("tag","example"))
                            .must(QueryBuilders.matchPhraseQuery("tag","example"))
                            .must(QueryBuilders.termQuery("age",18)))
                    .setFrom(0).setSize(100).execute().actionGet();
            SearchHit[] results = searchResponse.getHits().getHits();
            for(SearchHit hit : results){
                String sourceAsString = hit.getSourceAsString();
                JSONObject json = JSONObject.parseObject(sourceAsString);
                System.out.println(json.toJSONString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }





}
