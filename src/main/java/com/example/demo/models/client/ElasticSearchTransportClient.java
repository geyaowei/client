package com.example.demo.models.client;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

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



}
