package com.example.demo.models.controller;

import com.example.demo.models.client.ElasticSearchTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by hadoop on 2018/8/17.
 */
@RestController
@RequestMapping("/api/vaildate")
@Component
public class VaildateClientController {

    @Value("${elasticSearch.cluster.name}")
    private String clusterName;

    @Value("${elasticSearch.server.host}")
    private String hostName;

    @RequestMapping(value="/client",method= RequestMethod.GET)
    public String getClient(){
        try{
            ElasticSearchTransportClient client = new ElasticSearchTransportClient.Builder().clusterName(clusterName).hostName(hostName).client().build();
            //client.CreateIndexAndMapping()
            //client.CreateIndexAndMapping2();
            client.CreateIndexAndMapping3();
            //client.CreateRolloverIndexAndMapping();
        }catch(UnknownHostException e){
            System.out.println(e);
        }catch(IOException e){
            System.out.println(e);
        }

        return "success";
    }

}
