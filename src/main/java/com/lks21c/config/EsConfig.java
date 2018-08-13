package com.lks21c.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author lks21c
 */
@ComponentScan("com.lks21c")
@Configuration
public class EsConfig {
    private Logger logger = LoggerFactory.getLogger(EsConfig.class);

    @Value("${es.transport.host}")
    private String esTransportHost;

    @Value("${es.transport.port}")
    private int esTransportPort;

    @Value("${es.rest.host}")
    private String esRestHost;

    @Value("${es.rest.port}")
    private int esRestPort;

    @Bean(destroyMethod = "close")
    public TransportClient transportClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "alyes").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName(esTransportHost), esTransportPort));
        return client;
    }

    @Bean
    public RestClient restClient() {
        RestClient restClient = RestClient.builder(
                new HttpHost(esRestHost, esRestPort, "http")
        ).build();
        return restClient;
    }
}
