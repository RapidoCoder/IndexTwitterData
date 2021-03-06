package com.xululabs.IndexTwitterData;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import twitter4j.Twitter;

import com.xululabs.twitter.Twitter4jApi;
public class DeployServer extends AbstractVerticle {
  HttpServer server;
  Router router;
  String host;
  int port;
  String esHost;
  int esPort;
  int bulkSize;
  Twitter4jApi twitter4jApi;
  public DeployServer() {

    this.host = "localhost";
    this.port = 8484;
    this.twitter4jApi = new Twitter4jApi();
    this.esHost = "localhost";
    this.esPort = 9300;
    this.bulkSize = 500;

  }

  /**
   * Deploying the verical
   */
  @Override
  public void start() {
    server = vertx.createHttpServer();
    router = Router.router(vertx);
    // Enable multipart form data parsing
    router.route().handler(BodyHandler.create());
    router.route().handler(CorsHandler.create("*").allowedMethod(HttpMethod.GET).allowedMethod(HttpMethod.POST)
        .allowedMethod(HttpMethod.OPTIONS).allowedHeader("Content-Type, Authorization"));
    // registering different route handlers
    this.registerHandlers();
    server.requestHandler(router::accept).listen(port, host);
  }

  /**
   * For Registering different Routes
   */
  public void registerHandlers() {
    router.route(HttpMethod.GET, "/").handler(this::welcomeRoute);
    router.route(HttpMethod.POST, "/search").blockingHandler(this::search);
    router.route(HttpMethod.POST, "/index_tweets").blockingHandler(this::indexTweets);

  }

  /**
   * welcome route
   * 
   * @param routingContext
   */
  public void welcomeRoute(RoutingContext routingContext) {
    routingContext.response().end("<h1> Welcome To Route </h1>");
  }

  /**
   * use to search tweets for given keyword
   * 
   * @param routingContext
   * @throws Exception
   */
  public void search(RoutingContext routingContext) {
    String response;
    String keyword = (routingContext.request().getParam("keyword") == null) ? "iphone" : routingContext.request().getParam("keyword");
    try {
      response = new ObjectMapper().writeValueAsString(this.searchTweets(this.getTwitterInstance(), keyword));
    } catch (Exception ex) {
      response = "{status: 'error', 'msg' : " + ex.getMessage() + "}";
    }
    routingContext.response().end(response);

  }

  /**
   * use to index tweets for given keyword
   * 
   * @param routingContext
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  public void indexTweets(RoutingContext routingContext) {
    String response;
    String keyword = (routingContext.request().getParam("keyword") == null) ? "iphone" : routingContext.request().getParam("keyword");
    try {
      ArrayList<Map<String, Object>> tweets = this.searchTweets(this.getTwitterInstance(), keyword);
      List<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
      for (int i = 0; i < tweets.size(); i += bulkSize) {
        ArrayList<Map<String, Object>>  bulk = new  ArrayList<Map<String, Object>>(tweets.subList(i, Math.min(i + bulkSize, tweets.size())));
        bulks.add(bulk);
      }
      for(ArrayList<Map<String, Object>> tweetsList : bulks){
              this.indexInES(tweetsList);
      }    
      response = "{status : 'success'}";
    } catch (Exception ex) {
      response = "{status: 'error', 'msg' : " + ex.getMessage() + "}";
    }
    routingContext.response().end(response);

  }

  /**
   * use to search tweets
   * 
   * @param keyword
   * @return
   * @throws Exception
   */
  public ArrayList<Map<String, Object>> searchTweets(Twitter twitter, String keyword) throws Exception {
    ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter, keyword);
    return tweets;

  }

  /**
   * use to index tweets in ES
   * 
   * @param tweets
   * @throws UnknownHostException
   */
  public void indexInES(ArrayList<Map<String, Object>> tweets) throws UnknownHostException {
    TransportClient client = this.esClient(this.esHost, this.esPort);
  
    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();   
    for(Map<String, Object> tweet : tweets){
    bulkRequestBuilder.add(client.prepareUpdate("twitter", "tweets", tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));
    }
    bulkRequestBuilder.setRefresh(true).execute().actionGet();
   
    client.close();
  }
  
  /**
   * use to get es instance
   * @param esHost
   * @param esPort
   * @return
   * @throws UnknownHostException
   */
  public TransportClient esClient(String esHost, int esPort) throws UnknownHostException{
    TransportClient client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(esHost, esPort));
    return client;
  }

  /**
   * get instance of twitter api
   * 
   * @return twitter4jApi
   * @throws Exception
   */
  public Twitter getTwitterInstance() throws Exception {
    return twitter4jApi.getTwitterInstance();
  }

}

