package com.xululabs.IndexTwitterData;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.xululabs.twitter.Twitter4jApi;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import twitter4j.Twitter;
public class DeployServer extends AbstractVerticle {
  HttpServer server;
  Router router;
  String host;
  int port;
  Twitter4jApi twitter4jApi;
  String esHost = "localhost";
  int esPort = 9300;

  public DeployServer() {

    this.host = "localhost";
    this.port = 8383;
    this.twitter4jApi = new Twitter4jApi();

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
    String keyword = (routingContext.request().getParam("keyword") == null) ? "iphone"
        : routingContext.request().getParam("keyword");
    try {
      response = new ObjectMapper().writeValueAsString(this.searchTweets(this.getTwitterInstance(), keyword));
    } catch (Exception ex) {
      // TODO Auto-generated catch block
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
  public void indexTweets(RoutingContext routingContext) {
    String response;
    String keyword = (routingContext.request().getParam("keyword") == null) ? "iphone"
        : routingContext.request().getParam("keyword");
    try {
      ArrayList tweets = this.searchTweets(this.getTwitterInstance(), keyword);
      this.indexInES(tweets);
      response = "{status : 'success'}";
      // response = new ObjectMapper().writeValueAsString();
    } catch (Exception ex) {
      // TODO Auto-generated catch block
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
    long startTime = System.currentTimeMillis();
    TransportClient client = this.esClient(this.esHost, this.esPort);
    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
    for(Map<String, Object> tweet : tweets){
    bulkRequestBuilder.add(client.prepareUpdate("twitter", "tweets", tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));
    }
    BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    client.close();
      long endTime   = System.currentTimeMillis();
      long totalTime = endTime - startTime;
      System.out.println("Total program execution time : " + totalTime / 1000 );

  }
  
  /**
   * use to get es instance
   * @param esHost
   * @param esPort
   * @return
   * @throws UnknownHostException
   */
  public TransportClient esClient(String esHost, int esPort) throws UnknownHostException{
    TransportClient client = new TransportClient()
            .addTransportAddress(new InetSocketTransportAddress(esHost, esPort));
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

