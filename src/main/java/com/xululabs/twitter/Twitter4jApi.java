package com.xululabs.twitter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
public class Twitter4jApi {

  Query query;
  QueryResult result;

  /**
   * use to get twitter instance 
   * @return Twiiter Instance
   */
  public Twitter getTwitterInstance() throws Exception{

    Twitter twitter = new TwitterFactory().getInstance();
    return twitter;
  }
  
  public ArrayList<Map<String, Object>> search(Twitter twitter, String keyword) throws Exception{
    int searchResultCount = 0;
    long lowestTweetId = Long.MAX_VALUE;
    int tweetsCount = 0;
    int requestsCount = 0;
    
    ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();  
    Query query = new Query(keyword);
    query.setCount(100);
    do {
        QueryResult queryResult;
      try {
        queryResult = twitter.search(query);
          searchResultCount = queryResult.getTweets().size();
           requestsCount++;
          for (Status tweet : queryResult.getTweets()) {
            Map<String, Object> tweetInfo = new HashMap<String, Object>();
            tweetInfo.put("id", tweet.getId());
            tweetInfo.put("tweet", tweet.getText());
            tweetInfo.put("screenName", tweet.getUser().getScreenName());
            tweets.add(tweetInfo);
            tweetsCount++;
              if (tweet.getId() < lowestTweetId) {
                  lowestTweetId = tweet.getId();
                  query.setMaxId(lowestTweetId);
              }           
          }
      } catch (TwitterException e) {
        break;
      } 
    } while (true);   
    twitter = null;
    return tweets;
    
  }

  

}
