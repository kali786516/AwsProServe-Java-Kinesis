package com.awsproserve.kinesis.fundamentals.producer;

import com.amazonaws.services.kinesis.producer.*;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author  sri tummala
 */


public class TwitterProducerMain {

    /**
     * Main Class (Main Entry Point)
     * @param args
     */
  public static void main(String[] args){
   // create twitter stream
   TwitterStream twitterStream = createTwitterStream();

   //create twitter listener , which listens when a new tweet is available
   twitterStream.addListener(createListerner());

   //twitter stream data
    twitterStream.sample();

  }


    /**
     * createTwitterStream returns TwitterStream method build configuration first and then returns TwitterStreamFactory
     * @return TwitterStream
     */
    public static TwitterStream createTwitterStream(){
      //create a Twitter stream
      //step 1 create configurations and pass the access keys
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.setOAuthConsumerKey("z")
              .setOAuthConsumerSecret("z")
              .setOAuthAccessToken("")
              .setOAuthAccessTokenSecret("z");

      // create new instance of twitter stream
      return new TwitterStreamFactory(cb.build()).getInstance();
  }

    /**
     * createListerner method returns RawStreamListener abstract class
     * @return
     */
  public static RawStreamListener createListerner(){

      KinesisProducer kinesisProducer=createKinesisProducer();
      return new TweetsStatusListener(kinesisProducer);
  }

    /**
     * createKinesisProducer returns KinesisProducer by taking in KinesisProducerConfiguration and returning KinesisProducer
     * @return
     */
  private static KinesisProducer createKinesisProducer(){
      //producer configuration
      KinesisProducerConfiguration config=new KinesisProducerConfiguration()
              .setRequestTimeout(60000) // request timeout
              .setRecordMaxBufferedTime(15000) // max buffered time
              .setRegion("us-east-1");
      return new KinesisProducer(config);
  }

    /**
     * TweetsStatusListener takes in kinesisProducer as variable value and implements onMessage method and onException method
     * @return
     */
  private static class TweetsStatusListener implements RawStreamListener {

      private KinesisProducer kinesisProducer;

        public TweetsStatusListener(KinesisProducer kinesisProducer) {
            this.kinesisProducer = kinesisProducer;
        }

        public void onMessage(String tweetJson){
          try {
              // status is nothing but twitter Json message
              Status status = TwitterObjectFactory.createStatus(tweetJson);
              if (status.getUser()!= null) {
                  System.out.println(tweetJson);
                  /*
                  System.out.println(status.getUser());
                  System.out.println(status.getText());
                  */
                  //convert json to bytes
                  byte[] tweetBytes = tweetJson.getBytes(StandardCharsets.UTF_8);
                  String partitionKey = status.getLang(); //partition keys tweet lang

                  //open producer add record get asynchronous ListenableFuture

                  ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(
                          "tweets-stream", // stream name
                          partitionKey, //partition key
                          ByteBuffer.wrap(tweetBytes) // convert tweets json to bytes
                  );

                  Futures.addCallback(f, new FutureCallback<UserRecordResult>() {
                      @Override
                      // on success of message put do nothing
                      public void onSuccess(UserRecordResult userRecordResult) {

                      }
                      @Override
                      // on failure of message put
                      public void onFailure(Throwable throwable) {
                          if (throwable instanceof UserRecordFailedException){
                              UserRecordFailedException e = (UserRecordFailedException) throwable;
                              UserRecordResult result = e.getResult();

                              Attempt last = Iterables.getLast(result.getAttempts());
                              System.err.println(String.format(
                                      "Put failed - %s",
                                      last.getErrorMessage()
                              ));
                          }
                      }
                  });


              }

          } catch (TwitterException e){
              e.printStackTrace();
          }

      }

      public void onException(Exception e){
          e.printStackTrace();

      }
  }


}
