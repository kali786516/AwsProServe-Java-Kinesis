package com.awsproserve.kinesis.fundamentals.consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 *
 */
public class TwitterConsumerMain {

    /**
     * @param args
     */
    /* Consumer Main class*/
    public static void main(String[] args){

        /*create instance of kinesis client lib configuration*/
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                "tweets-processor",
                "tweets-stream",
                new DefaultAWSCredentialsProviderChain(),
                "worker-1"
        );
        // can be : LATEST (Latest record kinesis stream),
        // AT_TIMESTAMP (At particular timestamp of kinesis stream),
        // TRIM_HORIZON (Very first record in kinesis stream)
        config.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        config.withIdleTimeBetweenReadsInMillis(200);


        /*Initiate record processor factory*/
        IRecordProcessorFactory factory = new TweetsProcessorFactory();

        Worker worker=new Worker.Builder()
                .config(config)
                .recordProcessorFactory(factory)
                .build();

        worker.run();


    }



}
