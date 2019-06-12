package com.awsproserve.kinesis.fundamentals.consumer;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * TweetsProcessor class implements IRecordProcessor Interface which has three methods initialize,processRecords and shutdown
 * initialize:- initialize the processors
 * processRecords :- process records (tweets)
 * shutdown :- shutdown processor
 */
public class TweetsProcessor implements IRecordProcessor {

    /**
     * initialize Methos takes in InitializationInput and reutns nothing just start the job
     * @param initializationInput
     */
    @Override
    public void initialize(InitializationInput initializationInput){

    }

    /**
     * processRecords :- getStatus
     * @param processRecordsInput
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record:processRecordsInput.getRecords()){
            System.out.println(getStatus(record));
            //get seq number
            System.out.println(record.getSequenceNumber());
            //get partition key
            System.out.println(record.getPartitionKey());
            //check pointer
            checkPoint(processRecordsInput.getCheckpointer());
        }
    }

    /**
     * @param checkpointer
     */
    private void checkPoint(IRecordProcessorCheckpointer checkpointer){

        try {
            checkpointer.checkpoint();

        } catch (InvalidStateException e){
            System.out.println("Kinesis Producer DynamoDB Table might be deleted accidentally");
            //Accidental Deletion of DynamoDB Table deletion might cause this issue
            e.printStackTrace();
        } catch (ShutdownException e){
            //two processors are processing same shard
            e.printStackTrace();
        }
    }


    /** getStatus takes in Record as variable and gets data out of it data and passes it to parseTweet
     * @param record
     * @return Status
     */
    private Status getStatus(Record record){
        ByteBuffer data = record.getData();
        String tweetJson = new String(data.array(),StandardCharsets.UTF_8);
        return parseTweet(tweetJson);
    }

    /**
     * @param tweetJson
     * @return
     */
    private Status parseTweet(String tweetJson){
        try{
            return TwitterObjectFactory.createStatus(tweetJson);
        } catch (TwitterException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * shutdown shutdowns
     * @param shutdownInput
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput){
        ShutdownReason reason = shutdownInput.getShutdownReason();

        switch (reason) {
            case TERMINATE:
            case REQUESTED:
                //whole application is shutting down
                checkPoint(shutdownInput.getCheckpointer());
                break;
            case ZOMBIE:
                System.out.println("Zoombie resource, no need to checkpoint");
                break;
        }

    }

}
