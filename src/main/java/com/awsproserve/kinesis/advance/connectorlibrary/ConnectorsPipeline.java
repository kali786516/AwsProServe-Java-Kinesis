package com.awsproserve.kinesis.advance.connectorlibrary;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.*;
import com.amazonaws.services.kinesis.connectors.s3.S3Emitter;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

//IKinesisConnectorPipeline Interface has two type parameters Type T source and Type O which is target as we are writing to S3
// we are converting it to byte
// Data Flow :- ITransformerBase --> to class --> from class -->IFilter --> IBuffer --> IEmitter

/**
 * IKinesisConnectorPipeline Interface has two type parameters Type T source and Type O which is target as we are writing to S3
 *  we are converting it to byte
 *  Data Flow :- ITransformerBase --> to class --> from class -->IFilter --> IBuffer --> IEmitter
 */
public class ConnectorsPipeline implements IKinesisConnectorPipeline<Status, byte[]> {

    /**
     * IEmitter has two methods
     * emit :- write content of buffer to destination
     * fail :- responsible for hadnling records which cannot be succesfully written
     * @param configuration
     * @return
     */
    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        // we are using builtin S3Emitter which writes to S3
        return new S3Emitter(configuration);
    }

    /**
     * IBuffer has ConsumeRecord which writes buffer to a record
     * shouldFlush buffer get written to destination (S3,Redshift etc...)
     * getRecords what allows get records for this buffer
     * @param configuration
     * @return
     */
    @Override
    public IBuffer<Status> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<>(configuration);
    }

    /**
     * first step is to convert kinesis records to type T , to do this we need to implement itransform interface
     * @param configuration
     * @return
     */
    @Override
    public ITransformerBase<Status, byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new ITransformer<Status, byte[]>() {
            //convert kinesis record to a object of type T,in this case we take a record and get Status
            // (Status is twitter data)
            @Override
            public Status toClass(Record record) throws IOException {
                return getStatus(record);
            }
            //from class which converts records from our pipeline to type o , in this case to byte as S3 support Byte as type
            @Override
            public byte[] fromClass(Status record) throws IOException {
                try {
                    return new ObjectMapper().writeValueAsString(record).getBytes();
                } catch (JsonProcessingException e) {
                    throw new IOException("Failed to serialize record", e);
                }

            }
        };
    }


    /**
     * @param record
     * @return
     */
    private Status getStatus(Record record) {
        ByteBuffer data = record.getData();
        String tweetJson = new String(data.array(), StandardCharsets.UTF_8);
        return parseTweet(tweetJson);
    }

    /**
     * @param tweetJson
     * @return
     */
    private Status parseTweet(String tweetJson) {
        try {
            return TwitterObjectFactory.createStatus(tweetJson);
        } catch (TwitterException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * IFilter is another method in IKinesisConnectorPipeline which can filter recrods
     * after filtering it goes to next stage which is Ibuffer
     * @param configuration
     * @return
     */
    @Override
    public IFilter<Status> getFilter(KinesisConnectorConfiguration configuration) {
        // Use AllPassFilter if all tweets should be passed
        return record -> record.getLang().equals("en");
    }
}
