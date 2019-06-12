package com.awsproserve.kinesis.advance.connectorlibrary;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import twitter4j.Status;

import java.util.Properties;

/**
 *
 */
public class ConnectorsMain {

    /**
     * @param args
     */
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(
                // App name
                KinesisConnectorConfiguration.PROP_APP_NAME,
                "save-to-s3"
        );
        properties.setProperty(
                //S3 bucket name
                KinesisConnectorConfiguration.PROP_S3_BUCKET,
                "aws-pro-serve-kinesis-poc"
        );
        properties.setProperty(
                //buffer size limit
                KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT,
                "100"
        );

        KinesisConnectorConfiguration config = new KinesisConnectorConfiguration(
                properties,
                new DefaultAWSCredentialsProviderChain()
        );

        // create pipeline
        KinesisConnectorRecordProcessorFactory<Status, byte[]> factory =
                new KinesisConnectorRecordProcessorFactory<>(
                        new ConnectorsPipeline(),
                        config
                );


        final KinesisClientLibConfiguration kclConfig = new KinesisClientLibConfiguration(
                "kinesis-to-s3",
                "tweets-stream",
                new DefaultAWSCredentialsProviderChain(),
                "worker-1"
        );

        // kcl worker
        Worker worker = new Worker.Builder()
                .recordProcessorFactory(factory)
                .config(kclConfig)
                .build();

        worker.run();
    }
}
