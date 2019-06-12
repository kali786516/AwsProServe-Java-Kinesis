package com.awsproserve.kinesis.fundamentals.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 *
 */
public class TweetsProcessorFactory implements IRecordProcessorFactory {

    /*create instance of our processor */

    /**
     * createProcessor returns IRecordProcessor which is TweetsProcessor class
     * @return
     */
    @Override
    public IRecordProcessor createProcessor(){
        return new TweetsProcessor();

    }

}
