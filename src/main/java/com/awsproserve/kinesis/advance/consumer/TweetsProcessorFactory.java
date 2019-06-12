package com.awsproserve.kinesis.advance.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class TweetsProcessorFactory  implements IRecordProcessorFactory {
    @Override
    public IRecordProcessor createProcessor() {
        return new TweetsCounter();
    }
}
