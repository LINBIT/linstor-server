package com.linbit.linstor.api;

import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;

/**
 * API call which runs asynchronously.
 */
public interface ApiCallReactive extends BaseApiCall
{
    /**
     * Execute the API call.
     *
     * @param msgDataIn The input stream containing serialized parameters to the call
     * @return An asynchronous stream of answers
     */
    Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException;
}
