package com.linbit.linstor.api;

import java.io.IOException;
import java.io.InputStream;

/**
 * Controller / Satellite API call
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ApiCall
{
    /**
     * Executes the API call
     *
     * @param msgDataIn The input stream containing serialized parameters to the call
     */
    void execute(InputStream msgDataIn)
        throws IOException;
}
