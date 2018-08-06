package com.linbit.linstor.api;

import java.io.IOException;
import java.io.InputStream;

/**
 * Controller / Satellite API call
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ApiCall extends BaseApiCall
{
    /**
     * Execute the API call.
     * This is run in a transaction and a {@link LinStorScope}.
     * The API call is automatically completed on return, so any answers must be sent before then.
     *
     * @param msgDataIn The input stream containing serialized parameters to the call
     */
    void execute(InputStream msgDataIn)
        throws IOException;
}
