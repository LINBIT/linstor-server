package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;

import jakarta.ws.rs.core.Response;

import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import reactor.core.Exceptions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** A rejected publishOn (RestResponse queue full) is recognised, also when wrapped. */
public class RequestHelperRejectTest
{
    @Test
    public void rejectedIsRecognised()
    {
        assertTrue(RequestHelper.isRejected(new RejectedExecutionException("full")));
        assertTrue(RequestHelper.isRejected(Exceptions.failWithRejected()));
        assertTrue(RequestHelper.isRejected(new RuntimeException(new RejectedExecutionException("wrapped"))));
        assertFalse(RequestHelper.isRejected(new IllegalStateException("other")));
        assertFalse(RequestHelper.isRejected(null));
    }

    @Test
    public void queueFullAnswerIs503WithRetryAfter()
    {
        // toResponse(rc, status) alone answers 500 for an rc with an error entry
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_UNKNOWN_ERROR, "busy"));
        Response response = RequestHelper.serviceUnavailable(apiCallRc);
        assertEquals(503, response.getStatus());
        assertEquals("5", response.getHeaderString("Retry-After"));
        assertTrue(String.valueOf(response.getEntity()).contains("busy"));
    }
}
