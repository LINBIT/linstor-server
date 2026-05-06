package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.locks.LockGuardFactory;

import javax.inject.Provider;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Lightweight contract test. The full repick flow needs DB-backed test scaffolding and lives in
 * an integration test; this test just pins the static contract surface (retcode, maxRetries) and
 * verifies the "missing objref" early-return so a typo in extraction code is caught.
 */
@SuppressFBWarnings("NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
public class DrbdAdjustBlockedPortHandlerTest
{
    private DrbdAdjustBlockedPortHandler handler;

    @Before
    public void setup()
    {
        ErrorReporter errorReporter = mock(ErrorReporter.class);
        AccessContext apiCtx = mock(AccessContext.class);
        ScopeRunner scopeRunner = mock(ScopeRunner.class);
        LockGuardFactory lockGuardFactory = mock(LockGuardFactory.class);
        CtrlTransactionHelper ctrlTransactionHelper = mock(CtrlTransactionHelper.class);
        @SuppressWarnings("unchecked")
        Provider<CtrlSatelliteUpdateCaller> updateCallerProvider = mock(Provider.class);

        handler = new DrbdAdjustBlockedPortHandler(
            errorReporter, apiCtx, scopeRunner, lockGuardFactory, ctrlTransactionHelper, updateCallerProvider
        );
    }

    @Test
    public void retcodeMatchesInfoPortBlockedDrbdAdjust()
    {
        assertEquals(ApiConsts.FAIL_PORT_BLOCKED_DRBD_ADJUST, handler.retcode());
    }

    @Test
    public void maxRetriesIsFinite()
    {
        assertEquals(5, handler.maxRetries());
    }

    @Test
    public void beforeRetryEarlyReturnsWhenObjrefMissing()
    {
        // No KEY_BLOCKED_PORTS_LIST objref on the entry -> handler must log + Mono.empty,
        // not crash trying to enter a tx scope with no work to do.
        Resource rsc = mockResource();
        ApiCallRc signalRc = ApiCallRcImpl.singletonApiCallRc(
            ApiCallRcImpl.entryBuilder(ApiConsts.FAIL_PORT_BLOCKED_DRBD_ADJUST, "blocked").build()
        );

        Mono<Void> result = handler.beforeRetry(rsc, signalRc, handler.newContext());
        StepVerifier.create(result).verifyComplete();
    }

    @Test
    public void beforeRetryEarlyReturnsWhenObjrefBlank()
    {
        Resource rsc = mockResource();
        ApiCallRc signalRc = ApiCallRcImpl.singletonApiCallRc(
            ApiCallRcImpl.entryBuilder(ApiConsts.FAIL_PORT_BLOCKED_DRBD_ADJUST, "blocked")
                .putObjRef(ApiConsts.KEY_BLOCKED_PORTS_LIST, "  ")
                .build()
        );

        StepVerifier.create(handler.beforeRetry(rsc, signalRc, handler.newContext())).verifyComplete();
    }

    @Test
    public void describeExhaustionEnumeratesCumulativeBlockedPortsFromContext()
    {
        Resource rsc = mockResource();
        DrbdAdjustBlockedPortHandler.RepickHistory history = handler.newContext();
        // simulate three retries that each reported different blocked ports
        history.blockedPortsTried.add(7000);
        history.blockedPortsTried.add(7002);
        history.blockedPortsTried.add(7004);

        ApiCallRc.RcEntry entry = handler.describeExhaustion(rsc, history, 3);

        assertEquals(ApiConsts.FAIL_INVLD_RSC_PORT, entry.getReturnCode());
        String msg = entry.getMessage();
        assertTrue("message has rsc name: " + msg, msg.contains("rscTest"));
        assertTrue("message has node name: " + msg, msg.contains("nodeTest"));
        assertTrue("message has attempts: " + msg, msg.contains("3"));
        // cumulative list, not just the last attempt
        assertTrue("message has all tried ports: " + msg, msg.contains("[7000, 7002, 7004]"));
    }

    private static Resource mockResource()
    {
        Resource rsc = mock(Resource.class);
        ResourceDefinition rscDfn = mock(ResourceDefinition.class);
        Node node = mock(Node.class);
        try
        {
            ResourceName rscName = new ResourceName("rscTest");
            NodeName nodeName = new NodeName("nodeTest");
            Mockito.when(rsc.getResourceDefinition()).thenReturn(rscDfn);
            Mockito.when(rscDfn.getName()).thenReturn(rscName);
            Mockito.when(rsc.getNode()).thenReturn(node);
            Mockito.when(node.getName()).thenReturn(nodeName);
        }
        catch (Exception exc)
        {
            throw new AssertionError(exc);
        }
        return rsc;
    }
}
