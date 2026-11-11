package com.linbit.linstor.tasks;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.testutils.EmptyErrorReporter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A resource whose first delete answer was an error and whose retry succeeds finishes the resource
 * definition's delete; a retry that fails again does not.
 */
public class RetryResourcesTaskFinishDeleteTest
{
    private final List<ResourceName> finishCalls = new ArrayList<>();
    private final AtomicReference<Flux<ApiCallRc>> finishResult = new AtomicReference<>();
    private RetryResourcesTask task;
    private Resource rsc;
    private Peer peer;
    private NodeName nodeName;
    private ResourceName rscName;

    @Before
    public void setUp() throws Exception
    {
        task = new RetryResourcesTask(
            mock(CtrlStltSerializer.class, RETURNS_DEEP_STUBS),
            new EmptyErrorReporter(),
            name ->
            {
                finishCalls.add(name);
                return finishResult.get();
            }
        );
        rscName = new ResourceName("rd1");
        nodeName = new NodeName("node-a");
        ResourceDefinition rscDfn = mock(ResourceDefinition.class);
        when(rscDfn.getName()).thenReturn(rscName);
        rsc = mock(Resource.class);
        when(rsc.getResourceDefinition()).thenReturn(rscDfn);
        when(rsc.getUuid()).thenReturn(UUID.randomUUID());
        peer = mock(Peer.class);
        finishResult.set(Flux.just(ApiCallRcImpl.singleApiCallRc(ApiConsts.DELETED, "rd1 deleted")));
    }

    private static ByteArrayInputStream answer(long retCode, String msg) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ApiCallResponse.newBuilder().setRetCode(retCode).setMessage(msg).build().writeDelimitedTo(out);
        return new ByteArrayInputStream(out.toByteArray());
    }

    @Test
    public void successfulRetryFinishesTheRscDfnDeleteAfterTheNextStep() throws Exception
    {
        when(peer.apiCall(anyString(), any())).thenReturn(Flux.just(answer(ApiConsts.MODIFIED, "deleted")));
        AtomicBoolean nextStepRan = new AtomicBoolean(false);
        AtomicBoolean nextStepRanFirst = new AtomicBoolean(false);
        finishResult.set(
            Flux.defer(() ->
            {
                nextStepRanFirst.set(nextStepRan.get());
                return Flux.just(ApiCallRcImpl.singleApiCallRc(ApiConsts.DELETED, "rd1 deleted"));
            })
        );

        List<ApiCallRc> out = task.retryFlux(
            rsc, peer, nodeName, Flux.defer(() ->
            {
                nextStepRan.set(true);
                return Flux.empty();
            })
        ).collectList().block();

        assertEquals(List.of(rscName), finishCalls);
        assertTrue("next step must run before the rd delete is finished", nextStepRanFirst.get());
        assertEquals(2, out.size());
        assertTrue(
            out.get(1).stream().anyMatch(entry -> (entry.getReturnCode() & ApiConsts.DELETED) == ApiConsts.DELETED)
        );
    }

    @Test
    public void retryThatFailsAgainDoesNotTouchTheRscDfn() throws Exception
    {
        when(peer.apiCall(anyString(), any())).thenReturn(
            Flux.just(answer(ApiConsts.FAIL_UNKNOWN_ERROR, "cannot destroy: dataset is busy"))
        );

        task.retryFlux(rsc, peer, nodeName, null).collectList().block();

        assertTrue(finishCalls.isEmpty());
    }

    @Test
    public void peerErrorEndsTheRetryWithoutFinishing() throws Exception
    {
        when(peer.apiCall(anyString(), any())).thenReturn(
            Flux.error(new com.linbit.linstor.netcom.PeerNotConnectedException())
        );

        List<ApiCallRc> out = task.retryFlux(rsc, peer, nodeName, null).collectList().block();

        assertTrue(finishCalls.isEmpty());
        assertFalse(out == null);
        assertTrue(out.isEmpty());
    }
}
