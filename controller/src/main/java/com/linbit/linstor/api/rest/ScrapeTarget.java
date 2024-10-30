package com.linbit.linstor.api.rest;

import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.RequestHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("metrics/scrape-target")
public class ScrapeTarget
{
    private static final int DEFAULT_REACTOR_METRICS_PORT = 9942;
    private final RequestHelper requestHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final NodeRepository nodeRepository;
    private final ObjectMapper objectMapper;

    @Inject
    public ScrapeTarget(
        RequestHelper requestHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        LockGuardFactory lockGuardFactoryRef,
        NodeRepository nodeRepositoryRef
    )
    {
        requestHelper = requestHelperRef;
        peerAccCtx = peerAccCtxRef;
        lockGuardFactory = lockGuardFactoryRef;
        nodeRepository = nodeRepositoryRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response scrapeTarget(@Context Request request)
    {
        return requestHelper.doInScope("scrape-target", request, () ->
        {
            try (LockGuard ignored = lockGuardFactory.build(
                LockGuardFactory.LockType.READ,
                LockGuardFactory.LockObj.NODES_MAP))
            {
                List<String> nodeScrapeAddrs = new ArrayList<>();
                for (var node : nodeRepository.getMapForView(peerAccCtx.get()).values())
                {
                    NodeApi nodeApi = node.getApiData(peerAccCtx.get(), null, null);
                    if (nodeApi.getType().equalsIgnoreCase(Node.Type.SATELLITE.name()) ||
                        nodeApi.getType().equalsIgnoreCase(Node.Type.COMBINED.name()))
                    {
                        nodeScrapeAddrs.add(String.format(
                            "%s:%d", nodeApi.getActiveStltConn().getAddress(), DEFAULT_REACTOR_METRICS_PORT));
                    }
                }
                Map<String, List<String>> scrapeTarget = Collections.singletonMap("targets", nodeScrapeAddrs);
                List<Object> result = Collections.singletonList(scrapeTarget);
                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(result))
                    .build();
            }
            catch (AccessDeniedException accExc)
            {
                throw new ApiAccessDeniedException(
                    accExc, "Unable to access node list", ApiConsts.FAIL_ACC_DENIED_NODE);
            }
        }, false);
    }
}
