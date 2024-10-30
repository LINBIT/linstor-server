package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlErrorListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.StorPoolApi;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Mono;

@Path("v1/stats")
@Produces(MediaType.APPLICATION_JSON)
public class Stats
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlErrorListApiCallHandler ctrlErrorListApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;

    @Inject
    Stats(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        CtrlErrorListApiCallHandler ctrlErrorListApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        ctrlErrorListApiCallHandler = ctrlErrorListApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    @Path("nodes")
    public Response nodeStats(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_NODE_STATS,
            request,
            () ->
            {
                Stream<NodeApi> nodeApiStream = ctrlApiCallHandler.listNodes(
                    Collections.emptyList(), Collections.emptyList()).stream();

                JsonGenTypes.NodeStats objStats = new JsonGenTypes.NodeStats();
                objStats.count = nodeApiStream.count();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(objStats))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("resource-groups")
    public Response resourceGroupStats(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_RSC_GRP_STATS,
            request,
            () ->
            {
                Stream<ResourceGroupApi> rscGrpApiStream =
                    ctrlApiCallHandler.listResourceGroups(Collections.emptyList(), Collections.emptyList()).stream();

                JsonGenTypes.ResourceGroupStats objStats = new JsonGenTypes.ResourceGroupStats();
                objStats.count = rscGrpApiStream.count();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(objStats))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("resource-definitions")
    public Response rscDfnStats(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_RSC_DFN_STATS,
            request,
            () ->
            {
                Stream<ResourceDefinitionApi> rscDfnApiStream =
                    ctrlApiCallHandler.listResourceDefinitions(Collections.emptyList(), Collections.emptyList()).stream();
                JsonGenTypes.ResourceDefinitionStats objStats = new JsonGenTypes.ResourceDefinitionStats();
                objStats.count = rscDfnApiStream.count();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(objStats))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("resources")
    public Response resourceStats(@Context Request request)
    {
        return requestHelper.doInScope(
            ApiConsts.API_RSC_STATS,
            request,
            () ->
            {
                ResourceList resourceList = ctrlApiCallHandler.listResource(
                    Collections.emptyList(), Collections.emptyList());

                JsonGenTypes.ResourceStats objStats = new JsonGenTypes.ResourceStats();
                objStats.count = resourceList.getResources().size();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(objStats))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("storage-pools")
    public Response storagePoolsStats(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_STOR_POOL_STATS,
            request,
            () ->
            {
                final List<StorPoolApi> storagePoolList = ctrlStorPoolListApiCallHandler
                    .listStorPoolsCached(Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

                JsonGenTypes.StoragePoolStats objStats = new JsonGenTypes.StoragePoolStats();
                objStats.count = storagePoolList.size();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(objStats))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("error-reports")
    public void errorReportStats(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse
    )
    {
        Mono<Response> flux = ctrlErrorListApiCallHandler.listErrorReports(
                Collections.emptySet(),
                false,
                null,
                null,
                Collections.emptySet(),
                1L,
                0L)
            .flatMap(
                errorReportResult ->
                {
                    JsonGenTypes.ErrorReportStats objStats = new JsonGenTypes.ErrorReportStats();
                    objStats.count = errorReportResult.getTotalCount();

                    Response resp;
                    try
                    {
                        resp = Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(objStats))
                            .build();
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return Mono.just(resp);
                }
            )
            .next();

        requestHelper.doFlux(
            ApiConsts.API_ERR_REPORT_STATS,
            request,
            asyncResponse,
            flux
        );
    }
}
