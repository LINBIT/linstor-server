package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("view")
@Produces(MediaType.APPLICATION_JSON)
public class View
{
    private final RequestHelper requestHelper;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    View(
        RequestHelper requestHelperRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }


    @GET
    @Path("resources")
    public void viewResources(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("resources") List<String> resources,
        @QueryParam("storage_pools") List<String> storagePools,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        List<String> nodesFilter = nodes != null ? nodes : Collections.emptyList();
        List<String> storagePoolsFilter = storagePools != null ? storagePools : Collections.emptyList();
        List<String> resourcesFilter = resources != null ? resources : Collections.emptyList();

        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            Flux<ApiCallRcWith<ResourceList>> flux = ctrlVlmListApiCallHandler.listVlms(
                nodesFilter, storagePoolsFilter, resourcesFilter)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_LST_VLM, request));

            requestHelper.doFlux(
                asyncResponse,
                listVolumesApiCallRcWithToResponse(flux, limit, offset)
            );
        });
    }

    private Mono<Response> listVolumesApiCallRcWithToResponse(
        Flux<ApiCallRcWith<ResourceList>> apiCallRcWithFlux,
        int limit,
        int offset
    )
    {
        return apiCallRcWithFlux.flatMap(apiCallRcWith ->
        {
            Response resp;
            if (apiCallRcWith.hasApiCallRc())
            {
                resp = ApiCallRcConverter.toResponse(
                    apiCallRcWith.getApiCallRc(),
                    Response.Status.INTERNAL_SERVER_ERROR
                );
            }
            else
            {
                ResourceList resourceList = apiCallRcWith.getValue();
                Stream<Resource.RscApi> rscApiStream = resourceList.getResources().stream();

                if (limit > 0)
                {
                    rscApiStream = rscApiStream.skip(offset).limit(limit);
                }

                final List<JsonGenTypes.Resource> rscs = rscApiStream
                    .map(rscApi -> Json.apiToResource(rscApi, resourceList.getSatelliteStates(), true))
                    .collect(Collectors.toList());

                try
                {
                    resp = Response
                        .status(Response.Status.OK)
                        .entity(objectMapper.writeValueAsString(rscs))
                        .build();
                }
                catch (JsonProcessingException exc)
                {
                    exc.printStackTrace();
                    resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                }
            }

            return Mono.just(resp);
        }).next();
    }

    @GET
    @Path("storage-pools")
    public void viewStoragePools(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @QueryParam("nodes") List<String> nodes,
        @QueryParam("storage_pools") List<String> storagePools,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        List<String> nodesFilter = nodes != null ? nodes : Collections.emptyList();
        List<String> storagePoolsFilter = storagePools != null ? storagePools : Collections.emptyList();

        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            Flux<List<StorPool.StorPoolApi>> flux = ctrlStorPoolListApiCallHandler
                .listStorPools(nodesFilter, storagePoolsFilter)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_LST_STOR_POOL, request));

            requestHelper.doFlux(asyncResponse, storPoolListToResponse(flux, limit, offset));
        });
    }

    private Mono<Response> storPoolListToResponse(
        Flux<List<StorPool.StorPoolApi>> storPoolListFlux,
        int limit,
        int offset
    )
    {
        return storPoolListFlux.flatMap(storPoolList ->
        {
            Response resp;
            Stream<StorPool.StorPoolApi> storPoolApiStream = storPoolList.stream();
            if (limit > 0)
            {
                storPoolApiStream = storPoolApiStream.skip(offset).limit(limit);
            }
            List<JsonGenTypes.StoragePool> storPoolDataList = storPoolApiStream
                .map(Json::storPoolApiToStoragePool)
                .collect(Collectors.toList());

            try
            {
                resp = Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(storPoolDataList))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
            }
            catch (JsonProcessingException exc)
            {
                exc.printStackTrace();
                resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }

            return Mono.just(resp);
        }).next();
    }
}
