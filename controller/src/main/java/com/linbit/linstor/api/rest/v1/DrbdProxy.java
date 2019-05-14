package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyDisableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyEnableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyModifyApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.HashSet;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("resource-definitions/{rscName}/drbd-proxy")
public class DrbdProxy
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;
    private final CtrlDrbdProxyEnableApiCallHandler ctrlDrbdProxyEnableApiCallHandler;
    private final CtrlDrbdProxyDisableApiCallHandler ctrlDrbdProxyDisableApiCallHandler;
    private final CtrlDrbdProxyModifyApiCallHandler ctrlDrbdProxyModifyApiCallHandler;

    @Inject
    public DrbdProxy(
        RequestHelper requestHelperRef,
        CtrlDrbdProxyEnableApiCallHandler ctrlDrbdProxyEnableApiCallHandlerRef,
        CtrlDrbdProxyDisableApiCallHandler ctrlDrbdProxyDisableApiCallHandlerRef,
        CtrlDrbdProxyModifyApiCallHandler ctrlDrbdProxyModifyApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlDrbdProxyEnableApiCallHandler = ctrlDrbdProxyEnableApiCallHandlerRef;
        ctrlDrbdProxyDisableApiCallHandler = ctrlDrbdProxyDisableApiCallHandlerRef;
        ctrlDrbdProxyModifyApiCallHandler = ctrlDrbdProxyModifyApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    @Path("enable/{nodeA}/{nodeB}")
    public void enableProxy(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.DrbdProxyEnable proxyEnable = objectMapper
                .readValue(jsonData, JsonGenTypes.DrbdProxyEnable.class);
            Flux<ApiCallRc> flux = ctrlDrbdProxyEnableApiCallHandler.enableProxy(
                null,
                nodeA,
                nodeB,
                rscName,
                proxyEnable.port
            ).subscriberContext(requestHelper.createContext(ApiConsts.API_ENABLE_DRBD_PROXY, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
        }
        catch (IOException ioExc)
        {
            ApiCallRcConverter.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @POST
    @Path("disable/{nodeA}/{nodeB}")
    public void disableProxy(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB
    )
    {
        Flux<ApiCallRc> flux = ctrlDrbdProxyDisableApiCallHandler.disableProxy(
            null,
            nodeA,
            nodeB,
            rscName
        ).subscriberContext(requestHelper.createContext(ApiConsts.API_DISABLE_DRBD_PROXY, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }

    @PUT
    public Response modifyProxy(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_MOD_DRBD_PROXY, request, () ->
        {
            JsonGenTypes.DrbdProxyModify proxyModify = objectMapper
                .readValue(jsonData, JsonGenTypes.DrbdProxyModify.class);

            ApiCallRc apiCallRc = ctrlDrbdProxyModifyApiCallHandler.modifyDrbdProxy(
                null,
                rscName,
                proxyModify.override_props,
                new HashSet<>(proxyModify.delete_props),
                proxyModify.compression_type,
                proxyModify.compression_props
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }
}
