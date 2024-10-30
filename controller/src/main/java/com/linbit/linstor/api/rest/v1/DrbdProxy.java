package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyDisableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyEnableApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyModifyApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/resource-definitions/{rscName}/drbd-proxy")
public class DrbdProxy
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;
    private final CtrlDrbdProxyEnableApiCallHandler ctrlDrbdProxyEnableApiCallHandler;
    private final CtrlDrbdProxyDisableApiCallHandler ctrlDrbdProxyDisableApiCallHandler;
    private final CtrlDrbdProxyModifyApiCallHandler ctrlDrbdProxyModifyApiCallHandler;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    public DrbdProxy(
        RequestHelper requestHelperRef,
        CtrlDrbdProxyEnableApiCallHandler ctrlDrbdProxyEnableApiCallHandlerRef,
        CtrlDrbdProxyDisableApiCallHandler ctrlDrbdProxyDisableApiCallHandlerRef,
        CtrlDrbdProxyModifyApiCallHandler ctrlDrbdProxyModifyApiCallHandlerRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlDrbdProxyEnableApiCallHandler = ctrlDrbdProxyEnableApiCallHandlerRef;
        ctrlDrbdProxyDisableApiCallHandler = ctrlDrbdProxyDisableApiCallHandlerRef;
        ctrlDrbdProxyModifyApiCallHandler = ctrlDrbdProxyModifyApiCallHandlerRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;

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
            );

            requestHelper.doFlux(
                ApiConsts.API_ENABLE_DRBD_PROXY,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
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
        );

        requestHelper.doFlux(
            ApiConsts.API_DISABLE_DRBD_PROXY,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux)
        );
    }

    @PUT
    public Response modifyProxy(
        @Context Request request,
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

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @GET
    @Path("properties/info")
    public Response listCtrlPropsInfo(
        @Context Request request
    )
    {
        Map<String, Map<String, JsonGenTypes.PropsInfo>> props = new HashMap<>();
        LinStorObject[] types =
        {
            LinStorObject.DRBD_PROXY, LinStorObject.DRBD_PROXY_LZMA,
            LinStorObject.DRBD_PROXY_ZLIB, LinStorObject.DRBD_PROXY_ZSTD, LinStorObject.DRBD_PROXY_LZ4
        };
        for (LinStorObject obj : types)
        {
            props.put(obj.name(), ctrlPropsInfoApiCallHandler.listFilteredProps(obj));
        }

        return requestHelper.doInScope(
            ApiConsts.API_LST_PROPS_INFO, request,
            () -> Response.status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(props))
                .build(),
            false
        );
    }
}
