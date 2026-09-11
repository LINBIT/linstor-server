package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apis.ResourceConnectionApi;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/resource-definitions/{rscName}/resource-connections")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceConnections
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    public ResourceConnections(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listResourceConnections(
        @Context Request request,
        @PathParam("rscName") String rscName
    )
    {
        return listResourceConnections(request, rscName, null, null);
    }

    @GET
    @Path("{nodeA}/{nodeB}")
    public Response listResourceConnections(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("nodeA") @Nullable String nodeA,
        @PathParam("nodeB") @Nullable String nodeB
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_RSC_CONN, request, () ->
        {
            List<ResourceConnectionApi> rscConns = ctrlApiCallHandler.listResourceConnections(rscName);

            List<ResourceConnectionApi> filtered = rscConns.stream()
                .filter(rscConnApi -> nodeA == null || (rscConnApi.getSourceNodeName().equalsIgnoreCase(nodeA) &&
                    rscConnApi.getTargetNodeName().equalsIgnoreCase(nodeB)) ||
                    (rscConnApi.getSourceNodeName().equalsIgnoreCase(nodeB) &&
                        rscConnApi.getTargetNodeName().equalsIgnoreCase(nodeA)))
                .collect(Collectors.toList());

            Response resp;

            if (nodeA != null && filtered.isEmpty())
            {
                resp = RequestHelper.notFoundResponse(
                    ApiConsts.FAIL_NOT_FOUND_RSC_CONN,
                    String.format("Resource connection between '%s' and '%s' not found.", nodeA, nodeB)
                );
            }
            else
            {
                List<JsonGenTypes.ResourceConnection> resList = filtered.stream()
                    .map(Json::apiToResourceConnection)
                    .collect(Collectors.toList());
                resp = Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(nodeA != null ? resList.get(0) : resList))
                    .build();
            }
            return resp;
        }, false);
    }

    @PUT
    @Path("{nodeA}/{nodeB}")
    public void modifyResourceConnection(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB,
        String jsonData
    )
        throws IOException
    {
        JsonGenTypes.ResourceConnectionModify rscConnModify = objectMapper.readValue(
            jsonData,
            JsonGenTypes.ResourceConnectionModify.class
        );

        Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyRscConn(
            null,
            nodeA,
            nodeB,
            rscName,
            rscConnModify.override_props,
            new HashSet<>(rscConnModify.delete_props),
            new HashSet<>(rscConnModify.delete_namespaces)
        );

        requestHelper.doFlux(
            ApiConsts.API_MOD_RSC_CONN,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }

    @GET
    @Path("properties/info")
    public Response listCtrlPropsInfo(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_PROPS_INFO, request,
            () -> Response.status(Response.Status.OK)
                .entity(
                    objectMapper
                        .writeValueAsString(ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.RSC_CONN))
                )
                .build(),
            false
        );
    }
}
