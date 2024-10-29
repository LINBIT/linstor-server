package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apis.ResourceConnectionApi;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_RSC_CONN, request, () ->
        {
            List<ResourceConnectionApi> rscConns = ctrlApiCallHandler.listResourceConnections(rscName);

            List<ResourceConnectionApi> filtered = rscConns.stream()
                .filter(rscConnApi -> nodeA == null || (rscConnApi.getSourceNodeName().equalsIgnoreCase(nodeA) &&
                    rscConnApi.getTargetNodeName().equalsIgnoreCase(nodeB)) ||
                    rscConnApi.getSourceNodeName().equalsIgnoreCase(nodeB) &&
                        rscConnApi.getTargetNodeName().equalsIgnoreCase(nodeA))
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
        )
        .contextWrite(requestHelper.createContext(ApiConsts.API_MOD_RSC_CONN, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
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
