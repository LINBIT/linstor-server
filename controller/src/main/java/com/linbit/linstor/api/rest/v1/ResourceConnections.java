package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("resource-definitions/{rscName}/resource-connections")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceConnections
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public ResourceConnections(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;

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
            List<ResourceConnection.RscConnApi> rscConns = ctrlApiCallHandler.listResourceConnections(rscName);

            List<ResourceConnection.RscConnApi> filtered = rscConns.stream()
                .filter(rscConnApi -> nodeA == null || (rscConnApi.getSourceNodeName().equalsIgnoreCase(nodeA) &&
                    rscConnApi.getTargetNodeName().equalsIgnoreCase(nodeB)))
                .collect(Collectors.toList());

            Response resp;

            if (filtered.isEmpty())
            {
                ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        String.format("Resource connection between '%s' and '%s' not found.", nodeA, nodeB)
                    )
                );
                resp = Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(ApiCallRcConverter.toJSON(apiCallRc))
                    .build();
            }
            else
            {
                resp = Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(
                        filtered.stream().map(Json.ResourceConnectionData::new).collect(Collectors.toList())
                    ))
                    .build();
            }
            return resp;
        }, false);
    }

    @PUT
    @Path("{nodeA}/{nodeB}")
    public Response modifyResourceConnection(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_MOD_RSC_CONN, request, () ->
        {
            Json.ResourceConnectionModify rscConnModify = objectMapper.readValue(
                jsonData,
                Json.ResourceConnectionModify.class
            );
            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyRscConn(
                null,
                nodeA,
                nodeB,
                rscName,
                rscConnModify.override_props,
                rscConnModify.delete_props,
                rscConnModify.delete_namespaces
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

//    @DELETE
//    @Path("{nodeA}/{nodeB}")
//    public Response deleteResourceConnection(
//        @Context Request request,
//        @PathParam("rscName") String rscName,
//        @PathParam("nodeA") String nodeA,
//        @PathParam("nodeB") String nodeB
//    )
//    {
//    }

}
