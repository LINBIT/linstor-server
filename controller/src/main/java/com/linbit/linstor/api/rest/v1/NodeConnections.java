package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeConnectionApiCallHandler;
import com.linbit.linstor.core.apis.NodeConnectionApi;

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

@Path("v1/node-connections")
@Produces(MediaType.APPLICATION_JSON)
public class NodeConnections
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlNodeConnectionApiCallHandler nodeConnHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public NodeConnections(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlNodeConnectionApiCallHandler nodeconHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        nodeConnHandler = nodeconHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listNodeConnections(
        @Context Request request
    )
    {
        return listNodeConnections(request, null, null);
    }

    @GET
    @Path("manage/{nodeA}/{nodeB}")
    public Response listNodeConnections(
        @Context Request request,
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_RSC_CONN, request, () ->
        {
            Response resp;

            List<NodeConnectionApi> nodeConns = nodeConnHandler.listNodeConnections(nodeA, nodeB);

            if (nodeA != null && nodeConns.isEmpty())
            {
                resp = RequestHelper.notFoundResponse(
                    ApiConsts.FAIL_NOT_FOUND_NODE_CONN,
                    String.format("Node connection between '%s' and '%s' not found.", nodeA, nodeB)
                );
            }
            else
            {
                List<JsonGenTypes.NodeConnection> nodeConList = nodeConns.stream()
                    .map(Json::apiToNodeConnection)
                    .collect(Collectors.toList());
                resp = Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(nodeA != null ? nodeConList.get(0) : nodeConList))
                    .build();
            }
            return resp;
        }, false);
    }

    @PUT
    @Path("manage/{nodeA}/{nodeB}")
    public void modifyNodeConnection(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeA") String nodeA,
        @PathParam("nodeB") String nodeB,
        String jsonData
    )
        throws IOException
    {
        JsonGenTypes.NodeConnectionModify nodeConnModify = objectMapper.readValue(
            jsonData,
            JsonGenTypes.NodeConnectionModify.class
        );

        Flux<ApiCallRc> flux = nodeConnHandler.modifyNodeConn(
            null,
            nodeA,
            nodeB,
            nodeConnModify.override_props,
            new HashSet<>(nodeConnModify.delete_props),
            new HashSet<>(nodeConnModify.delete_namespaces)
        )
            .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_NODE_CONN, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
    }
}
