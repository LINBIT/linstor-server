package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.Node;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeLostApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("nodes")
@Produces(MediaType.APPLICATION_JSON)
public class Nodes
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlNodeCrtApiCallHandler ctrlNodeCrtApiCallHandler;
    private final CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandler;
    private final CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    Nodes(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlNodeCrtApiCallHandler ctrlNodeCrtApiCallHandlerRef,
        CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandlerRef,
        CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlNodeCrtApiCallHandler = ctrlNodeCrtApiCallHandlerRef;
        ctrlNodeDeleteApiCallHandler = ctrlNodeDeleteApiCallHandlerRef;
        ctrlNodeLostApiCallHandler = ctrlNodeLostApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listNodes(
        @Context Request request,
        @QueryParam("nodes") List<String> nodeNames,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listNodesOneOrMany(request, null, nodeNames, limit, offset);
    }

    @GET
    @Path("{nodeName}")
    public Response listSingleNode(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listNodesOneOrMany(request, nodeName, Collections.singletonList(nodeName), limit, offset);
    }

    private Response listNodesOneOrMany(
        Request request,
        String searchNodeName,
        List<String> nodeNames,
        int limit,
        int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_NODE, request), () ->
        {
            Stream<NodeApi> nodeApiStream = ctrlApiCallHandler.listNodes(nodeNames).stream();
            if (limit > 0)
            {
                nodeApiStream = nodeApiStream.skip(offset).limit(limit);
            }
            List<Node> nodeDataList = nodeApiStream
                .map(Json::apiToNode)
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper, ApiConsts.FAIL_NOT_FOUND_NODE, "Node", searchNodeName, nodeDataList
            );
        }, false);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void createNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.Node data = objectMapper.readValue(jsonData, JsonGenTypes.Node.class);
            Flux<ApiCallRc> flux = ctrlNodeCrtApiCallHandler
                .createNode(
                    data.name,
                    data.type,
                    data.net_interfaces.stream().map(Json::netInterfacetoApi).collect(Collectors.toList()),
                    data.props
                )
                .subscriberContext(requestHelper.createContext(ApiConsts.API_CRT_NODE, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux, Response.Status.CREATED));
        }
        catch (IOException ioExc)
        {
            ApiCallRcConverter.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{nodeName}")
    public void modifyNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.NodeModify modifyData = objectMapper.readValue(jsonData, JsonGenTypes.NodeModify.class);

            Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyNode(
                null,
                nodeName,
                modifyData.node_type,
                modifyData.override_props,
                new HashSet<>(modifyData.delete_props),
                new HashSet<>(modifyData.delete_namespaces)
            )
            .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_NODE, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux, Response.Status.CREATED));
        }
        catch (IOException ioExc)
        {
            ApiCallRcConverter.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    @Path("{nodeName}")
    public void deleteNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName
    )
    {
        Flux<ApiCallRc> flux = ctrlNodeDeleteApiCallHandler
            .deleteNode(nodeName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_DEL_NODE, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }

    @DELETE
    @Path("{nodeName}/lost")
    public void lostNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName
    )
    {
        Flux<ApiCallRc> flux = ctrlNodeLostApiCallHandler
            .lostNode(nodeName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_LOST_NODE, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }

    @PUT
    @Path("{nodeName}/reconnect")
    public Response reconnectNode(
        @Context Request request,
        @PathParam("nodeName") String nodeName
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_NODE_RECONNECT, request), () ->
        {
            List<String> nodes = new ArrayList<>();
            nodes.add(nodeName);
            ApiCallRc apiCallRc = ctrlApiCallHandler.reconnectNode(nodes);

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, false);
    }

    @GET
    @Path("{nodeName}/net-interfaces")
    public Response listNetInterfaces(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listNetInterfaces(request, nodeName, null, limit, offset);
    }

    @GET
    @Path("{nodeName}/net-interfaces/{netif}")
    public Response listNetInterfaces(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @PathParam("netif") String netInterfaceName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_NET_IF, request, () ->
        {
            List<String> nodeNameList = new ArrayList<>(1);
            if (nodeName != null)
            {
                nodeNameList.add(nodeName);
            }
            List<NodeApi> nodes = ctrlApiCallHandler.listNodes(nodeNameList);
            Optional<NodeApi> optNode = nodes.stream()
                .filter(nodeApi -> nodeApi.getName().equalsIgnoreCase(nodeName))
                .findFirst();

            Response resp;
            if (optNode.isPresent())
            {
                Stream<NetInterfaceApi> netIfApiStream = optNode.get().getNetInterfaces()
                    .stream()
                    .filter(netInterfaceApi -> netInterfaceName == null ||
                        netInterfaceApi.getName().equalsIgnoreCase(netInterfaceName));

                if (limit > 0)
                {
                    netIfApiStream = netIfApiStream.skip(offset).limit(limit);
                }

                ArrayList<JsonGenTypes.NetInterface> netIfs = new ArrayList<>();
                for (NetInterfaceApi netif : netIfApiStream.collect(Collectors.toList()))
                {
                    JsonGenTypes.NetInterface netIfResp = Json.apiToNetInterface(netif);
                    if (netif.getUuid().equals(optNode.get().getActiveStltConn().getUuid()))
                    {
                        netIfResp.is_active = Boolean.TRUE;
                    }
                    netIfs.add(netIfResp);
                }

                resp = RequestHelper.queryRequestResponse(
                    objectMapper, ApiConsts.FAIL_NOT_FOUND_NET_IF, "Netinterface", netInterfaceName, netIfs
                );
            }
            else
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_NODE,
                        "Node '" + nodeName + "' not found."
                    )
                    .setCause("The specified node '" + nodeName + "' could not be found in the database")
                    .setCorrection("Create a node with the name '" + nodeName + "' first.")
                    .build()
                );
            }
            return resp;
        }, false);
    }

    @POST
    @Path("{nodeName}/net-interfaces")
    public Response createNetInterface(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_CRT_NET_IF, request, () ->
        {
            JsonGenTypes.NetInterface netInterface = objectMapper
                .readValue(jsonData, JsonGenTypes.NetInterface.class);

            ApiCallRc apiCallRc = ctrlApiCallHandler.createNetInterface(
                nodeName,
                netInterface.name,
                netInterface.address,
                netInterface.satellite_port,
                netInterface.satellite_encryption_type,
                netInterface.is_active
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    @PUT
    @Path("{nodeName}/net-interfaces/{netif}")
    public Response modifyNetInterface(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @PathParam("netif") String netIfName,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_MOD_NET_IF, request, () ->
        {
            JsonGenTypes.NetInterface netInterface = objectMapper
                .readValue(jsonData, JsonGenTypes.NetInterface.class);

            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyNetInterface(
                nodeName,
                netIfName,
                netInterface.address,
                netInterface.satellite_port,
                netInterface.satellite_encryption_type,
                netInterface.is_active
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @DELETE
    @Path("{nodeName}/net-interfaces/{netif}")
    public Response deleteNetInterface(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @PathParam("netif") String netIfName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_DEL_NET_IF, request, () ->
        {
            ApiCallRc apiCallRc = ctrlApiCallHandler.deleteNetInterface(
                nodeName,
                netIfName
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }
}
