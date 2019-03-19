package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeDeleteApiCallHandler;
import com.linbit.linstor.api.rest.v1.serializer.Json.NodeData;
import com.linbit.linstor.api.rest.v1.serializer.Json.NodeModifyData;
import com.linbit.linstor.api.rest.v1.serializer.Json.NetInterfaceData;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeLostApiCallHandler;

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
import java.util.ArrayList;
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
    private final CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandler;
    private final CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    Nodes(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandlerRef,
        CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlNodeDeleteApiCallHandler = ctrlNodeDeleteApiCallHandlerRef;
        ctrlNodeLostApiCallHandler = ctrlNodeLostApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listNodes(
        @Context Request request,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listNodes(request, null, limit, offset);
    }

    @GET
    @Path("{nodeName}")
    public Response listNodes(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_NODE, request), () ->
        {
            Stream<Node.NodeApi> nodeApiStream = ctrlApiCallHandler.listNode().stream()
                .filter(nodeApi -> nodeName == null || nodeApi.getName().equalsIgnoreCase(nodeName));

            if (limit > 0)
            {
                nodeApiStream = nodeApiStream.skip(offset).limit(limit);
            }
            final List<NodeData> nds = nodeApiStream.map(nodeApi ->
                {
                    NodeData nd = new NodeData();
                    nd.name = nodeApi.getName();
                    nd.type = nodeApi.getType();
                    nd.connection_status = nodeApi.connectionStatus().toString();
                    nd.props = nodeApi.getProps();
                    nd.flags = Node.NodeFlag.toStringList(nodeApi.getFlags());
                    for (NetInterface.NetInterfaceApi netif : nodeApi.getNetInterfaces())
                    {
                        nd.net_interfaces.add(new NetInterfaceData(netif));
                    }
                    return nd;
                })
                .collect(Collectors.toList());

            Response resp;
            if (nds.isEmpty() && nodeName != null)
            {
                resp = Response.status(Response.Status.NOT_FOUND).build();
            }
            else
            {
                resp = Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(nds))
                    .build();
            }

            return resp;
        }, false);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createNode(@Context Request request, String jsonData)
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_CRT_NODE, request), () ->
        {
            NodeData data = objectMapper.readValue(jsonData, NodeData.class);
            ApiCallRc apiCallRc = ctrlApiCallHandler.createNode(
                data.name,
                data.type,
                data.net_interfaces.stream().map(NetInterfaceData::toApi).collect(Collectors.toList()),
                data.props
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{nodeName}")
    public Response modifyNode(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        String jsonData
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_MOD_NODE, request), () ->
        {
            NodeModifyData modifyData = objectMapper.readValue(jsonData, NodeModifyData.class);
            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyNode(
                null,
                nodeName,
                modifyData.node_type,
                modifyData.override_props,
                modifyData.delete_props,
                modifyData.delete_namespaces
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
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
            List<Node.NodeApi> nodes =  ctrlApiCallHandler.listNode();
            Optional<Node.NodeApi> optNode = nodes.stream()
                .filter(nodeApi -> nodeApi.getName().equalsIgnoreCase(nodeName))
                .findFirst();

            Response resp;
            if (optNode.isPresent())
            {
                Stream<NetInterface.NetInterfaceApi> netIfApiStream = optNode.get().getNetInterfaces()
                    .stream()
                    .filter(netInterfaceApi -> netInterfaceName == null ||
                        netInterfaceApi.getName().equalsIgnoreCase(netInterfaceName));

                if (limit > 0)
                {
                    netIfApiStream = netIfApiStream.skip(offset).limit(limit);
                }

                ArrayList<NetInterfaceData> netIfs = new ArrayList<>();
                for (NetInterface.NetInterfaceApi netif : netIfApiStream.collect(Collectors.toList()))
                {
                    netIfs.add(new NetInterfaceData(netif));
                }

                if (netInterfaceName != null && netIfs.isEmpty())
                {
                    resp = Response.status(Response.Status.NOT_FOUND).build();
                }
                else
                {
                    resp = Response.status(Response.Status.OK)
                        .entity(objectMapper.writeValueAsString(netIfs))
                        .build();
                }
            }
            else
            {
                resp = Response.status(Response.Status.NOT_FOUND).build();
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
            NetInterfaceData netInterfaceData = objectMapper.readValue(jsonData, NetInterfaceData.class);

            ApiCallRc apiCallRc = ctrlApiCallHandler.createNetInterface(
                nodeName,
                netInterfaceData.name,
                netInterfaceData.address,
                netInterfaceData.satellite_port,
                netInterfaceData.satellite_encryption_type
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
            NetInterfaceData netInterfaceData = objectMapper.readValue(jsonData, NetInterfaceData.class);

            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyNetInterface(
                nodeName,
                netIfName,
                netInterfaceData.address,
                netInterfaceData.satellite_port,
                netInterfaceData.satellite_encryption_type
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
