package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.Node;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.NodeRestore;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeLostApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.SatelliteConfigApi;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/nodes")
@Produces(MediaType.APPLICATION_JSON)
public class Nodes
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlNodeApiCallHandler ctrlNodeApiCallHandler;
    private final CtrlNodeCrtApiCallHandler ctrlNodeCrtApiCallHandler;
    private final CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandler;
    private final CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandler;
    private final ObjectMapper objectMapper;
    private final ErrorReporter errorReporter;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    Nodes(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlNodeApiCallHandler ctrlNodeApiCallHandlerRef,
        CtrlNodeCrtApiCallHandler ctrlNodeCrtApiCallHandlerRef,
        CtrlNodeDeleteApiCallHandler ctrlNodeDeleteApiCallHandlerRef,
        CtrlNodeLostApiCallHandler ctrlNodeLostApiCallHandlerRef,
        ErrorReporter errorReporterRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlNodeApiCallHandler = ctrlNodeApiCallHandlerRef;
        ctrlNodeCrtApiCallHandler = ctrlNodeCrtApiCallHandlerRef;
        ctrlNodeDeleteApiCallHandler = ctrlNodeDeleteApiCallHandlerRef;
        ctrlNodeLostApiCallHandler = ctrlNodeLostApiCallHandlerRef;
        errorReporter = errorReporterRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listNodes(
        @Context Request request,
        @QueryParam("nodes") List<String> nodeNames,
        @QueryParam("props") List<String> propFilters,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listNodesOneOrMany(request, null, nodeNames, propFilters, limit, offset);
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
        return listNodesOneOrMany(
                request, nodeName, Collections.singletonList(nodeName), Collections.emptyList(), limit, offset);
    }

    private Response listNodesOneOrMany(
        Request request,
        @Nullable String searchNodeName,
        List<String> nodeNames,
        List<String> propFilters,
        int limit,
        int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_NODE, request, () ->
        {
            Stream<NodeApi> nodeApiStream = ctrlApiCallHandler.listNodes(nodeNames, propFilters).stream();
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.Node data = objectMapper.readValue(jsonData, JsonGenTypes.Node.class);
            Flux<ApiCallRc> flux = ctrlNodeCrtApiCallHandler
                .createNode(
                    data.name,
                    data.type,
                    data.net_interfaces.stream().map(Json::netInterfacetoApi).collect(Collectors.toList()),
                    data.props
                );

            requestHelper.doFlux(
                ApiConsts.API_CRT_NODE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @POST
    @Path("ebs")
    public void createEbsNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.NodeCreateEbs data = objectMapper.readValue(jsonData, JsonGenTypes.NodeCreateEbs.class);
            Flux<ApiCallRc> flux = ctrlNodeCrtApiCallHandler.createEbsNode(
                data.name,
                data.ebs_remote_name
            );

            requestHelper.doFlux(
                ApiConsts.API_CRT_NODE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.NodeModify modifyData = objectMapper.readValue(jsonData, JsonGenTypes.NodeModify.class);

            Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyNode(
                null,
                nodeName,
                modifyData.node_type,
                modifyData.override_props,
                new HashSet<>(modifyData.delete_props),
                new HashSet<>(modifyData.delete_namespaces)
            );

            requestHelper.doFlux(
                ApiConsts.API_MOD_NODE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
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
            .deleteNode(nodeName);

        requestHelper.doFlux(
            ApiConsts.API_DEL_NODE,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux)
        );
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
            .lostNode(nodeName);

        requestHelper.doFlux(
            ApiConsts.API_LOST_NODE,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux)
        );
    }

    @PUT
    @Path("{nodeName}/reconnect")
    public void reconnectNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName
    )
    {
        List<String> nodes = new ArrayList<>();
        nodes.add(nodeName);
        Flux<ApiCallRc> flux = ctrlNodeApiCallHandler.reconnectNode(nodes);

        requestHelper.doFlux(
            ApiConsts.API_NODE_RECONNECT,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }

    @PUT
    @Path("{nodeName}/restore")
    public void restoreNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.NodeRestore data;
            if (jsonData != null && !jsonData.isEmpty())
            {
                data = objectMapper.readValue(jsonData, JsonGenTypes.NodeRestore.class);
            }
            else
            {
                // values will be uninitialized / null -> will not delete resources or snapshots
                data = new NodeRestore();
            }
            final Flux<ApiCallRc> flux = ctrlNodeApiCallHandler
                .restoreNode(
                    nodeName,
                    data.delete_resources != null && data.delete_resources,
                    data.delete_snapshots != null && data.delete_snapshots
                );

            requestHelper.doFlux(
                ApiConsts.API_NODE_RESTORE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Path("{nodeName}/evict")
    public void evictNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName
    )
    {
        final Flux<ApiCallRc> flux = ctrlNodeApiCallHandler.evictNode(nodeName);
        requestHelper.doFlux(
            ApiConsts.API_NODE_EVICT,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }

    @PUT
    @Path("{nodeName}/evacuate")
    public void evacuateNode(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName
    )
    {
        final Flux<ApiCallRc> flux = ctrlNodeApiCallHandler.evacuateNode(nodeName);
        requestHelper.doFlux(
            ApiConsts.API_NODE_EVACUATE,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
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
        @PathParam("nodeName") @Nullable String nodeName,
        @PathParam("netif") @Nullable String netInterfaceName,
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
            List<NodeApi> nodes = ctrlApiCallHandler.listNodes(nodeNameList, Collections.emptyList());
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
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);
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

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
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

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @GET
    @Path("{nodeName}/config")
    public Response getStltConfig(
        @Context
        Request request,
        @PathParam("nodeName")
        String nodeName
    )
    {
        return requestHelper.doInScope(
            InternalApiConsts.API_LST_STLT_CONFIG, request,
            () ->
            {
                JsonGenTypes.SatelliteConfig stltConfig = new JsonGenTypes.SatelliteConfig();

                Response resp;
                try
                {
                    StltConfig stltConf = ctrlNodeApiCallHandler.getConfig(nodeName);
                    if (stltConf == null)
                    {
                        ApiCallRc rc = ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.WARN_NOT_CONNECTED | ApiConsts.MASK_NODE,
                            "Node is offline"
                        );
                        resp = Response
                            .status(Response.Status.SERVICE_UNAVAILABLE)
                            .entity(ApiCallRcRestUtils.toJSON(rc))
                            .build();
                    }
                    else
                    {
                        // Do NOT expose stltConf.getWhitelistedExternalFilePaths() due to security reasons!
                        stltConfig.config = new JsonGenTypes.ControllerConfigConfig();
                        stltConfig.config.dir = stltConf.getConfigDir();
                        stltConfig.debug = new JsonGenTypes.ControllerConfigDebug();
                        stltConfig.debug.console_enabled = stltConf.isDebugConsoleEnabled();
                        stltConfig.log = new JsonGenTypes.SatelliteConfigLog();
                        stltConfig.log.print_stack_trace = stltConf.isLogPrintStackTrace();
                        stltConfig.log.directory = stltConf.getLogDirectory();
                        stltConfig.log.level = stltConf.getLogLevel();
                        stltConfig.log.level_linstor = stltConf.getLogLevelLinstor();
                        stltConfig.stlt_override_node_name = stltConf.getStltOverrideNodeName();
                        stltConfig.remote_spdk = stltConf.isRemoteSpdk();
                        stltConfig.ebs = stltConf.isEbs();
                        stltConfig.special_satellite = stltConf.isRemoteSpdk() || stltConf.isEbs();
                        stltConfig.net = new JsonGenTypes.SatelliteConfigNet();
                        stltConfig.net.bind_address = stltConf.getNetBindAddress();
                        stltConfig.net.port = stltConf.getNetPort();
                        stltConfig.net.com_type = stltConf.getNetType();

                        resp = Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(stltConfig))
                            .build();
                    }
                }
                catch (JsonProcessingException exc)
                {
                    errorReporter.reportError(exc);
                    resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                }
                catch (AccessDeniedException exc)
                {
                    errorReporter.reportError(exc);
                    resp = Response.status(Response.Status.UNAUTHORIZED).build();
                }
                return resp;
            },
            false
        );
    }

    private static class SatelliteConfigPojo implements SatelliteConfigApi
    {
        private final JsonGenTypes.SatelliteConfig config;

        SatelliteConfigPojo(JsonGenTypes.SatelliteConfig configRef)
        {
            config = configRef;
        }

        @Override
        public String getLogLevel()
        {
            return config.log.level;
        }

        @Override
        public String getLogLevelLinstor()
        {
            return config.log.level_linstor;
        }
    }

    @PUT
    @Path("{nodeName}/config")
    public void setConfig(
        @Context
        Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName")
        String nodeName,
        String jsonData
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            JsonGenTypes.SatelliteConfig config = objectMapper
                .readValue(jsonData, JsonGenTypes.SatelliteConfig.class);
            SatelliteConfigPojo conf = new SatelliteConfigPojo(config);
            flux = ctrlNodeApiCallHandler.setConfig(nodeName, conf);
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
        requestHelper.doFlux(
            InternalApiConsts.API_MOD_STLT_CONFIG,
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
            () ->
            {
                return Response.status(Response.Status.OK).entity(
                    objectMapper.writeValueAsString(ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.NODE))
                )
                .build();
            },
            false
        );
    }
}
