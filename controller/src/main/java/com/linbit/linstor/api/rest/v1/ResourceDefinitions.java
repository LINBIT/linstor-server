package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ResourceDefinition;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;

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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonParseException;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("v1/resource-definitions")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceDefinitions
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandler;
    private final CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    ResourceDefinitions(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandlerRef,
        CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandlerRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscDfnDeleteApiCallHandler = ctrlRscDfnDeleteApiCallHandlerRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listResourceDefinitions(
        @Context Request request,
        @QueryParam("resource_definitions") List<String> rscDfnNames,
        @QueryParam("with_volume_definitions") boolean withVlmDfns,
        @QueryParam("vlmNr") Integer vlmNr,
        @QueryParam("props") List<String> propFilters,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceDefinitionsOneOrMany(request, null, rscDfnNames, withVlmDfns, propFilters, limit, offset);
    }

    @GET
    @Path("{rscDfnName}")
    public Response listSingleResourceDefinition(
        @Context Request request,
        @PathParam("rscDfnName") String rscDfnName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceDefinitionsOneOrMany(
            request, rscDfnName, Collections.singletonList(rscDfnName), false, Collections.emptyList(), limit, offset);
    }

    private Response listResourceDefinitionsOneOrMany(
        Request request,
        String singleRscDfn,
        List<String> rscDfnNames,
        boolean withVlmDfn,
        List<String> propFilters,
        int limit,
        int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_RSC_DFN, request, () ->
        {
            Stream<ResourceDefinitionApi> rscDfnApiStream =
                ctrlApiCallHandler.listResourceDefinitions(rscDfnNames, propFilters).stream();
            if (limit > 0)
            {
                rscDfnApiStream = rscDfnApiStream.skip(offset).limit(limit);
            }
            List<ResourceDefinition> rscDfnDataList = rscDfnApiStream
                .map(rscDfnApi -> Json.apiToResourceDefinition(rscDfnApi, withVlmDfn))
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper, ApiConsts.FAIL_NOT_FOUND_RSC_DFN, "Resource definition", singleRscDfn, rscDfnDataList
            );
        }, false);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createResourceDefinition(@Context Request request, String jsonData)
    {
        return requestHelper.doInScope(
            ApiConsts.API_CRT_RSC_DFN,
            request,
            () ->
            {
                JsonGenTypes.ResourceDefinitionCreate rscDfnCreate = objectMapper.readValue(
                    jsonData,
                    JsonGenTypes.ResourceDefinitionCreate.class
                );

                List<JsonGenTypes.ResourceDefinitionLayer> layerDataList = rscDfnCreate.resource_definition.layer_data;
                // currently we ignore the possible payload, only extract the layer-stack

                byte[] externalNameBytes = rscDfnCreate.resource_definition.external_name != null ?
                    rscDfnCreate.resource_definition.external_name.getBytes(StandardCharsets.UTF_8) : null;

                LayerPayload payload = new LayerPayload();
                payload.drbdRscDfn.peerSlotsNewResource = rscDfnCreate.drbd_peer_slots == null ?
                    null : rscDfnCreate.drbd_peer_slots.shortValue();
                payload.drbdRscDfn.tcpPort = rscDfnCreate.drbd_port;
                if (rscDfnCreate.drbd_transport_type != null && !rscDfnCreate.drbd_transport_type.trim().isEmpty())
                {
                    try
                    {
                        payload.drbdRscDfn.transportType = TransportType.byValue(rscDfnCreate.drbd_transport_type);
                    }
                    catch (IllegalArgumentException unknownValueExc)
                    {
                        throw new JsonParseException(
                            "The given transport type '" + rscDfnCreate.drbd_transport_type + "' is invalid.",
                            unknownValueExc
                        );
                    }
                }
                payload.drbdRscDfn.sharedSecret = rscDfnCreate.drbd_secret;

                ApiCallRc apiCallRc = ctrlApiCallHandler.createResourceDefinition(
                    rscDfnCreate.resource_definition.name,
                    externalNameBytes,
                    rscDfnCreate.resource_definition.props,
                    new ArrayList<>(), // do not allow volume definition creations
                    layerDataList.stream().map(rscDfnData -> rscDfnData.type).collect(Collectors.toList()),
                    payload,
                    rscDfnCreate.resource_definition.resource_group_name
                );
                return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);
            },
            true
        );
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{rscName}")
    public void modifyResourceDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String jsonData
    )
        throws IOException
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.ResourceDefinitionModify modifyData =
                objectMapper.readValue(jsonData, JsonGenTypes.ResourceDefinitionModify.class);

            Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyRscDfn(
                    null,
                    rscName,
                    modifyData.drbd_port,
                    modifyData.override_props,
                    new HashSet<>(modifyData.delete_props),
                    new HashSet<>(modifyData.delete_namespaces),
                    modifyData.layer_stack,
                    modifyData.drbd_peer_slots == null ? null : modifyData.drbd_peer_slots.shortValue(),
                    modifyData.resource_group
                )
                .contextWrite(reactor.util.context.Context.of(InternalApiConsts.ONLY_WARN_IF_OFFLINE, Boolean.TRUE));

            requestHelper.doFlux(
                ApiConsts.API_MOD_RSC_DFN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
    }

    @DELETE
    @Path("{rscName}")
    public void deleteResourceDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName)
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Flux<ApiCallRc> flux = ctrlRscDfnDeleteApiCallHandler.deleteResourceDefinition(rscName);

            requestHelper.doFlux(
                ApiConsts.API_DEL_RSC_DFN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
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
                        .writeValueAsString(
                            ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.RSC_DFN)
                        )
                )
                .build(),
            false
        );
    }

    @POST
    @Path("{rscDfnName}/files/{path}")
    public void deployFile(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscDfnName") String rscName,
        @PathParam("path") String path
    )
    {
        try
        {
            Flux<ApiCallRc> flux = ctrlRscDfnApiCallHandler.setDeployFile(
                rscName,
                URLDecoder.decode(path, StandardCharsets.UTF_8.displayName()),
                true
            );
            requestHelper.doFlux(
                ApiConsts.API_DEPLOY_EXT_FILE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (UnsupportedEncodingException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @DELETE
    @Path("{rscDfnName}/files/{path}")
    public void undeployFile(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscDfnName") String rscName,
        @PathParam("path") String path
    )
    {
        try
        {
            Flux<ApiCallRc> flux = ctrlRscDfnApiCallHandler.setDeployFile(
                rscName,
                URLDecoder.decode(path, StandardCharsets.UTF_8.displayName()),
                false
            );
            requestHelper.doFlux(
                ApiConsts.API_UNDEPLOY_EXT_FILE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (UnsupportedEncodingException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Mono<Response> mapToCloneStarted(String srcName, String clonedName, Flux<ApiCallRc> flux)
    {
        return flux
            .collectList()
            .map(
                apiCallRcList ->
                {
                    Response.ResponseBuilder builder = Response.status(Response.Status.CREATED);
                    ApiCallRcImpl flatApiCallRc = new ApiCallRcImpl(
                        apiCallRcList.stream().flatMap(Collection::stream).collect(Collectors.toList())
                    );
                    try
                    {
                        builder.entity(
                            objectMapper.writeValueAsString(
                                Json.resourceDefCloneStarted(srcName, clonedName, flatApiCallRc)
                            )
                        );
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        return Response.serverError().build();
                    }
                    return builder.build();
                }
            );
    }

    @POST
    @Path("{rscDfnName}/clone")
    public void clone(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscDfnName") String srcName,
        String jsonData
    ) throws IOException
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.ResourceDefinitionCloneRequest requestData =
                objectMapper.readValue(jsonData, JsonGenTypes.ResourceDefinitionCloneRequest.class);

            Flux<ApiCallRc> flux = ctrlRscDfnApiCallHandler.cloneRscDfn(
                    srcName,
                    requestData.name,
                    requestData.external_name != null ?
                        requestData.external_name.getBytes(StandardCharsets.UTF_8) : null,
                    requestData.use_zfs_clone,
                    requestData.volume_passphrases,
                    requestData.layer_list
                );

            requestHelper.doFlux(
                ApiConsts.API_CLONE_RSCDFN,
                request,
                asyncResponse,
                mapToCloneStarted(srcName, requestData.name, flux)
            );
        }
    }

    @GET
    @Path("{rscDfnName}/clone/{cloneName}")
    public Response getCloneStatus(
        @Context Request request,
        @PathParam("rscDfnName") String srcName,
        @PathParam("cloneName") String cloneName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_CLONE_RSCDFN_STATUS, request, () ->
            {
                JsonGenTypes.ResourceDefinitionCloneStatus status =
                    new JsonGenTypes.ResourceDefinitionCloneStatus();

                status.status = ctrlApiCallHandler.isCloneReady(cloneName).getValue();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(status))
                    .build();
            }, false);
    }

    @GET
    @Path("{resource}/sync-status")
    public Response getSyncStatus(
        @Context Request request,
        @PathParam("resource") String rscName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_RSCDFN_SYNC_STATUS, request, () ->
        {
            JsonGenTypes.ResourceDefinitionSyncStatus status =
                new JsonGenTypes.ResourceDefinitionSyncStatus();

            status.synced_on_all = ctrlApiCallHandler.isResourceSynced(rscName);

            return Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(status))
                .build();
        }, false);
    }
}
