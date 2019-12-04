package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ResourceDefinition;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("resource-definitions")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceDefinitions
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    ResourceDefinitions(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscDfnDeleteApiCallHandler ctrlRscDfnDeleteApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscDfnDeleteApiCallHandler = ctrlRscDfnDeleteApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listResourceDefinitions(
        @Context Request request,
        @QueryParam("resource_definitions") List<String> rscDfnNames,
        @QueryParam("with_volume_definitions") boolean withVlmDfns,
        @QueryParam("vlmNr") Integer vlmNr,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceDefinitionsOneOrMany(request, null, rscDfnNames, withVlmDfns, limit, offset);
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
            request, rscDfnName, Collections.singletonList(rscDfnName), false, limit, offset
        );
    }

    private Response listResourceDefinitionsOneOrMany(
        Request request,
        String singleRscDfn,
        List<String> rscDfnNames,
        boolean withVlmDfn,
        int limit,
        int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC_DFN, request), () ->
        {
            Stream<ResourceDefinitionApi> rscDfnApiStream =
                ctrlApiCallHandler.listResourceDefinitions(rscDfnNames).stream();
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
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_CRT_RSC_DFN, request), () ->
        {
            JsonGenTypes.ResourceDefinitionCreate rscDfnCreate = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ResourceDefinitionCreate.class
            );
//            final List<VolumeDefinitionData> vlmDfns =
//                rscDfnCreate.resource_definition.volume_definitions != null ?
//                    rscDfnCreate.resource_definition.volume_definitions : new ArrayList<>();

            List<JsonGenTypes.ResourceDefinitionLayer> layerDataList = rscDfnCreate.resource_definition.layer_data;
            // currently we ignore the possible payload, only extract the layer-stack

            byte[] externalNameBytes = rscDfnCreate.resource_definition.external_name != null ?
                rscDfnCreate.resource_definition.external_name.getBytes(StandardCharsets.UTF_8) : null;


            ApiCallRc apiCallRc = ctrlApiCallHandler.createResourceDefinition(
                rscDfnCreate.resource_definition.name,
                externalNameBytes,
                rscDfnCreate.drbd_port,
                rscDfnCreate.drbd_secret,
                rscDfnCreate.drbd_transport_type,
                rscDfnCreate.resource_definition.props,
                new ArrayList<>(), // do not allow volume definition creations
                layerDataList.stream().map(rscDfnData -> rscDfnData.type).collect(Collectors.toList()),
                rscDfnCreate.drbd_peer_slots == null ? null : rscDfnCreate.drbd_peer_slots.shortValue(),
                rscDfnCreate.resource_definition.resource_group_name
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
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
            modifyData.drbd_peer_slots == null ? null : modifyData.drbd_peer_slots.shortValue()
        )
        .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_RSC_DFN, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux, Response.Status.OK));
    }

    @DELETE
    @Path("{rscName}")
    public void deleteResourceDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName)
    {
        Flux<ApiCallRc> flux = ctrlRscDfnDeleteApiCallHandler.deleteResourceDefinition(rscName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_DEL_RSC_DFN, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }
}
