package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnDeleteApiCallHandler;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceDefinitionData;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceDefinitionModifyData;
import com.linbit.linstor.api.rest.v1.serializer.Json.VolumeDefinitionData;

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
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceDefinitions(request, null, limit, offset);
    }

    @GET
    @Path("{rscName}")
    public Response listResourceDefinitions(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC_DFN, request), () ->
        {
            Stream<ResourceDefinition.RscDfnApi> rscDfnStream =
                ctrlApiCallHandler.listResourceDefinition().stream()
                    .filter(rscDfnApi -> rscName == null || rscDfnApi.getResourceName().equalsIgnoreCase(rscName));
            if (limit > 0)
            {
                rscDfnStream = rscDfnStream.skip(offset).limit(limit);
            }
            final List<ResourceDefinitionData> rscDfns = rscDfnStream.map(apiRscDfn ->
            {
                ResourceDefinitionData data = new ResourceDefinitionData();
                data.name = apiRscDfn.getResourceName();
                data.port = apiRscDfn.getPort();
                data.props = apiRscDfn.getProps();
                data.secret = apiRscDfn.getSecret();
                data.flags = ResourceDefinition.RscDfnFlags.toStringList(apiRscDfn.getFlags());
                data.is_down = apiRscDfn.isDown();
                return data;
            })
                .collect(Collectors.toList());

            return Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(rscDfns))
                .build();
        }, false);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createResourceDefinition(@Context Request request, String jsonData)
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_CRT_RSC_DFN, request), () ->
        {
            ResourceDefinitionData rscDfnData = objectMapper.readValue(jsonData, ResourceDefinitionData.class);
            final List<VolumeDefinitionData> vlmDfns =
                rscDfnData.volume_definitions != null ? rscDfnData.volume_definitions : new ArrayList<>();
            ApiCallRc apiCallRc = ctrlApiCallHandler.createResourceDefinition(
                rscDfnData.name,
                rscDfnData.port,
                rscDfnData.secret,
                null, // will be removed
                rscDfnData.props,
                vlmDfns.stream()
                    .map(VolumeDefinitionData::toVlmDfnApi)
                    .collect(Collectors.toList())
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{rscName}")
    public Response modifyResourceDefinition(
        @Context Request request,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_MOD_RSC_DFN, request), () ->
        {
            ResourceDefinitionModifyData modifyData =
                objectMapper.readValue(jsonData, ResourceDefinitionModifyData.class);
            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyRscDfn(
                null,
                rscName,
                modifyData.port,
                modifyData.override_props,
                modifyData.delete_props,
                modifyData.delete_namespaces
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
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
