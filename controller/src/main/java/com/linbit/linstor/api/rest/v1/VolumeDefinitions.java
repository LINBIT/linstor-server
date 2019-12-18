package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnModifyApiCallHandler;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWtihCreationPayload;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("resource-definitions/{rscName}/volume-definitions")
@Produces(MediaType.APPLICATION_JSON)
public class VolumeDefinitions
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandler;
    private final CtrlVlmDfnDeleteApiCallHandler ctrlVlmDfnDeleteApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    VolumeDefinitions(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandlerRef,
        CtrlVlmDfnDeleteApiCallHandler ctrlVlmDfnDeleteApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlVlmDfnModifyApiCallHandler = ctrlVlmDfnModifyApiCallHandlerRef;
        ctrlVlmDfnDeleteApiCallHandler = ctrlVlmDfnDeleteApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listVolumeDefinition(
        @Context Request request,
        @PathParam("rscName") String rscName
        // TODO: use limit and offset (like ResourceDefinitions#listResourceDefinitions)
    )
    {
        return listVolumeDefinition(request, rscName, null);
    }

    @GET
    @Path("{vlmNr}")
    public Response listVolumeDefinition(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("vlmNr") Integer vlmNumber
        // TODO: use limit and offset (like ResourceDefinitions#listResourceDefinitions)
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC_DFN, request), () ->
        {
            Optional<ResourceDefinitionApi> foundRscDfn = ctrlApiCallHandler.listResourceDefinitions().stream()
                .filter(rscDfnApi -> rscDfnApi.getResourceName().equalsIgnoreCase(rscName))
                .findFirst();
            // TODO: instead of building a list of ALL rscDfns(Api) and filtering the one we are interested in
            // move this method into a new ctlrApiCallHandler.listVlmDfns(rscName) (which takes the readlock of rscDfnMap
            // and makes that one rscDfnMap.get(rscName) lookup

            Response response;
            if (foundRscDfn.isPresent())
            {
                List<JsonGenTypes.VolumeDefinition> data = new ArrayList<>();
                for (VolumeDefinitionApi vlmDfnApi : foundRscDfn.get().getVlmDfnList().stream()
                        .filter(vlmDfnApi -> vlmNumber == null || vlmDfnApi.getVolumeNr().equals(vlmNumber))
                        .collect(Collectors.toList()))
                {
                    data.add(Json.apiToVolumeDefinition(vlmDfnApi));
                }

                response = RequestHelper.queryRequestResponse(
                    objectMapper,
                    ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                    "Volume definition",
                    vlmNumber == null ? null : vlmNumber.toString(),
                    data
                );
            }
            else
            {
                response = RequestHelper.notFoundResponse(
                    ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                    String.format("Resource definition '%s' not found.", rscName)
                );
            }
            return response;
        }, false);
    }

    private static class VlmDfnCreationWithPayload implements VolumeDefinitionWtihCreationPayload
    {
        JsonGenTypes.VolumeDefinitionCreate vlmCreateData;

        VlmDfnCreationWithPayload(JsonGenTypes.VolumeDefinitionCreate data)
        {
            vlmCreateData = data;
        }

        @Override
        public VolumeDefinitionApi getVlmDfn()
        {
            return Json.volumeDefinitionToApi(vlmCreateData.volume_definition);
        }

        @Override
        public Integer getDrbdMinorNr()
        {
            return vlmCreateData.drbd_minor_number;
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolumeDefinition(
        @Context Request request,
        @PathParam("rscName") String rscName,
        String dataJson
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_CRT_VLM_DFN, request), () ->
        {
            JsonGenTypes.VolumeDefinitionCreate vlmDfnData = objectMapper.readValue(
                dataJson,
                JsonGenTypes.VolumeDefinitionCreate.class
            );
            List<VolumeDefinitionWtihCreationPayload> vlmList = new ArrayList<>();
            vlmList.add(new VlmDfnCreationWithPayload(vlmDfnData));
            ApiCallRc apiCallRc = ctrlApiCallHandler.createVlmDfns(rscName, vlmList);

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{vlmNr}")
    public void modifyVolumeDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("vlmNr") int vlmNr,
        String dataJson
    )
    {
        try
        {
            JsonGenTypes.VolumeDefinitionModify vlmDfnData = objectMapper
                .readValue(dataJson, JsonGenTypes.VolumeDefinitionModify.class);

            Flux<ApiCallRc> flux = ctrlVlmDfnModifyApiCallHandler.modifyVlmDfn(
                null,
                rscName,
                vlmNr,
                vlmDfnData.size_kib,
                vlmDfnData.override_props,
                new HashSet<>(vlmDfnData.delete_props))
                .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_VLM_DFN, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
        }
        catch (IOException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @DELETE
    @Path("{vlmNr}")
    public void deleteVolumeDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("vlmNr") int vlmNr
    )
    {
        Flux<ApiCallRc> flux = ctrlVlmDfnDeleteApiCallHandler.deleteVolumeDefinition(rscName, vlmNr)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_DEL_VLM_DFN, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
    }
}
