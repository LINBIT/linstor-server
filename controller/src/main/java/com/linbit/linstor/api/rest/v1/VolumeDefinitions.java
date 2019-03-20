package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.Json.VolumeDefinitionData;
import com.linbit.linstor.api.rest.v1.serializer.Json.VolumeDefinitionModifyData;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnModifyApiCallHandler;

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
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC_DFN, request), () ->
        {
            Optional<ResourceDefinition.RscDfnApi> foundRscDfn = ctrlApiCallHandler.listResourceDefinition().stream()
                .filter(rscDfnApi -> rscDfnApi.getResourceName().equalsIgnoreCase(rscName))
                .findFirst();

            Response response;
            if (foundRscDfn.isPresent())
            {
                List<VolumeDefinitionData> data = new ArrayList<>();
                for (VolumeDefinition.VlmDfnApi vlmDfnApi : foundRscDfn.get().getVlmDfnList().stream()
                        .filter(vlmDfnApi -> vlmNumber == null || vlmDfnApi.getVolumeNr().equals(vlmNumber))
                        .collect(Collectors.toList()))
                {
                    data.add(new VolumeDefinitionData(vlmDfnApi));
                }

                if (data.isEmpty())
                {
                    ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                            String.format("Volume definition '%d' not found.", vlmNumber)
                        )
                    );
                    response = Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(ApiCallRcConverter.toJSON(apiCallRc))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
                }
                else
                {
                    response = Response
                        .status(Response.Status.OK)
                        .entity(objectMapper.writeValueAsString(data))
                        .build();
                }
            }
            else
            {
                ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
                apiCallRc.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                        String.format("Resource definition '%s' not found.", rscName)
                    )
                );
                response = Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(ApiCallRcConverter.toJSON(apiCallRc))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
            }
            return response;
        }, false);
    }

    private static class VlmDfnCreationWithPayload implements VolumeDefinition.VlmDfnWtihCreationPayload
    {
        Json.VolumeDefinitionCreateData vlmCreateData;

        public VlmDfnCreationWithPayload(Json.VolumeDefinitionCreateData data)
        {
            vlmCreateData = data;
        }

        @Override
        public VolumeDefinition.VlmDfnApi getVlmDfn()
        {
            return vlmCreateData.volume_definition.toVlmDfnApi();
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
            Json.VolumeDefinitionCreateData vlmDfnData = objectMapper.readValue(
                dataJson,
                Json.VolumeDefinitionCreateData.class
            );
            List<VolumeDefinition.VlmDfnWtihCreationPayload> vlmList = new ArrayList<>();
            vlmList.add(new VlmDfnCreationWithPayload(vlmDfnData));
            ApiCallRc apiCallRc = ctrlApiCallHandler.createVlmDfns(rscName, vlmList);

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
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
            VolumeDefinitionModifyData vlmDfnData = objectMapper.readValue(dataJson, VolumeDefinitionModifyData.class);

            Flux<ApiCallRc> flux = ctrlVlmDfnModifyApiCallHandler.modifyVlmDfn(
                null,
                rscName,
                vlmNr,
                vlmDfnData.size_kib,
                vlmDfnData.override_props,
                vlmDfnData.delete_props)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_VLM_DFN, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
        }
        catch (IOException exc)
        {
            ApiCallRcConverter.handleJsonParseException(exc, asyncResponse);
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

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }
}
