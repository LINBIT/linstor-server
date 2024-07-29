package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnModifyApiCallHandler;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;
import com.linbit.linstor.logging.ErrorReporter;

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
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/resource-definitions/{rscName}/volume-definitions")
@Produces(MediaType.APPLICATION_JSON)
public class VolumeDefinitions
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandler;
    private final CtrlVlmDfnDeleteApiCallHandler ctrlVlmDfnDeleteApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    VolumeDefinitions(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlVlmDfnModifyApiCallHandler ctrlVlmDfnModifyApiCallHandlerRef,
        CtrlVlmDfnDeleteApiCallHandler ctrlVlmDfnDeleteApiCallHandlerRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlVlmDfnModifyApiCallHandler = ctrlVlmDfnModifyApiCallHandlerRef;
        ctrlVlmDfnDeleteApiCallHandler = ctrlVlmDfnDeleteApiCallHandlerRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;
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
        @PathParam("vlmNr") @Nullable Integer vlmNumber
        // TODO: use limit and offset (like ResourceDefinitions#listResourceDefinitions)
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_RSC_DFN, request, () ->
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

    private static class VlmDfnCreationWithPayload implements VolumeDefinitionWithCreationPayload
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

        @Override
        public @Nullable String passphrase()
        {
            return vlmCreateData.passphrase;
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void createVolumeDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String dataJson
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.VolumeDefinitionCreate vlmDfnData = objectMapper.readValue(
                dataJson,
                JsonGenTypes.VolumeDefinitionCreate.class
            );

            List<VolumeDefinitionWithCreationPayload> vlmList = new ArrayList<>();
            vlmList.add(new VlmDfnCreationWithPayload(vlmDfnData));

            Flux<ApiCallRc> flux = ctrlApiCallHandler.createVlmDfns(rscName, vlmList);

            requestHelper.doFlux(
                ApiConsts.API_CRT_VLM_DFN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (IOException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.VolumeDefinitionModify vlmDfnData = objectMapper
                .readValue(dataJson, JsonGenTypes.VolumeDefinitionModify.class);

            Flux<ApiCallRc> flux = ctrlVlmDfnModifyApiCallHandler.modifyVlmDfn(
                null,
                rscName,
                vlmNr,
                vlmDfnData.size_kib,
                vlmDfnData.override_props,
                new HashSet<>(vlmDfnData.delete_props),
                vlmDfnData.flags
            );

            requestHelper.doFlux(
                ApiConsts.API_MOD_VLM_DFN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Flux<ApiCallRc> flux = ctrlVlmDfnDeleteApiCallHandler.deleteVolumeDefinition(rscName, vlmNr);

            requestHelper.doFlux(
                ApiConsts.API_DEL_VLM_DFN,
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
                            ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.VLM_DFN)
                        )
                )
                .build(),
            false
        );
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{vlmNr}/encryption-passphrase")
    public void modifyVolumeDefinitionPassphrase(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("vlmNr") int vlmNr,
        String dataJson
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.VolumeDefinitionModifyPassphrase vlmDfnData = objectMapper
                .readValue(dataJson, JsonGenTypes.VolumeDefinitionModifyPassphrase.class);

            Flux<ApiCallRc> flux = ctrlVlmDfnModifyApiCallHandler.modifyVlmDfnPassphrase(
                    rscName,
                    vlmNr,
                    vlmDfnData.new_passphrase
                );

            requestHelper.doFlux(
                ApiConsts.API_MOD_VLM_DFN_PASS,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux)
            );
        }
        catch (IOException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }
}
