package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoPlaceApiCallHandler;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/resource-definitions/{rscName}/autoplace")
public class AutoPlace
{
    private final RequestHelper requestHelper;
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public AutoPlace(
        RequestHelper requestHelperRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public void autoplace(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.AutoPlaceRequest autoPlaceRequest = objectMapper
                .readValue(jsonData, JsonGenTypes.AutoPlaceRequest.class);

            autoPlaceRequest.select_filter.diskless_on_remaining = autoPlaceRequest.diskless_on_remaining;
            autoPlaceRequest.select_filter.layer_stack = autoPlaceRequest.layer_list;

            Flux<ApiCallRc> flux = ctrlRscAutoPlaceApiCallHandler.autoPlace(
                rscName,
                new Json.AutoSelectFilterData(autoPlaceRequest.select_filter),
                autoPlaceRequest.copy_all_snaps != null && autoPlaceRequest.copy_all_snaps,
                autoPlaceRequest.snap_names
            );

            requestHelper.doFlux(
                ApiConsts.API_AUTO_PLACE_RSC,
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
}
