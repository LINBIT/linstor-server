package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.core.apicallhandler.controller.CtrlQueryMaxVlmSizeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityAutoPoolSelectorUtils;

import javax.inject.Inject;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Mono;

@Path("query-max-volume-size")
@Produces(MediaType.APPLICATION_JSON)
public class QueryMaxVlmSize
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;
    private final CtrlQueryMaxVlmSizeApiCallHandler ctrlQueryMaxVlmSizeApiCallHandler;

    @Inject
    public QueryMaxVlmSize(
        RequestHelper requestHelperRef,
        CtrlQueryMaxVlmSizeApiCallHandler ctrlQueryMaxVlmSizeApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlQueryMaxVlmSizeApiCallHandler = ctrlQueryMaxVlmSizeApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @OPTIONS
    public void queryMaxVlmSize(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            Json.AutoSelectFilterData selectFilterData = objectMapper.readValue(
                jsonData,
                Json.AutoSelectFilterData.class
            );

            Mono<Response> flux = ctrlQueryMaxVlmSizeApiCallHandler.queryMaxVlmSize(
                selectFilterData
            )
                .subscriberContext(requestHelper.createContext(ApiConsts.API_QRY_MAX_VLM_SIZE, request))
                .flatMap(apiCallRcWith ->
                {
                    Response resp;
                    if (apiCallRcWith.hasApiCallRc())
                    {
                        resp = ApiCallRcConverter.toResponse(
                            apiCallRcWith.getApiCallRc(),
                            Response.Status.INTERNAL_SERVER_ERROR
                        );
                    }
                    else
                    {
                        List<MaxVlmSizeCandidatePojo> maxVlmSizeCandidates = apiCallRcWith.getValue();
                        Json.MaxVolumeSizesData maxVolumeSizesData = new Json.MaxVolumeSizesData(maxVlmSizeCandidates);
                        maxVolumeSizesData.default_max_oversubscription_ratio =
                            FreeCapacityAutoPoolSelectorUtils.DEFAULT_MAX_OVERSUBSCRIPTION_RATIO;

                        try
                        {
                            resp = Response
                                .status(Response.Status.OK)
                                .entity(objectMapper.writeValueAsString(maxVolumeSizesData))
                                .build();
                        }
                        catch (JsonProcessingException exc)
                        {
                            exc.printStackTrace();
                            resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                        }
                    }

                    return Mono.just(resp);
                }).next();

            requestHelper.doFlux(asyncResponse, flux);
        }
        catch (IOException ioExc)
        {
            ApiCallRcConverter.handleJsonParseException(ioExc, asyncResponse);
        }
    }
}
