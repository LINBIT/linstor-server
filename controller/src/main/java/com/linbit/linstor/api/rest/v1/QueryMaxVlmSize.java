package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlQueryMaxVlmSizeApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Mono;

@Path("v1/query-max-volume-size")
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
        throws JsonProcessingException
    {
        JsonGenTypes.AutoSelectFilter selectFilterData = objectMapper.readValue(
            jsonData,
            JsonGenTypes.AutoSelectFilter.class
        );

        Mono<Response> flux = ctrlQueryMaxVlmSizeApiCallHandler.queryMaxVlmSize(
            new Json.AutoSelectFilterData(selectFilterData)
        )
            .flatMap(apiCallRcWith ->
            {
                Response resp;
                if (apiCallRcWith.hasApiCallRc())
                {
                    resp = ApiCallRcRestUtils.toResponse(
                        apiCallRcWith.getApiCallRc(),
                        Response.Status.INTERNAL_SERVER_ERROR
                    );
                }
                else
                {
                    List<MaxVlmSizeCandidatePojo> maxVlmSizeCandidates = apiCallRcWith.getValue();
                    JsonGenTypes.MaxVolumeSizes maxVolumeSizesData =
                        Json.pojoToMaxVolumeSizes(maxVlmSizeCandidates);
                    maxVolumeSizesData.default_max_oversubscription_ratio =
                        LinStor.OVERSUBSCRIPTION_RATIO_DEFAULT;

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

        requestHelper.doFlux(
            ApiConsts.API_QRY_MAX_VLM_SIZE,
            request,
            asyncResponse,
            flux
        );
    }
}
