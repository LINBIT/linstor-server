package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoResponsePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("v1/queries")
@Produces(MediaType.APPLICATION_JSON)
public class Queries
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;
    private final CtrlRscGrpApiCallHandler ctrlRscGrpApiCallHandler;

    @Inject
    Queries(
        RequestHelper requestHelperRef,
        CtrlRscGrpApiCallHandler ctrlRscGrpApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlRscGrpApiCallHandler = ctrlRscGrpApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    @Path("resource-groups/query-all-size-info")
    public void resourceGroupsQueryAllSizeInfo(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        String jsonData
    )
        throws JsonProcessingException
    {
        String nonEmptyJsonData = jsonData == null || jsonData.isEmpty() ? "{}" : jsonData;
        JsonGenTypes.QueryAllSizeInfoRequest qasiReq = objectMapper.readValue(
            nonEmptyJsonData,
            JsonGenTypes.QueryAllSizeInfoRequest.class
        );
        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            MDC.put(ErrorReporter.LOGID, ErrorReporter.getNewLogId());
            Mono<Response> flux = ctrlRscGrpApiCallHandler.queryAllSizeInfo(Json.queryAllSizeInfoReqToPojo(qasiReq))
                .contextWrite(requestHelper.createContext(ApiConsts.API_QRY_ALL_SIZE_INFO, request))
                .onErrorResume(
                    ApiRcException.class,
                    apiExc -> Flux.just(
                        new QueryAllSizeInfoResponsePojo(null, apiExc.getApiCallRc())
                    )
                )
                .flatMap(queryAllSizeInfoResult ->
                {
                    Response resp;
                    JsonGenTypes.QueryAllSizeInfoResponse qasiResp = Json.pojoToQueryAllSizeInfoResp(
                        queryAllSizeInfoResult
                    );

                    try
                    {
                        resp = Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(qasiResp))
                            .build();
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return Mono.just(resp);
                })
                .next();
            requestHelper.doFlux(asyncResponse, flux);
        });
    }
}

