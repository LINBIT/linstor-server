package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ApiCallRcConverter
{
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class ApiCallData
    {
        public long ret_code;
        public String message;
        public String cause;
        public String correction;
        public String details;
        public Set<String> error_report_ids;
        public Map<String, String> obj_refs;

        ApiCallData()
        {
        }

        ApiCallData(
            long retCodeRef,
            String messageRef,
            String causeRef,
            String correctionRef,
            String detailsRef,
            Map<String, String> objRefs,
            Set<String> errIds
            )
        {
            ret_code = retCodeRef;
            message = messageRef;
            cause = causeRef;
            correction = correctionRef;
            details = detailsRef;
            obj_refs = objRefs;
            error_report_ids = errIds;
        }
    }

    static String toJSON(ApiCallRc apiCallRc)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayList<ApiCallData> jsonData = new ArrayList<>();

        for (ApiCallRc.RcEntry rc : apiCallRc.getEntries())
        {
            jsonData.add(new ApiCallData(
                rc.getReturnCode(),
                rc.getMessage(),
                rc.getCause(),
                rc.getCorrection(),
                rc.getDetails(),
                rc.getObjRefs(),
                rc.getErrorIds()
            ));
        }

        String ret = null;
        try
        {
            ret = objectMapper.writeValueAsString(jsonData);
        }
        catch (JsonProcessingException exc)
        {
            exc.printStackTrace();
        }

        return ret;
    }

    static Response toResponse(ApiCallRc apiCallRc, Response.Status successStatus)
    {
        Response.Status status = successStatus;

        for (ApiCallRc.RcEntry rc : apiCallRc.getEntries())
        {
            if ((ApiConsts.MASK_ERROR & rc.getReturnCode()) == ApiConsts.MASK_ERROR)
            {
                status = Response.Status.INTERNAL_SERVER_ERROR;
                break;
            }
        }

        return Response.status(status).entity(toJSON(apiCallRc)).type(MediaType.APPLICATION_JSON).build();
    }

    static void handleJsonParseException(IOException ioexc, AsyncResponse asyncResponse)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.API_CALL_PARSE_ERROR, "Unable to parse input json.")
                .setDetails(ioexc.getMessage())
                .build()
        );
        asyncResponse.resume(
            ApiCallRcConverter.toResponse(
                apiCallRc,
                Response.Status.BAD_REQUEST
            )
        );
    }

    static Mono<Response> mapToMonoResponse(Flux<ApiCallRc> fluxApiCalls)
    {
        return mapToMonoResponse(fluxApiCalls, Response.Status.OK);
    }

    static Mono<Response> mapToMonoResponse(Flux<ApiCallRc> fluxApiCalls, Response.Status status)
    {
        return fluxApiCalls
            .collectList()
            .map(apiCallRcList ->
                ApiCallRcConverter.toResponse(
                    new ApiCallRcImpl(apiCallRcList.stream().flatMap(
                        apiCallRc -> apiCallRc.getEntries().stream()
                    ).collect(Collectors.toList())),
                    status
                )
            );
    }
}
