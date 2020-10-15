package com.linbit.linstor.api.rest.v1.utils;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ApiCallRcRestUtils
{
    private static final long MIN_NOT_FOUND = ApiConsts.FAIL_NOT_FOUND_NODE & ~ApiConsts.MASK_ERROR;
    private static final long MAX_NOT_FOUND = 399;
    private static final long MIN_ACC_DENIED = ApiConsts.FAIL_ACC_DENIED_NODE & ~ApiConsts.MASK_ERROR;
    private static final long MAX_ACC_DENIED = 499;

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

    public static String toJSON(ApiCallRc apiCallRc)
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

    public static Response toResponse(ApiCallRc apiCallRc, Response.Status successStatus)
    {
        Response.Status status = successStatus;
        HashMap<String, String> header = new HashMap<>();

        for (ApiCallRc.RcEntry rc : apiCallRc.getEntries())
        {
            if ((ApiConsts.MASK_ERROR & rc.getReturnCode()) == ApiConsts.MASK_ERROR)
            {
                long strippedCode = rc.getReturnCode() & ApiConsts.MASK_BITS_CODE;
                if (strippedCode >= MIN_NOT_FOUND &&
                    strippedCode <= MAX_NOT_FOUND)
                {
                    status = Response.Status.NOT_FOUND;
                }
                else
                if (rc.getReturnCode() == ApiConsts.FAIL_SIGN_IN ||
                    strippedCode >= MIN_ACC_DENIED &&
                    strippedCode <= MAX_ACC_DENIED)
                {
                    status = Response.Status.FORBIDDEN;
                }
                else
                if (rc.getReturnCode() == ApiConsts.FAIL_SIGN_IN_MISSING_CREDENTIALS)
                {
                    status = Response.Status.UNAUTHORIZED;
                    header.put("WWW-Authenticate", "Basic realm=\"Linstor\"");
                }
                else
                {
                    status = Response.Status.INTERNAL_SERVER_ERROR;
                }
                break;
            }
        }

        Response.ResponseBuilder builder = Response.status(status);

        for (Map.Entry<String, String> entry : header.entrySet())
        {
            builder.header(entry.getKey(), entry.getValue());
        }

        return builder.entity(toJSON(apiCallRc)).type(MediaType.APPLICATION_JSON).build();
    }

    public static void handleJsonParseException(IOException ioexc, AsyncResponse asyncResponse)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(ApiConsts.API_CALL_PARSE_ERROR, "Unable to parse input json.")
                .setDetails(ioexc.getMessage())
                .build()
        );
        asyncResponse.resume(
            ApiCallRcRestUtils.toResponse(
                apiCallRc,
                Response.Status.BAD_REQUEST
            )
        );
    }

    public static Mono<Response> mapToMonoResponse(Flux<ApiCallRc> fluxApiCalls)
    {
        return mapToMonoResponse(fluxApiCalls, Response.Status.OK);
    }

    public static Mono<Response> mapToMonoResponse(Flux<ApiCallRc> fluxApiCalls, Response.Status status)
    {
        return fluxApiCalls
            .collectList()
            .map(apiCallRcList ->
                ApiCallRcRestUtils.toResponse(
                    new ApiCallRcImpl(apiCallRcList.stream().flatMap(
                        apiCallRc -> apiCallRc.getEntries().stream()
                    ).collect(Collectors.toList())),
                    status
                )
            );
    }

    private ApiCallRcRestUtils()
    {
    }
}
