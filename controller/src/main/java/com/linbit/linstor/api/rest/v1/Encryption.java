package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlConfApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/encryption")
public class Encryption
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;

    @Inject
    public Encryption(
        RequestHelper requestHelperRef,
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    @Path("passphrase")
    public Response passphraseStatus(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_STATUS_CRYPT_PASS,
            request,
            () ->
            {
                JsonGenTypes.PassphraseStatus passStatus = new JsonGenTypes.PassphraseStatus();
                passStatus.status = ctrlConfApiCallHandler.masterPassphraseStatus().toString().toLowerCase();

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(passStatus))
                    .build();
            },
            false
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("passphrase")
    public void createPassphrase(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.PassPhraseCreate passPhraseCreate = objectMapper
                .readValue(jsonData, JsonGenTypes.PassPhraseCreate.class);

            Flux<ApiCallRc> flux = ctrlConfApiCallHandler.setPassphrase(
                passPhraseCreate.new_passphrase,
                null
            );

            requestHelper.doFlux(
                ApiConsts.API_CRT_CRYPT_PASS,
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

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("passphrase")
    public void modifyPassphrase(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.PassPhraseCreate passPhraseCreate = objectMapper
                .readValue(jsonData, JsonGenTypes.PassPhraseCreate.class);

            Flux<ApiCallRc> flux = ctrlConfApiCallHandler.setPassphrase(
                passPhraseCreate.new_passphrase,
                passPhraseCreate.old_passphrase
            );

            requestHelper.doFlux(
                ApiConsts.API_MOD_CRYPT_PASS,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PATCH
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("passphrase")
    public void enterPassphrase(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            String passPhrase = objectMapper.readValue(jsonData, String.class);
            Flux<ApiCallRc> flux = ctrlConfApiCallHandler.enterPassphrase(passPhrase);

            requestHelper.doFlux(
                ApiConsts.API_ENTER_CRYPT_PASS,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }
}
