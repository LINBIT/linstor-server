package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupL2LDstApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingResponse;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
 * Intentionally NOT mentioned in rest_v1_openapi.yaml
 * This API is for internal (ctrl <-> ctrl) usage only!
 */
@Path("v1/internal/backups")
@Produces(MediaType.APPLICATION_JSON)
public class BackupShippingInternal
{
    private final RequestHelper requestHelper;
    private final CtrlBackupL2LDstApiCallHandler backupL2LDstApiCallHandler;
    private final ErrorReporter errorReporter;

    private final ObjectMapper objectMapper;

    @Inject
    public BackupShippingInternal(
        RequestHelper requestHelperRef,
        CtrlBackupL2LDstApiCallHandler backupL2LDstApiCallHandlerRef,
        ErrorReporter errorReporterRef
    )
    {
        requestHelper = requestHelperRef;
        backupL2LDstApiCallHandler = backupL2LDstApiCallHandlerRef;
        errorReporter = errorReporterRef;
        objectMapper = new ObjectMapper();
    }

    @POST
    @Path("requestShip")
    public void internalRequestShip(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        Flux<BackupShippingResponse> responses;
        BackupShippingRequest shipRequest;
        try
        {
            // intentionally not a member of JsonGenTypes
            shipRequest = objectMapper.readValue(jsonData, BackupShippingRequest.class);
            responses = backupL2LDstApiCallHandler.startReceiving(
                shipRequest.srcVersion,
                shipRequest.dstRscName,
                shipRequest.metaData,
                shipRequest.srcBackupName,
                shipRequest.srcClusterId,
                shipRequest.srcSnapDfnUuids,
                shipRequest.dstNodeName,
                shipRequest.dstNodeNetIfName,
                shipRequest.dstStorPool,
                shipRequest.storPoolRenameMap,
                shipRequest.useZstd,
                shipRequest.downloadOnly
            ).subscriberContext(
                requestHelper.createContext(InternalApiConsts.API_BACKUP_REST_START_RECEIVING, request)
            );
        }
        catch (JsonProcessingException exc)
        {
            responses = Flux.just(
                new BackupShippingResponse(
                    false,
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_INVLD_REQUEST,
                        "Failed to deserialize JSON",
                        exc.getMessage()
                    ),
                    null,
                    null,
                    null,
                    null
                )
            );
        }
        responses.single()
            .map(shipResponse ->
            {
                Response.ResponseBuilder builder = Response
                    .status(shipResponse.canReceive ? Response.Status.OK : Response.Status.BAD_REQUEST);
                return builder.entity(responseToJson(shipResponse))
                    .type(MediaType.APPLICATION_JSON).build();
            })
            .onErrorResume(exc ->
            {
                String reportErrorId = errorReporter.reportError(exc);
                Response.ResponseBuilder builder = Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR);
                String jsonResponse = responseToJson(
                    new BackupShippingResponse(
                        false,
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            exc.getMessage(),
                            "ErrorReport id on target cluster: " + reportErrorId
                        ),
                        null,
                        null,
                        null,
                        null
                    )
                );
                return Mono.just(
                    builder.entity(jsonResponse)
                        .type(MediaType.APPLICATION_JSON).build()
                );
            })
            .subscribe(asyncResponse::resume);

    }

    private String responseToJson(BackupShippingResponse responseRef)
    {
        String json;
        try
        {
            json = objectMapper.writeValueAsString(responseRef);
        }
        catch (JsonProcessingException exc)
        {
            throw new ImplementationError(exc);
        }
        return json;
    }
}
