package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupL2LDstApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlBackupL2LSrcApiCallHandler.BackupShippingData;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingReceiveRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingResponse;
import com.linbit.linstor.core.identifier.RemoteName;
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
    private final CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandler;
    private final ErrorReporter errorReporter;
    private final BackupInfoManager backupInfoMgr;

    private final ObjectMapper objectMapper;

    @Inject
    public BackupShippingInternal(
        RequestHelper requestHelperRef,
        CtrlBackupL2LDstApiCallHandler backupL2LDstApiCallHandlerRef,
        CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandlerRef,
        ErrorReporter errorReporterRef,
        BackupInfoManager backupInfoMgrRef
    )
    {
        requestHelper = requestHelperRef;
        backupL2LDstApiCallHandler = backupL2LDstApiCallHandlerRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        errorReporter = errorReporterRef;
        backupInfoMgr = backupInfoMgrRef;
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
                shipRequest.downloadOnly,
                shipRequest.srcL2LRemoteName
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
                    )
                )
            );
        }
        responses.single()
            .map(shipResponse ->
            {
                Response.ResponseBuilder builder = Response
                    .status(Response.Status.OK);
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
                        )
                    )
                );
                return Mono.just(
                    builder.entity(jsonResponse)
                        .type(MediaType.APPLICATION_JSON).build()
                );
            })
            .subscribe(asyncResponse::resume);

    }

    @POST
    @Path("requestReceive")
    public void internalRequestReceive(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        BackupShippingReceiveRequest receiveRequest;
        try
        {
            receiveRequest = objectMapper.readValue(jsonData, BackupShippingReceiveRequest.class);
            RemoteName remote = new RemoteName(receiveRequest.remoteName);
            BackupShippingData data = backupInfoMgr.getL2LSrcData(remote);
            backupInfoMgr.removeL2LSrcData(remote);
            responses = backupL2LSrcApiCallHandler.startShipping(receiveRequest, data)
                .subscriberContext(
                    requestHelper.createContext(InternalApiConsts.API_BACKUP_REST_START_RECEIVING, request)
                );
        }
        catch (JsonProcessingException exc)
        {
            responses = Flux.just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_INVLD_REQUEST,
                    "Failed to deserialize JSON",
                    exc.getMessage()
                )
            );
        }
        catch (InvalidNameException exc)
        {
            responses = Flux.just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_INVLD_REMOTE_NAME,
                    "Given RemoteName was invalid",
                    exc.getMessage()
                )
            );
        }
        ApiCallRcRestUtils.mapToMonoResponse(responses).subscribe(asyncResponse::resume);
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
