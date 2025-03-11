package com.linbit.linstor.api.rest.v1;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LDstApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.BackupShippingPrepareAbortRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveDoneRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingRequestPrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponse;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingSrcData;
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
    @Path("requestPrevSnap")
    public void internalRequestPrevSnap(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        Flux<BackupShippingResponsePrevSnap> response;
        BackupShippingRequestPrevSnap data;
        try
        {
            data = objectMapper.readValue(jsonData, BackupShippingRequestPrevSnap.class);
            response = backupL2LDstApiCallHandler.getPrevSnap(
                data.srcVersion,
                data.srcClusterId,
                data.dstRscName,
                data.srcSnapDfnUuids,
                data.dstNodeName
            )
                .contextWrite(
                    requestHelper.createContext(InternalApiConsts.API_BACKUP_REST_START_RECEIVING, request)
            );
        }
        catch (JsonProcessingException exc)
        {
            response = Flux.just(
                new BackupShippingResponsePrevSnap(
                    false,
                    null,
                    false,
                    null,
                    null,
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_INVLD_REQUEST,
                        "Failed to deserialize JSON",
                        exc.getMessage()
                    )
                )
            );
        }
        response.single()
            .map(shipResponse ->
            {
                Response.ResponseBuilder builder = Response
                    .status(Response.Status.OK);
                return builder.entity(prevSnapResponseToJson(shipResponse))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
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
                        .type(MediaType.APPLICATION_JSON)
                        .build()
                );
            })
            .subscribe(asyncResponse::resume);
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
                shipRequest.dstNodeName,
                shipRequest.dstNodeNetIfName,
                shipRequest.dstStorPool,
                shipRequest.storPoolRenameMap,
                shipRequest.dstRscGrp,
                shipRequest.useZstd,
                shipRequest.downloadOnly,
                shipRequest.forceRestore,
                shipRequest.srcL2LRemoteName, // linstorRemoteName, not StltRemoteName
                shipRequest.srcStltRemoteName,
                shipRequest.srcRscName,
                shipRequest.resetData,
                shipRequest.dstBaseSnapName,
                shipRequest.dstActualNodeName,
                shipRequest.forceRscGrp
            ).contextWrite(
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
        catch (Exception exc)
        {
            responses = Flux.error(exc);
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
            RemoteName linstorRemoteName = new RemoteName(receiveRequest.linstorRemoteName);
            RemoteName stltRemoteName = new RemoteName(receiveRequest.srcStltRemoteName, true);
            BackupShippingSrcData data = backupInfoMgr.getL2LSrcData(linstorRemoteName, stltRemoteName);
            responses = backupL2LSrcApiCallHandler.startShipping(receiveRequest, data)
                .contextWrite(
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

    @POST
    @Path("requestReceiveDone")
    public void internalRequestReceiveDone(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        BackupShippingReceiveDoneRequest receiveRequest;
        try
        {
            receiveRequest = objectMapper.readValue(jsonData, BackupShippingReceiveDoneRequest.class);
            RemoteName linstorRemoteName = new RemoteName(receiveRequest.linstorRemoteName);
            RemoteName stltRemoteName = new RemoteName(receiveRequest.stltRemoteName, true);
            BackupShippingSrcData data = backupInfoMgr.removeL2LSrcData(linstorRemoteName, stltRemoteName);
            // srcSuccessRef is always true, regardless of actual success - since this is only important for the src
            // (aka backup-send)
            responses = backupL2LSrcApiCallHandler.startQueueIfReady(data.getStltRemote(), true, false)
                .contextWrite(
                    requestHelper.createContext(InternalApiConsts.API_BACKUP_REST_RECEIVING_DONE, request)
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

    @POST
    @Path("requestPrepareAbort")
    public void internalRequestPrepareAbort(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        BackupShippingPrepareAbortRequest prepareAbortRequest;
        try
        {
            prepareAbortRequest = objectMapper.readValue(jsonData, BackupShippingPrepareAbortRequest.class);
            responses = backupL2LDstApiCallHandler.prepareForAbort(prepareAbortRequest);
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
        requestHelper.doFlux(
            InternalApiConsts.API_BACKUP_REST_PREPARE_ABORT,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(
                responses,
                Response.Status.OK
            )
        );
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

    private String prevSnapResponseToJson(BackupShippingResponsePrevSnap responseRef)
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
