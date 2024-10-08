package com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveDoneRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingReceiveRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingRequest;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingRequestPrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponse;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingResponsePrevSnap;
import com.linbit.linstor.core.apicallhandler.controller.backup.l2l.rest.data.BackupShippingSrcData;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Singleton
public class BackupShippingRestClient
{
    private static final int OK = Response.Status.OK.getStatusCode();
    private static final int NOT_FOUND = Response.Status.NOT_FOUND.getStatusCode();
    private static final int BAD_REQUEST = Response.Status.BAD_REQUEST.getStatusCode();
    private static final int INTERNAL_SERVER_ERROR = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
    private final BackupInfoManager backupInfoMgr;
    private final ErrorReporter errorReporter;
    private final RestHttpClient restClient;
    private final ObjectMapper objMapper;

    @Inject
    BackupShippingRestClient(ErrorReporter errorReporterRef, BackupInfoManager backupInfoMgrRef)
    {
        errorReporter = errorReporterRef;
        backupInfoMgr = backupInfoMgrRef;
        restClient = new RestHttpClient(errorReporterRef);
        objMapper = new ObjectMapper();
    }

    public Flux<BackupShippingResponsePrevSnap> sendPrevSnapRequest(
        BackupShippingRequestPrevSnap data,
        LinstorRemote remote,
        AccessContext accCtx
    )
    {
        return Flux.create(fluxSink ->
        {
            Runnable run = () ->
            {
                try
                {
                    String restURL = remote.getUrl(accCtx).toExternalForm() +
                        "/v1/internal/backups/requestPrevSnap";
                    RestResponse<BackupShippingResponsePrevSnap> response = restClient.execute(
                        null,
                        RestOp.POST,
                        restURL,
                        Collections.emptyMap(),
                        objMapper.writeValueAsString(data),
                        Arrays.asList(OK, NOT_FOUND, BAD_REQUEST, INTERNAL_SERVER_ERROR),
                        BackupShippingResponsePrevSnap.class
                    );
                    if (
                        isResponseOk(
                            response,
                            remote.getName().displayValue,
                            fluxSink,
                            response.getData().responses
                        )
                    )
                    {
                        fluxSink.next(response.getData());
                        fluxSink.complete();
                    }
                }
                catch (AccessDeniedException | StorageException | IOException exc)
                {
                    errorReporter.reportError(exc);
                    fluxSink.error(exc);
                }
            };
            new Thread(run).start();
        });
    }

    public Flux<BackupShippingResponse> sendBackupRequest(BackupShippingSrcData data, AccessContext accCtx)
    {
        return Flux.create(fluxSink ->
        {
            // Runnable needed for flux shenanigans
            // (avoids deadlock if flux error occurs while building pipeline)
            Runnable run = () ->
            {
                try
                {
                    backupInfoMgr.addL2LSrcData(
                        data.getLinstorRemote().getName(),
                        data.getStltRemote().getName(),
                        data
                    );
                    String restURL = data.getLinstorRemote().getUrl(accCtx).toExternalForm() +
                        "/v1/internal/backups/requestShip";
                    RestResponse<BackupShippingResponse> response = restClient.execute(
                        null,
                        RestOp.POST,
                        restURL,
                        Collections.emptyMap(),
                        objMapper.writeValueAsString(
                            new BackupShippingRequest(
                                LinStor.VERSION_INFO_PROVIDER.getSemanticVersion(),
                                data.getMetaDataPojo(),
                                data.getSrcBackupName(),
                                data.getSrcClusterId(),
                                data.getLinstorRemote().getName().displayValue,
                                data.getStltRemote().getName().displayValue,
                                data.getDstRscName(),
                                data.getDstNodeName(),
                                data.getDstNetIfName(),
                                data.getDstStorPool(),
                                data.getStorPoolRename(),
                                data.getDstRscGrp(),
                                data.isUseZstd(),
                                data.isDownloadOnly(),
                                data.isForceRestore(),
                                data.isResetData(),
                                data.getDstBaseSnapName(),
                                data.getDstActualNodeName(),
                                data.getSrcRscName(),
                                data.isForceRscGrp()
                            )
                        ),
                        Arrays.asList(OK, NOT_FOUND, BAD_REQUEST, INTERNAL_SERVER_ERROR),
                        BackupShippingResponse.class
                    );
                    if (
                        isResponseOk(
                            response,
                            data.getLinstorRemote().getName().displayValue,
                            fluxSink,
                            response.getData().responses
                        )
                    )
                    {
                        fluxSink.next(response.getData());
                        fluxSink.complete();
                    }
                }
                catch (StorageException | IOException | AccessDeniedException exc)
                {
                    errorReporter.reportError(exc);
                    fluxSink.error(exc);
                }
            };
            new Thread(run).start();
        });
    }

    public Flux<JsonGenTypes.ApiCallRc> sendBackupReceiveRequest(
        BackupShippingReceiveRequest data
    )
    {
        return sendRequest(data, data.remoteUrl + "/v1/internal/backups/requestReceive", data.linstorRemoteName);
    }

    public Flux<JsonGenTypes.ApiCallRc> sendBackupReceiveDoneRequest(
        BackupShippingReceiveDoneRequest data
    )
    {
        return sendRequest(data, data.remoteUrl + "/v1/internal/backups/requestReceiveDone", data.linstorRemoteName);
    }

    public Flux<JsonGenTypes.ApiCallRc> sendPrepareAbortRequest(
        BackupShippingPrepareAbortRequest data,
        LinstorRemote remote,
        AccessContext accCtx
    )
    {
        try
        {
            return sendRequest(
                data,
                remote.getUrl(accCtx).toExternalForm() + "/v1/internal/backups/requestPrepareAbort",
                remote.getName().displayValue
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private <T> Flux<JsonGenTypes.ApiCallRc> sendRequest(T data, String restURL, String remoteName)
    {
        return Flux.create(fluxSink ->
        {
            // Runnable needed for flux shenanigans
            // (avoids deadlock if flux error occurs while building pipeline)
            Runnable run = () ->
            {
                try
                {
                    RestResponse<JsonGenTypes.ApiCallRc[]> response = restClient.execute(
                        null,
                        RestOp.POST,
                        restURL,
                        Collections.emptyMap(),
                        objMapper.writeValueAsString(
                            data
                        ),
                        Arrays.asList(OK, NOT_FOUND, BAD_REQUEST, INTERNAL_SERVER_ERROR),
                        JsonGenTypes.ApiCallRc[].class
                    );
                    ApiCallRcImpl respApiCalls = Json.jsonToApiCallRc(response.getData());
                    if (isResponseOk(response, remoteName, fluxSink, respApiCalls))
                    {
                        for (JsonGenTypes.ApiCallRc rc : response.getData())
                        {
                            fluxSink.next(rc);
                        }
                        fluxSink.complete();
                    }
                }
                catch (StorageException | IOException exc)
                {
                    errorReporter.reportError(exc);
                    fluxSink.error(exc);
                }
            };
            new Thread(run).start();
        });
    }

    private <RESPONSE_TYPE, FLUX_TYPE> boolean isResponseOk(
        RestResponse<RESPONSE_TYPE> response,
        String remoteName,
        FluxSink<FLUX_TYPE> fluxSink,
        ApiCallRcImpl responses
    )
    {
        boolean success = response.getStatusCode() == OK;
        if (!success)
        {
            ApiCallRcImpl apiCallRc;

            if (response.getStatusCode() == INTERNAL_SERVER_ERROR)
            {
                apiCallRc = ApiCallRcImpl.copyAndPrefix(
                    "Remote '" + remoteName + "': ",
                    responses
                );
            }
            else
            {
                if (responses.isEmpty())
                {
                    apiCallRc = new ApiCallRcImpl();
                    apiCallRc.addEntry(
                        ApiCallRcImpl.entryBuilder(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "Most likely the destination controller is incompatible"
                        )
                            .setCause("Probably the destination controller is not recent enough")
                            .setCorrection(
                                "Make sure the destination cluster is on the same version as the " +
                                    "current cluster (" +
                                    LinStor.VERSION_INFO_PROVIDER.getVersion() + ")"
                            )
                            .build()
                    );
                }
                else
                {
                    apiCallRc = ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.FAIL_UNKNOWN_ERROR,
                        "Destination controller had error code " + response.getStatusCode()
                    );
                    apiCallRc.addAll(
                        ApiCallRcImpl.copyAndPrefix(
                            "Remote '" + remoteName + "': ",
                            responses
                        )
                    );
                }
            }
            fluxSink.error(new ApiRcException(apiCallRc));
        }
        return success;
    }
}
