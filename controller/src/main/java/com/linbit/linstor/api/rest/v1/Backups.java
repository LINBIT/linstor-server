package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.backups.BackupInfoPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.BackupShip;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupL2LSrcApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupRestoreApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlScheduledBackupsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.BackupApi;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("v1/remotes/{remoteName}/backups")
@Produces(MediaType.APPLICATION_JSON)
public class Backups
{
    private static final int DEL_OPT_CASCADE = 1;
    private static final int DEL_OPT_ID = 1 << 1;
    private static final int DEL_OPT_ID_PREFIX = 1 << 2;
    private static final int DEL_OPT_S3_KEY = 1 << 3;
    private static final int DEL_OPT_TIME_RSC_NODE = 1 << 4;
    private static final int DEL_OPT_ALL = 1 << 5;
    private static final int DEL_OPT_ALL_LOCALCLUSTER = 1 << 6;

    private static final List<Integer> DEL_OPT_ALLOWED_COMBINATIONS = Arrays.asList(
        DEL_OPT_ID,
        DEL_OPT_ID | DEL_OPT_CASCADE,

        DEL_OPT_ID_PREFIX,
        DEL_OPT_ID_PREFIX | DEL_OPT_CASCADE,

        DEL_OPT_S3_KEY,
        DEL_OPT_S3_KEY | DEL_OPT_CASCADE,

        DEL_OPT_TIME_RSC_NODE,
        DEL_OPT_TIME_RSC_NODE | DEL_OPT_CASCADE,

        DEL_OPT_ALL, // forced cascade
        DEL_OPT_ALL | DEL_OPT_CASCADE,

        DEL_OPT_ALL_LOCALCLUSTER, // forced cascade
        DEL_OPT_ALL_LOCALCLUSTER | DEL_OPT_CASCADE
    );

    private final RequestHelper requestHelper;
    private final CtrlBackupApiCallHandler backupApiCallHandler;
    private final CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlBackupCreateApiCallHandler backupCrtApiCallHandler;
    private final CtrlBackupRestoreApiCallHandler backupRestoreApiCallHandler;
    private final CtrlScheduledBackupsApiCallHandler scheduledBackupsApiCallHandler;

    @Inject
    public Backups(
        RequestHelper requestHelperRef,
        CtrlBackupApiCallHandler backupApiCallHandlerRef,
        CtrlBackupL2LSrcApiCallHandler backupL2LSrcApiCallHandlerRef,
        CtrlBackupCreateApiCallHandler backupCrtApiCallHandlerRef,
        CtrlBackupRestoreApiCallHandler backupRestoreApiCallHandlerRef,
        CtrlScheduledBackupsApiCallHandler scheduledBackupsApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        backupApiCallHandler = backupApiCallHandlerRef;
        backupL2LSrcApiCallHandler = backupL2LSrcApiCallHandlerRef;
        backupCrtApiCallHandler = backupCrtApiCallHandlerRef;
        backupRestoreApiCallHandler = backupRestoreApiCallHandlerRef;
        scheduledBackupsApiCallHandler = scheduledBackupsApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @POST
    public void createBackup(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.BackupCreate data = objectMapper.readValue(jsonData, JsonGenTypes.BackupCreate.class);
            boolean incremental = data.incremental != null && data.incremental;
            responses = backupCrtApiCallHandler
                .createBackup(data.rsc_name, data.snap_name, remoteName, data.node_name, null, incremental, false)
                .contextWrite(requestHelper.createContext(ApiConsts.API_CRT_BACKUP, request));
            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.CREATED)
            );
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @POST
    @Path("restore")
    public void restoreBackup(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;

        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.BackupRestore data = objectMapper.readValue(jsonData, JsonGenTypes.BackupRestore.class);

            validateBackupSelectionParams(
                data.last_backup,
                data.src_rsc_name,
                data.src_snap_name,
                data.download_only,
                data.force_restore
            );

            responses = backupRestoreApiCallHandler.restoreBackup(
                data.src_rsc_name,
                data.src_snap_name,
                data.last_backup,
                data.stor_pool_map,
                data.node_name,
                data.target_rsc_name,
                remoteName,
                data.passphrase,
                data.download_only,
                data.force_restore
            ).contextWrite(requestHelper.createContext(ApiConsts.API_RESTORE_BACKUP, request));
                requestHelper.doFlux(asyncResponse,ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.CREATED));
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @DELETE
    public void deleteBackups(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        @DefaultValue("") @QueryParam("id") String id,
        @DefaultValue("") @QueryParam("id_prefix") String idPrefix,
        @DefaultValue("false") @QueryParam("cascading") boolean cascading,
        @DefaultValue("") @QueryParam("node_name") String nodeName,
        @DefaultValue("false") @QueryParam("all_local_cluster") boolean allLocalCluster,
        @DefaultValue("false") @QueryParam("all") boolean all,
        @DefaultValue("") @QueryParam("s3key") String s3key,
        @DefaultValue("") @QueryParam("timestamp") String timestamp,
        @DefaultValue("") @QueryParam("resource_name") String rscName,
        @DefaultValue("false") @QueryParam("dryrun") boolean dryRun,
        @DefaultValue("false") @QueryParam("keep_snaps") boolean keepSnaps
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            /*
             * timestamp means every backup created BEFORE the given timestamp
             */
            int combination = !cascading ? 0 : DEL_OPT_CASCADE;
            combination |= id.isEmpty() ? 0 : DEL_OPT_ID;
            combination |= idPrefix.isEmpty() ? 0 : DEL_OPT_ID_PREFIX;
            combination |= s3key.isEmpty() ? 0 : DEL_OPT_S3_KEY;
            /*
             * at least one of timestamp, rscName or nodeName need to be used. If one is used, the other two can also be
             * used in addition. Therefore we can treat these three as one "combination"
             */
            combination |= timestamp.isEmpty() && rscName.isEmpty() && nodeName.isEmpty() ? 0 : DEL_OPT_TIME_RSC_NODE;
            combination |= !all ? 0 : DEL_OPT_ALL;
            combination |= !allLocalCluster ? 0 : DEL_OPT_ALL_LOCALCLUSTER;
            // dryRun is combinable with all allowed combinations, no need to check

            if (!DEL_OPT_ALLOWED_COMBINATIONS.contains(combination))
            {
                String errorMsg;
                if (combination == 0)
                {
                    errorMsg = "You have to specify which backups you want to delete. Use 'all' to delete all backups in the specified bucket";
                }
                else
                {
                    errorMsg = "Too many parameters given. You can only use the following parameters in combination: id and cascading; s3key and cascading; " +
                        "since, resource_name and node_name in any combination; all other parameters can only be used on their own.";
                }
                // CONFUSION!!!!!
                requestHelper.doFlux(
                    asyncResponse,
                    ApiCallRcRestUtils.mapToMonoResponse(
                        Flux.just(
                            ApiCallRcImpl.singleApiCallRc(ApiConsts.FAIL_INVLD_REQUEST, errorMsg)
                        ),
                        Response.Status.BAD_REQUEST
                    )
                );
            }
            else
            {
                Flux<ApiCallRc> deleteFlux = backupApiCallHandler.deleteBackup(
                    rscName,
                    id,
                    idPrefix,
                    timestamp,
                    nodeName,
                    cascading,
                    allLocalCluster,
                    all,
                    s3key,
                    remoteName,
                    dryRun,
                    keepSnaps
                ).contextWrite(requestHelper.createContext(ApiConsts.API_DEL_BACKUP, request));
                requestHelper.doFlux(
                    asyncResponse,
                    ApiCallRcRestUtils.mapToMonoResponse(
                        deleteFlux,
                        Response.Status.OK
                    )
                );
            }
        }
    }

    @POST
    @Path("abort")
    public void abortBackup(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.BackupAbort data = objectMapper.readValue(jsonData, JsonGenTypes.BackupAbort.class);
            Flux<ApiCallRc> deleteFlux = backupApiCallHandler.backupAbort(
                data.rsc_name,
                data.restore != null && data.restore,
                data.create != null && data.create,
                remoteName
            ).contextWrite(requestHelper.createContext(ApiConsts.API_ABORT_BACKUP, request));
            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(
                    deleteFlux,
                    Response.Status.OK
                )
            );
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @GET
    public Response listBackups(
        @Context Request request,
        @DefaultValue("") @QueryParam("rsc_name") String rscName,
        @DefaultValue("") @QueryParam("snap_name") String snapName,
        @PathParam("remoteName") String remoteName
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_BACKUPS,
            request,
            () ->
            {
                Pair<Map<String, BackupApi>, Set<String>> backups = backupApiCallHandler
                    .listBackups(rscName, snapName, remoteName);
                JsonGenTypes.BackupList backupList = new JsonGenTypes.BackupList();
                backupList.linstor = Json.apiToBackup(backups.objA);
                backupList.other = new JsonGenTypes.BackupOther();
                backupList.other.files = new ArrayList<>(backups.objB);
                return Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(backupList))
                    .build();
            },
            false
        );
    }

    @POST
    @Path("ship")
    public void shipBackup(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        try
        {
            BackupShip data = objectMapper.readValue(jsonData, JsonGenTypes.BackupShip.class);
            responses = backupL2LSrcApiCallHandler.shipBackup(
                data.src_node_name,
                data.src_rsc_name,
                remoteName,
                data.dst_rsc_name,
                data.dst_node_name,
                data.dst_net_if_name,
                data.dst_stor_pool,
                data.stor_pool_rename,
                data.download_only,
                data.force_restore,
                null,
                data.allow_incremental,
                false
            )
                .contextWrite(requestHelper.createContext(ApiConsts.API_SHIP_BACKUP, request));
            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.CREATED)
            );
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @POST
    @Path("info")
    public void backupInfo(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.BackupInfoRequest data = objectMapper
                .readValue(jsonData, JsonGenTypes.BackupInfoRequest.class);

            validateBackupSelectionParams(data.last_backup, data.src_rsc_name, data.src_snap_name, false, false);

            boolean renameMapNullOrEmpty = data.stor_pool_map == null || data.stor_pool_map.isEmpty();
            boolean nodeNameNullOrEmpty = data.node_name == null || data.node_name.isEmpty();

            if (!renameMapNullOrEmpty && nodeNameNullOrEmpty)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_REQUEST,
                        "Too few parameters given. When using --storpool-rename, then --target-node is also needed."
                    )
                );
            }

            Flux<BackupInfoPojo> backupInfoFlux = backupApiCallHandler
                .backupInfo(
                    data.src_rsc_name,
                    data.src_snap_name,
                    data.last_backup,
                    data.stor_pool_map,
                    data.node_name,
                    remoteName
                )
                .contextWrite(requestHelper.createContext(ApiConsts.API_BACKUP_INFO, request));
            requestHelper.doFlux(asyncResponse, backupInfoPojoToResponse(backupInfoFlux, asyncResponse));
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @PUT
    @Path("schedule/{scheduleName}/enable")
    public void enableSchedule(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        @PathParam("scheduleName") String scheduleName,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.BackupSchedule data = objectMapper.readValue(jsonData, JsonGenTypes.BackupSchedule.class);
            responses = scheduledBackupsApiCallHandler
                .enableSchedule(
                    data.rsc_name,
                    data.grp_name,
                    remoteName,
                    scheduleName,
                    data.node_name,
                    data.dst_stor_pool,
                    data.stor_pool_rename,
                    data.force_restore
                )
                .contextWrite(requestHelper.createContext(ApiConsts.API_CRT_BACKUP, request));
            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.OK)
            );
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @PUT
    @Path("schedule/{scheduleName}/disable")
    public void disable(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        @PathParam("scheduleName") String scheduleName,
        String jsonData
    )
    {
        Flux<ApiCallRc> responses;
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.BackupSchedule data = objectMapper.readValue(jsonData, JsonGenTypes.BackupSchedule.class);
            responses = scheduledBackupsApiCallHandler
                .disableSchedule(data.rsc_name, data.grp_name, remoteName, scheduleName, data.node_name)
                .contextWrite(requestHelper.createContext(ApiConsts.API_CRT_BACKUP, request));
            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.OK)
            );
        }
        catch (JsonProcessingException exc)
        {
            ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
        }
    }

    @DELETE
    @Path("schedule/{scheduleName}/delete")
    public void deleteSchedule(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("remoteName") String remoteName,
        @PathParam("scheduleName") String scheduleName,
        @DefaultValue("") @QueryParam("rsc_dfn_name") String rscName,
        @DefaultValue("") @QueryParam("rsc_grp_name") String grpName
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Flux<ApiCallRc> responses;
            responses = scheduledBackupsApiCallHandler
                .deleteSchedule(rscName, grpName, remoteName, scheduleName)
                .contextWrite(requestHelper.createContext(ApiConsts.API_CRT_BACKUP, request));
            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.OK)
            );
        }
    }

    private Mono<Response> backupInfoPojoToResponse(
        Flux<BackupInfoPojo> backupInfoFlux,
        @Suspended final AsyncResponse asyncResponse
    )
    {
        return backupInfoFlux.flatMap(backupInfoPojo ->
        {
            Response resp = null;
            JsonGenTypes.BackupInfo backupInfo = Json.apiToBackupInfo(backupInfoPojo);
            try
            {
                resp = Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(backupInfo))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
            }
            catch (JsonProcessingException exc)
            {
                ApiCallRcRestUtils.handleJsonParseException(exc, asyncResponse);
            }
            return Mono.just(resp);
        }).next();
    }

    private static void validateBackupSelectionParams(
        String backupIdRef,
        String rscNameRef,
        String snapNameRef,
        boolean downloadOnly,
        boolean forceRestore
    )
        throws ApiRcException
    {
        String backupId = (backupIdRef == null) ? "" : backupIdRef;
        String rscName = (rscNameRef == null) ? "" : rscNameRef;
        String snapName = (snapNameRef == null) ? "" : snapNameRef;

        if (backupId.isEmpty() && rscName.isEmpty() && snapName.isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REQUEST,
                    "One of backup ID, resource name or snapshot name is required, but none was given"
                )
            );
        }
        if (downloadOnly && forceRestore)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_REQUEST,
                    "--download-only and --force-restore are mutually exclusive, use at most one of them."
                )
            );
        }
    }
}
