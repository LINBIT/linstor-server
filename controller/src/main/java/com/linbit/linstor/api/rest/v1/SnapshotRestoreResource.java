package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRestoreApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;

import java.io.IOException;
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/resource-definitions/{rscName}/snapshot-restore-resource")
public class SnapshotRestoreResource
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlSnapshotRestoreApiCallHandler ctrlSnapshotRestoreApiCallHandler;

    @Inject
    SnapshotRestoreResource(
        RequestHelper requestHelperRef,
        CtrlSnapshotRestoreApiCallHandler ctrlSnapshotRestoreApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlSnapshotRestoreApiCallHandler = ctrlSnapshotRestoreApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    @Path("{snapName}")
    public void restoreResource(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.SnapshotRestore snapRestore = objectMapper.readValue(
                jsonData,
                JsonGenTypes.SnapshotRestore.class
            );

            Flux<ApiCallRc> flux = ctrlSnapshotRestoreApiCallHandler.restoreSnapshot(
                snapRestore.nodes,
                rscName,
                snapName,
                snapRestore.to_resource,
                snapRestore.stor_pool_rename == null ? Collections.emptyMap() : snapRestore.stor_pool_rename
            ).contextWrite(requestHelper.createContext(ApiConsts.API_RESTORE_SNAPSHOT, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }
}
