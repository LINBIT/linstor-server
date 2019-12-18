package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotRollbackApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;

import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("resource-definitions/{rscName}/snapshot-rollback")
public class SnapshotRollback
{
    private final RequestHelper requestHelper;
    private final CtrlSnapshotRollbackApiCallHandler ctrlSnapshotRollbackApiCallHandler;

    @Inject
    public SnapshotRollback(
        RequestHelper requestHelperRef,
        CtrlSnapshotRollbackApiCallHandler ctrlSnapshotRollbackApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlSnapshotRollbackApiCallHandler = ctrlSnapshotRollbackApiCallHandlerRef;
    }

    @POST
    @Path("{snapName}")
    public void rollbackSnapshot(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName
    )
    {
        Flux<ApiCallRc> flux = ctrlSnapshotRollbackApiCallHandler.rollbackSnapshot(rscName, snapName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_ROLLBACK_SNAPSHOT, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
    }
}
