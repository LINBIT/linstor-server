package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.Snapshot;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest.SnapReq;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

/**
 * This class is meant to contain endpoints that need to access multiple objects at once
 * (similar to GET-requests) but can't be added to the already existing path due to
 * possible name conflicts.
 * While GET-requests fitting these constraints should most likely be added to /v1/view,
 * this endpoint is for all the POST, PUT, and DELETE requests one might need.
 * (e.g. one DELETE endpoint to delete all snapshots at once)
 */

@Path("v1/actions")
@Produces(MediaType.APPLICATION_JSON)
public class Actions
{
    private final RequestHelper requestHelper;
    private final CtrlSnapshotCrtApiCallHandler snapCrtHandler;

    private final ObjectMapper objectMapper;

    @Inject
    public Actions(
        RequestHelper requestHelperRef,
        CtrlSnapshotCrtApiCallHandler snapCrtHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        snapCrtHandler = snapCrtHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("snapshot/multi")
    public void createMultiSnapshots(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.CreateMultiSnapshotRequest req = objectMapper
                .readValue(jsonData, JsonGenTypes.CreateMultiSnapshotRequest.class);

            List<SnapReq> createSnapReqs = new ArrayList<>();
            for (Snapshot snapReq : req.snapshots)
            {
                createSnapReqs.add(new SnapReq(
                    snapReq.nodes, snapReq.resource_name, snapReq.name, snapReq.snapshot_definition_props));
            }
            Flux<ApiCallRc> flux = snapCrtHandler.createSnapshot(new CreateMultiSnapRequest(createSnapReqs));

            requestHelper.doFlux(
                ApiConsts.API_CRT_SNAPSHOT_MULTI,
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
}
