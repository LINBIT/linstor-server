package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("resource-definitions/{rscName}/snapshot-restore-resource")
public class SnapshotRestoreResource
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    SnapshotRestoreResource(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @POST
    @Path("{snapName}")
    public Response restoreResource(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_RESTORE_SNAPSHOT, request, () ->
        {
            Json.SnapshotRestore snapRestore = objectMapper.readValue(
                jsonData,
                Json.SnapshotRestore.class
            );
            ApiCallRc apiCallRc = ctrlApiCallHandler.restoreSnapshot(
                snapRestore.nodes,
                rscName,
                snapName,
                snapRestore.to_resource
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }
}
