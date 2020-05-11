package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("v1/resource-definitions/{rscName}/snapshot-restore-volume-definition")
public class SnapshotRestoreVolumeDefinition
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public SnapshotRestoreVolumeDefinition(
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
    public Response restoreVolumeDefinition(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_RESTORE_VLM_DFN, request, () ->
        {
            JsonGenTypes.SnapshotRestore snapRestore = objectMapper.readValue(
                jsonData,
                JsonGenTypes.SnapshotRestore.class
            );
            ApiCallRc apiCallRc = ctrlApiCallHandler.restoreVlmDfn(
                rscName,
                snapName,
                snapRestore.to_resource
            );

            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }
}
