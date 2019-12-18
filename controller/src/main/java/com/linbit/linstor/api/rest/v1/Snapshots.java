package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("resource-definitions/{rscName}/snapshots")
@Produces(MediaType.APPLICATION_JSON)
public class Snapshots
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandler;

    @Inject
    public Snapshots(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlSnapshotCrtApiCallHandler = ctrlSnapshotCrtApiCallHandlerRef;
        ctrlSnapshotDeleteApiCallHandler = ctrlSnapshotDeleteApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listSnapshots(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listSnapshots(request, rscName, null, limit, offset);
    }

    @GET
    @Path("{snapName}")
    public Response listSnapshots(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_SNAPSHOT_DFN, request, () ->
        {
            boolean rscDfnExists = ctrlApiCallHandler.listResourceDefinitions()
                .parallelStream()
                .anyMatch(rscDfnApi -> rscDfnApi.getResourceName().equalsIgnoreCase(rscName));

            Response response;

            if (rscDfnExists)
            {
                Stream<SnapshotDefinitionListItemApi> snapsStream =
                    ctrlApiCallHandler.listSnapshotDefinition().stream();

                if (limit > 0)
                {
                    snapsStream = snapsStream.skip(offset).limit(limit);
                }

                List<JsonGenTypes.Snapshot> snapshot = snapsStream
                    .filter(snaphotDfn -> snaphotDfn.getRscDfn().getResourceName().equalsIgnoreCase(rscName))
                    .map(Json::apiToSnapshot)
                    .collect(Collectors.toList());

                response = RequestHelper.queryRequestResponse(
                    objectMapper, ApiConsts.FAIL_NOT_FOUND_SNAPSHOT, "Snapshot", snapName, snapshot
                );
            }
            else
            {
                response = RequestHelper.notFoundResponse(
                    ApiConsts.FAIL_NOT_FOUND_RSC_DFN,
                    String.format("Resource definition '%s' not found.", rscName)
                );
            }

            return response;
        }, false);
    }

    @POST
    public void createSnapshot(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.Snapshot snapData = objectMapper.readValue(jsonData, JsonGenTypes.Snapshot.class);

            Flux<ApiCallRc> responses = ctrlSnapshotCrtApiCallHandler.createSnapshot(
                    snapData.nodes,
                    rscName,
                    snapData.name
                )
                .subscriberContext(requestHelper.createContext(ApiConsts.API_CRT_SNAPSHOT, request));

            requestHelper.doFlux(
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses, Response.Status.CREATED)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    @Path("{snapName}")
    public void deleteSnapshot(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName
    )
    {
        Flux<ApiCallRc> responses = ctrlSnapshotDeleteApiCallHandler.deleteSnapshot(rscName, snapName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_DEL_SNAPSHOT, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(responses));
    }
}
