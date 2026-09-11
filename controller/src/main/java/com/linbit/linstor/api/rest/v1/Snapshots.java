package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/resource-definitions/{rscName}/snapshots")
@Produces(MediaType.APPLICATION_JSON)
public class Snapshots
{
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandler;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandler;
    private final CtrlSnapshotApiCallHandler ctrlSnapshotApiCallHandler;

    @Inject
    public Snapshots(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlSnapshotCrtApiCallHandler ctrlSnapshotCrtApiCallHandlerRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef,
        CtrlSnapshotApiCallHandler ctrlSnapshotApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlSnapshotCrtApiCallHandler = ctrlSnapshotCrtApiCallHandlerRef;
        ctrlSnapshotDeleteApiCallHandler = ctrlSnapshotDeleteApiCallHandlerRef;
        ctrlSnapshotApiCallHandler = ctrlSnapshotApiCallHandlerRef;

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
        @PathParam("snapName") @Nullable String snapName,
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
                    ctrlApiCallHandler.listSnapshotDefinition(
                        Collections.emptyList(),
                        Collections.singletonList(rscName)).stream();

                if (limit > 0)
                {
                    snapsStream = snapsStream.skip(offset).limit(limit);
                }

                List<JsonGenTypes.Snapshot> snapshot = snapsStream
                    .filter(snaphotDfn -> snapName == null || snaphotDfn.getSnapshotName().equalsIgnoreCase(snapName))
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
        @Nullable String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.Snapshot snapData = objectMapper.readValue(jsonData, JsonGenTypes.Snapshot.class);

            Flux<ApiCallRc> responses = ctrlSnapshotCrtApiCallHandler.createSnapshot(
                    snapData.nodes,
                    rscName,
                    snapData.name,
                    snapData.snapshot_definition_props
                );

            requestHelper.doFlux(
                ApiConsts.API_CRT_SNAPSHOT,
                request,
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
        @PathParam("snapName") String snapName,
        @QueryParam("nodes") List<String> nodeNames,
        @DefaultValue("false") @QueryParam("delete_empty_resource_definition") boolean deleteEmptyRscDfn
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            Flux<ApiCallRc> responses = ctrlSnapshotDeleteApiCallHandler.deleteSnapshot(
                rscName,
                snapName,
                nodeNames,
                deleteEmptyRscDfn
            );

            requestHelper.doFlux(
                ApiConsts.API_DEL_SNAPSHOT,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(responses)
            );
        }
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{snapName}")
    public void modifySnapshot(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        String jsonData
    )
        throws IOException
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.SnapshotModify modifyData =
                objectMapper.readValue(jsonData, JsonGenTypes.SnapshotModify.class);

            Flux<ApiCallRc> flux = ctrlSnapshotApiCallHandler.modify(
                    rscName,
                    snapName,
                    modifyData.override_props,
                    new HashSet<>(modifyData.delete_props),
                    new HashSet<>(modifyData.delete_namespaces)
                );

            requestHelper.doFlux(
                ApiConsts.API_MOD_SNAPSHOT,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
    }
}
