package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlCbtSessionApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlCbtSessionApiCallHandler.CbtExtentInfo;
import com.linbit.linstor.core.apicallhandler.controller.CtrlCbtSessionApiCallHandler.CbtExtentsInfo;
import com.linbit.linstor.core.apicallhandler.controller.CtrlCbtSessionApiCallHandler.CbtSessionInfo;
import com.linbit.linstor.core.apicallhandler.controller.CtrlCbtSessionApiCallHandler.CbtSessionVolumeInfo;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
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
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;

@Path("v1/resource-definitions/{rscName}/snapshots/{snapName}/cbt-session")
@Produces(MediaType.APPLICATION_JSON)
public class CbtSessions
{
    private final RequestHelper requestHelper;
    private final CtrlCbtSessionApiCallHandler cbtSessionHandler;
    private final ObjectMapper objectMapper;

    @Inject
    CbtSessions(RequestHelper requestHelperRef, CtrlCbtSessionApiCallHandler cbtSessionHandlerRef)
    {
        requestHelper = requestHelperRef;
        cbtSessionHandler = cbtSessionHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @POST
    public void startSession(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        @Nullable String jsonData
    )
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.CbtSessionStart startData = objectMapper.readValue(
                jsonData,
                JsonGenTypes.CbtSessionStart.class
            );
            requestHelper.doFlux(
                ApiConsts.API_CRT_CBT_SESSION,
                request,
                asyncResponse,
                cbtSessionHandler.startSession(rscName, snapName, startData.node)
                    .map(info -> toResponse(Response.Status.CREATED, toJson(info)))
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @GET
    public void showSession(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName
    )
    {
        requestHelper.doFlux(
            ApiConsts.API_LST_CBT_SESSION,
            request,
            asyncResponse,
            cbtSessionHandler.showSession(rscName, snapName)
                .map(info -> toResponse(Response.Status.OK, toJson(info)))
        );
    }

    @GET
    @Path("volumes/{vlmNr}/extents")
    public void getExtents(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName,
        @PathParam("vlmNr") int vlmNr,
        @DefaultValue("0") @QueryParam("start_offset") long startOffset,
        @DefaultValue("" + CtrlCbtSessionApiCallHandler.DEFAULT_MAX_EXTENTS) @QueryParam("max_extents")
        int maxExtents,
        @DefaultValue(CtrlCbtSessionApiCallHandler.EXTENTS_MODE_DIRTY) @QueryParam("mode") String mode
    )
    {
        requestHelper.doFlux(
            ApiConsts.API_LST_CBT_EXTENTS,
            request,
            asyncResponse,
            cbtSessionHandler.getExtents(rscName, snapName, vlmNr, startOffset, maxExtents, mode)
                .map(extents -> toResponse(Response.Status.OK, toJson(extents)))
        );
    }

    @DELETE
    public void finishSession(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("snapName") String snapName
    )
    {
        requestHelper.doFlux(
            ApiConsts.API_DEL_CBT_SESSION,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(cbtSessionHandler.finishSession(rscName, snapName))
        );
    }

    private Response toResponse(Response.Status statusRef, Object jsonRef)
    {
        try
        {
            return Response.status(statusRef)
                .entity(objectMapper.writeValueAsString(jsonRef))
                .type(MediaType.APPLICATION_JSON)
                .build();
        }
        catch (JsonProcessingException exc)
        {
            throw new RuntimeException(exc);
        }
    }

    private static JsonGenTypes.CbtSession toJson(CbtSessionInfo infoRef)
    {
        JsonGenTypes.CbtSession json = new JsonGenTypes.CbtSession();
        json.resource_name = infoRef.rscName();
        json.node = infoRef.nodeName();
        json.cbt_id = infoRef.cbtId();
        json.volumes = new ArrayList<>();
        for (CbtSessionVolumeInfo vlm : infoRef.volumes())
        {
            JsonGenTypes.CbtSessionVolume jsonVlm = new JsonGenTypes.CbtSessionVolume();
            jsonVlm.volume_number = vlm.vlmNr();
            jsonVlm.device_path = vlm.devicePath();
            jsonVlm.volume_size = vlm.sizeBytes();
            json.volumes.add(jsonVlm);
        }
        return json;
    }

    private static JsonGenTypes.CbtExtents toJson(CbtExtentsInfo infoRef)
    {
        JsonGenTypes.CbtExtents json = new JsonGenTypes.CbtExtents();
        json.base_cbt_id = infoRef.baseCbtId();
        json.since = infoRef.since();
        json.volume_size = infoRef.volumeSize();
        json.next_offset = infoRef.nextOffset();
        json.extents = new ArrayList<>(infoRef.extents().size());
        for (CbtExtentInfo extent : infoRef.extents())
        {
            JsonGenTypes.CbtExtent jsonExtent = new JsonGenTypes.CbtExtent();
            jsonExtent.start = extent.start();
            jsonExtent.length = extent.length();
            jsonExtent.dirty = extent.dirty();
            jsonExtent.zero = extent.zero();
            json.extents.add(jsonExtent);
        }
        return json;
    }
}
