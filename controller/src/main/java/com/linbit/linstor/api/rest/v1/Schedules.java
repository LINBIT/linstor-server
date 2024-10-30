package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.SchedulePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlScheduleApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("v1/schedules")
@Produces(MediaType.APPLICATION_JSON)
public class Schedules
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;

    private final CtrlScheduleApiCallHandler scheduleHandler;

    @Inject
    Schedules(
        RequestHelper requestHelperRef,
        CtrlScheduleApiCallHandler scheduleHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        scheduleHandler = scheduleHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response getSchedules(@Context Request request)
    {
        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_LST_SCHEDULE, request),
            () ->
            {
                List<SchedulePojo> schedulePojoList = scheduleHandler.listSchedule();
                List<JsonGenTypes.Schedule> scheduleList = schedulePojoList.stream()
                    .map(pojo -> Json.apiToSchedule(pojo))
                    .collect(Collectors.toList());
                JsonGenTypes.ScheduleList schedules = new JsonGenTypes.ScheduleList();
                schedules.data = scheduleList;
                return Response.status(Response.Status.OK).entity(objectMapper.writeValueAsString(schedules))
                    .build();
            },
            false
        );
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void createSchedule(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.Schedule scheduleJson = objectMapper.readValue(jsonData, JsonGenTypes.Schedule.class);
            Flux<ApiCallRc> flux = scheduleHandler.createSchedule(
                scheduleJson.schedule_name,
                scheduleJson.full_cron,
                scheduleJson.inc_cron,
                scheduleJson.keep_local,
                scheduleJson.keep_remote,
                scheduleJson.on_failure,
                scheduleJson.max_retries
            );

            requestHelper.doFlux(
                ApiConsts.API_CRT_SCHEDULE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Path("{scheduleName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void changeSchedule(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("scheduleName") String scheduleName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.ScheduleModify scheduleJson = objectMapper
                .readValue(jsonData, JsonGenTypes.ScheduleModify.class);
            Flux<ApiCallRc> flux = scheduleHandler.changeSchedule(
                scheduleName,
                scheduleJson.full_cron,
                scheduleJson.inc_cron,
                scheduleJson.keep_local,
                scheduleJson.keep_remote,
                scheduleJson.on_failure,
                scheduleJson.max_retries
            );

            requestHelper.doFlux(
                ApiConsts.API_MOD_SCHEDULE,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @DELETE
    @Path("{scheduleName}")
    public void deleteSchedule(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("scheduleName") String scheduleName
    )
    {
        Flux<ApiCallRc> flux = scheduleHandler.delete(scheduleName);
        requestHelper.doFlux(
            ApiConsts.API_DEL_SCHEDULE,
            request,
            asyncResponse,
            ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
        );
    }
}
