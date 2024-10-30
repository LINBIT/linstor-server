package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ExosConnectionMapPojo;
import com.linbit.linstor.api.pojo.ExosDefaultsPojo;
import com.linbit.linstor.api.pojo.ExosEnclosureEventPojo;
import com.linbit.linstor.api.pojo.ExosEnclosureHealthPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExosConnectionMap;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExosDefaults;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExosDefaultsModify;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExosEnclosure;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExosEnclosureEvent;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ExosEnclosureHealth;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlExosApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Deprecated(forRemoval = true)
@Path("v1/vendor/seagate/exos")
public class Exos
{
    private final RequestHelper requestHelper;
    private final CtrlExosApiCallHandler ctrlExosHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public Exos(
        RequestHelper requestHelperRef,
        CtrlExosApiCallHandler ctrlExosHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlExosHandler = ctrlExosHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    @Path("defaults")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDefaultSettings(
        @Context Request request
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_EXOS_DFLTS, request, () ->
        {
            ExosDefaultsPojo dfltsPojo = ctrlExosHandler.getDefaults();
            ExosDefaults dflts = Json.apiToExosDefaults(dfltsPojo);
            return Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(dflts))
                .build();
        }, true);
    }

    @PUT
    @Path("defaults")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response modifyDefaultSettings(
        @Context Request request,
        String json
    )
    {
        return requestHelper.doInScope(ApiConsts.API_MOD_EXOS_DFLTS, request, () ->
        {
            ExosDefaultsModify modifyData = objectMapper.readValue(json, ExosDefaultsModify.class);

            ApiCallRcImpl apiCallRc = ctrlExosHandler.modifyDefaults(
                modifyData.username,
                modifyData.username_env,
                modifyData.password,
                modifyData.password_env,
                modifyData.unset_keys
            );
            addDeprecationWarning(apiCallRc);
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @GET
    @Path("enclosures")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listEnclosures(
        @Context Request request,
        @QueryParam("nocache") boolean nocache
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_EXOS_ENCLOSURES,
            request,
            () ->
            {
                List<ExosEnclosureHealthPojo> exosEnclosurePojoList = ctrlExosHandler.listEnclosures(nocache);
                List<ExosEnclosureHealth> exosEnclosureList = exosEnclosurePojoList.stream()
                    .map(Json::apiToExosEnclosure)
                .collect(Collectors.toList());

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(exosEnclosureList))
                    .build();
            },
            false
        );
    }

    @GET
    @Path("map")
    public Response map(@Context Request request)
    {
        return requestHelper.doInScope(ApiConsts.API_EXOS_MAP, request, () ->
        {
            List<ExosConnectionMapPojo> pojoList = ctrlExosHandler.showMap();
            List<ExosConnectionMap> jsonList = pojoList.stream()
                .map(Json::apiToExosConnectionMap)
                .collect(Collectors.toList());
            return Response.status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(jsonList))
                .build();
        }, false);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("enclosures/{enclosure}/events")
    public Response events(
        @Context Request request,
        @PathParam("enclosure") String enclosure,
        @QueryParam("count") Integer count
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_EXOS_ENCLOSURE_EVENTS,
            request,
            () ->
            {
                List<ExosEnclosureEventPojo> exosEnclosurePojoList = ctrlExosHandler
                    .describeEnclosure(enclosure, count);
                List<ExosEnclosureEvent> exosEnclosureList = exosEnclosurePojoList.stream()
                    .map(Json::apiToExosEnclosureEvent)
                    .collect(Collectors.toList());

                return Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(exosEnclosureList))
                    .build();
            },
            false
        );
    }

    @POST
    @Path("enclosures")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createEnclosure(
        @Context Request request,
        String json
    )
    {
        return requestHelper.doInScope(ApiConsts.API_CRT_EXOS_ENCLOSURE, request, () ->
        {
            ExosEnclosure createData = objectMapper.readValue(json, ExosEnclosure.class);

            ApiCallRcImpl apiCallRc = ctrlExosHandler.createEnclosure(
                createData.name,
                createData.ctrl_a_ip,
                createData.ctrl_b_ip,
                createData.password,
                createData.password_env,
                createData.username,
                createData.username_env
            );
            addDeprecationWarning(apiCallRc);
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("enclosures/{enclosure}")
    public Response modifyEnclosure(
        @Context Request request,
        @PathParam("enclosure") String enclosure,
        String json
    )
    {
        return requestHelper.doInScope(ApiConsts.API_MOD_EXOS_ENCLOSURE, request, () ->
        {
            ExosEnclosure createData = objectMapper.readValue(json, ExosEnclosure.class);

            ApiCallRcImpl apiCallRc = ctrlExosHandler.modifyEnclosure(
                enclosure,
                createData.ctrl_a_ip,
                createData.ctrl_b_ip,
                createData.password,
                createData.password_env,
                createData.username,
                createData.username_env
            );
            addDeprecationWarning(apiCallRc);
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @DELETE
    @Path("enclosures/{enclosure}")
    public Response deleteEnclosure(
        @Context Request request,
        @PathParam("enclosure") String enclosure
    )
    {
        return requestHelper.doInScope(ApiConsts.API_DEL_EXOS_ENCLOSURE, request, () ->
        {
            ApiCallRcImpl apiCallRc = ctrlExosHandler.deleteEnclosure(enclosure);
            addDeprecationWarning(apiCallRc);
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @GET
    @Path("enclosures/{enclosure}/exec/{cmd: .*}")
    public Response exosExec(
        @Context Request request,
        @PathParam("enclosure") String enclosure,
        @PathParam("cmd") List<PathSegment> params
    )
    {
        return requestHelper.doInScope(ApiConsts.API_EXOS_EXEC, request, () ->
        {
            List<String> cmds = new ArrayList<>();
            for (PathSegment ps : params)
            {
                cmds.add(ps.getPath());
            }
            Object response = ctrlExosHandler.exec(enclosure, cmds);
            return Response.status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(response))
                .build();
        }, false);
    }

    public static void addDeprecationWarning(ApiCallRcImpl apiCallRcRef)
    {
        apiCallRcRef.add(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.WARN_DEPRECATED,
                "EXOS is deprecated and will be deleted in a future LINSTOR release"
            )
        );
    }
}
