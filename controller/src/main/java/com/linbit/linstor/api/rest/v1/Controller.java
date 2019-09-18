package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.HashSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("controller")
@Produces(MediaType.APPLICATION_JSON)
public class Controller
{
    private final ErrorReporter errorReporter;
    private final ObjectMapper objectMapper;
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public Controller(
        ErrorReporter errorReporterRef,
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    @Path("properties")
    public Response listProperties(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_CTRL_PROPS, request,
            () ->
                Response.status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(ctrlApiCallHandler.listCtrlCfg()))
                    .build(),
            false
        );
    }

    private Pair<String, String> splitFullKey(final String fullKey)
    {
        String key = fullKey;
        String namespace = "";
        int lastSlash = fullKey.lastIndexOf("/");
        if (lastSlash > 0)
        {
            namespace = fullKey.substring(0, lastSlash);
            key = fullKey.substring(lastSlash + 1);
        }

        return new Pair<>(key, namespace);
    }

    @POST
    @Path("properties")
    public Response setProperties(
        @Context Request request,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_SET_CTRL_PROP, request, () ->
        {
            JsonGenTypes.ControllerPropsModify properties = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ControllerPropsModify.class
            );

            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyCtrl(
                properties.override_props,
                new HashSet<>(properties.delete_props),
                new HashSet<>(properties.delete_namespaces)
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);

        }, true);
    }

    @DELETE
    @Path("properties/{key : .*}")
    public Response deleteProperty(
        @Context Request request,
        @PathParam("key") String key
    )
    {
        return requestHelper.doInScope(ApiConsts.API_DEL_CTRL_PROP, request, () ->
        {
            Pair<String, String> keyPair = splitFullKey(key);

            ApiCallRc apiCallRc = ctrlApiCallHandler.deleteCtrlCfgProp(
                keyPair.objA,
                keyPair.objB
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @GET
    @Path("version")
    public Response version(
        @Context Request request
    )
    {
        JsonGenTypes.ControllerVersion controllerVersion = new JsonGenTypes.ControllerVersion();
        controllerVersion.version = LinStor.VERSION_INFO_PROVIDER.getVersion();
        controllerVersion.git_hash = LinStor.VERSION_INFO_PROVIDER.getGitCommitId();
        controllerVersion.build_time = LinStor.VERSION_INFO_PROVIDER.getBuildTime();
        controllerVersion.rest_api_version = JsonGenTypes.REST_API_VERSION;

        Response resp;
        try
        {
            resp =  Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(controllerVersion))
                .build();
        }
        catch (JsonProcessingException exc)
        {
            errorReporter.reportError(exc);
            resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }

        return resp;
    }
}
