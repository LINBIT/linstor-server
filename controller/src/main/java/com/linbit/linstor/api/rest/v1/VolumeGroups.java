package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.VlmGrpPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.objects.VolumeGroup.VlmGrpApi;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("resource-groups/{rscName}/volume-groups")
@Produces(MediaType.APPLICATION_JSON)
public class VolumeGroups
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public VolumeGroups(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listVolumeGroup(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listVolumeGroup(request, rscName, null, limit, offset);
    }

    @GET
    @Path("{vlmNr}")
    public Response listVolumeGroup(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("vlmNr") Integer vlmNr,
        @DefaultValue("0") @QueryParam("limit") int limitRef,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_VLM_GRP, request),
            () ->
        {

            List<VlmGrpApi> vlmGrpList =  ctrlApiCallHandler.listVolumeGroups(rscName, vlmNr);
            int limit = vlmGrpList.size();
            if (limitRef != 0)
            {
                limit = limitRef;
            }
            vlmGrpList = vlmGrpList.subList(offset, limit + offset);

            final List<JsonGenTypes.VolumeGroup> vlmGrps = vlmGrpList.stream().map(Json::apiToVolumeGroup)
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper,
                ApiConsts.FAIL_NOT_FOUND_VLM_GRP,
                "Volume Group",
                vlmNr == null ? null : vlmNr.toString(),
                vlmGrps
            );
        }, false);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createVolumeGroup(
        @Context Request request,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_CRT_VLM_GRP, request), () ->
        {
            JsonGenTypes.VolumeGroup vlmGrp;
            if (jsonData != null && !jsonData.isEmpty())
            {
                vlmGrp = objectMapper.readValue(
                    jsonData,
                    JsonGenTypes.VolumeGroup.class
                );
            }
            else
            {
                vlmGrp = new JsonGenTypes.VolumeGroup();
            }

            ApiCallRc apiCallRc = ctrlApiCallHandler.createVlmGrps(
                rscName,
                Arrays.asList(
                    new VlmGrpPojo(
                        null,
                        vlmGrp.volume_number,
                        vlmGrp.props
                    )
                )
            );

            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{volume_number}")
    public Response modifyVolumeGroup(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("volume_number") int volumeNumber,
        String jsonData
    ) throws IOException
    {
        JsonGenTypes.VolumeGroupModify modifyData = objectMapper.readValue(
            jsonData,
            JsonGenTypes.VolumeGroupModify.class
        );

        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_MOD_VLM_GRP, request),
            () -> ApiCallRcConverter.toResponse(
                ctrlApiCallHandler.modifyVolumeGroup(
                    rscName,
                    volumeNumber,
                    modifyData.override_props,
                    new HashSet<>(modifyData.delete_props),
                    new HashSet<>(modifyData.delete_namespaces)
                ),
                Response.Status.OK
            ),
            true
        );
    }


    @DELETE
    @Path("{volume_number}")
    public Response deleteVolumeGroup(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("volume_number") int volumeNumber
    )
    {
        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_DEL_VLM_GRP, request),
            () -> ApiCallRcConverter.toResponse(
                ctrlApiCallHandler.deleteVolumeGroup(rscName, volumeNumber),
                Response.Status.OK
            ),
            true
        );
    }
}
