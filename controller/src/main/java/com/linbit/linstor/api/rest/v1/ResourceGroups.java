package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.objects.ResourceGroup;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("resource-groups")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceGroups
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    ResourceGroups(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listResourceGroups(
        @Context Request request,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceGroups(request, null, limit, offset);
    }

    @GET
    @Path("{rscName}")
    public Response listResourceGroups(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC_GRP, request), () ->
        {
            Stream<ResourceGroup.RscGrpApi> rscGrpStream =
                ctrlApiCallHandler.listResourceGroups().stream()
                    .filter(rscGrpApi -> rscName == null || rscGrpApi.getName().equalsIgnoreCase(rscName));

            if (limit > 0)
            {
                rscGrpStream = rscGrpStream.skip(offset).limit(limit);
            }

            final List<JsonGenTypes.ResourceGroup> rscGrps = rscGrpStream.map(Json::apiToResourceGroup)
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper, ApiConsts.FAIL_NOT_FOUND_RSC_GRP, "Resource group", rscName, rscGrps
            );
        }, false);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createResourceGroup(@Context Request request, String jsonData)
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_CRT_RSC_GRP, request), () ->
        {
            JsonGenTypes.ResourceGroup rscGrp = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ResourceGroup.class
            );
//            final List<VolumeGroupData> vlmGrps =
//                rscGrpCreate.resource_definition.volume_definitions != null ?
//                    rscGrpCreate.resource_definition.volume_definitions : new ArrayList<>();

            List<String> layerDataList = rscGrp.layer_stack;

            AutoSelectFilterApi autoSelectFilter = null;
            if (rscGrp.select_filter != null)
            {
                autoSelectFilter = new AutoSelectFilterPojo(
                    rscGrp.select_filter.place_count,
                    rscGrp.select_filter.storage_pool,
                    rscGrp.select_filter.not_place_with_rsc,
                    rscGrp.select_filter.not_place_with_rsc_regex,
                    rscGrp.select_filter.replicas_on_same,
                    rscGrp.select_filter.replicas_on_different,
                    LinstorParsingUtils.asDeviceLayerKind(rscGrp.select_filter.layer_stack),
                    LinstorParsingUtils.asProviderKind(rscGrp.select_filter.provider_list)
                );
            }

            ApiCallRc apiCallRc = ctrlApiCallHandler.createResourceGroup(
                new RscGrpPojo(
                    null,
                    rscGrp.name,
                    rscGrp.description,
                    LinstorParsingUtils.asDeviceLayerKind(rscGrp.layer_stack),
                    Collections.emptyMap(), // currently props are not supported on creation
                    Collections.emptyList(), // currently VlmGrps are not supported on creation
                    autoSelectFilter
                )
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{rscName}")
    public Response modifyResourceGroup(
        @Context Request request,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_MOD_RSC_GRP, request), () ->
        {
            JsonGenTypes.ResourceGroupModify modifyData =
                objectMapper.readValue(jsonData, JsonGenTypes.ResourceGroupModify.class);

            AutoSelectFilterApi modifyAutoSelectFilter = null;
            if (modifyData.select_filter != null)
            {
                modifyAutoSelectFilter = new AutoSelectFilterPojo(
                    modifyData.select_filter.place_count,
                    modifyData.select_filter.storage_pool,
                    modifyData.select_filter.not_place_with_rsc,
                    modifyData.select_filter.not_place_with_rsc_regex,
                    modifyData.select_filter.replicas_on_same,
                    modifyData.select_filter.replicas_on_different,
                    LinstorParsingUtils.asDeviceLayerKind(modifyData.select_filter.layer_stack),
                    LinstorParsingUtils.asProviderKind(modifyData.select_filter.provider_list)
                );
            }

            ApiCallRc apiCallRc = ctrlApiCallHandler.modifyResourceGroup(
                rscName,
                modifyData.description,
                modifyData.override_props,
                new HashSet<>(modifyData.delete_props),
                new HashSet<>(modifyData.delete_namespaces),
                modifyData.layer_stack,
                modifyAutoSelectFilter
            );
            return ApiCallRcConverter.toResponse(apiCallRc, Response.Status.OK);
        }, true);
    }

    @DELETE
    @Path("{rscName}")
    public Response deleteResourceGroup(
        @Context Request request,
        @PathParam("rscName") String rscName)
    {
        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_DEL_RSC_GRP, request),
            () -> ApiCallRcConverter.toResponse(ctrlApiCallHandler.deleteResourceGroup(rscName), Response.Status.OK),
            true
        );
    }
}
