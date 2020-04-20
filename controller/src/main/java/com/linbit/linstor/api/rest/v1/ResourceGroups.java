package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.AutoSelectFilter;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ResourceGroup;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityAutoPoolSelectorUtils;
import com.linbit.linstor.core.apis.ResourceGroupApi;

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
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("resource-groups")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceGroups
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlRscGrpApiCallHandler ctrlRscGrpApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public ResourceGroups(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscGrpApiCallHandler ctrlRscGrpApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscGrpApiCallHandler = ctrlRscGrpApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listManyResourceGroups(
        @Context Request request,
        @QueryParam("resource_groups") List<String> rscGrpNames,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceGroupsOneOrMany(request, null, rscGrpNames, limit, offset);
    }

    @GET
    @Path("{rscGrpName}")
    public Response listSingleResourceGroup(
        @Context Request request,
        @PathParam("rscGrpName") String rscGrpName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceGroupsOneOrMany(request, rscGrpName, Collections.singletonList(rscGrpName), limit, offset);
    }

    private Response listResourceGroupsOneOrMany(
        Request request,
        String singleRscGrp,
        List<String> rscGrpNames,
        int limit,
        int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC_GRP, request), () ->
        {
            Stream<ResourceGroupApi> rscGrpApiStream = ctrlApiCallHandler.listResourceGroups(rscGrpNames).stream();
            if (limit > 0)
            {
                rscGrpApiStream = rscGrpApiStream.skip(offset).limit(limit);
            }
            List<ResourceGroup> rscGrpDataList = rscGrpApiStream
                .map(Json::apiToResourceGroup)
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper,
                ApiConsts.FAIL_NOT_FOUND_RSC_GRP,
                "Resource group",
                singleRscGrp,
                rscGrpDataList
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

            AutoSelectFilterApi autoSelectFilter = selectFilterToApi(rscGrp.select_filter);
            ApiCallRc apiCallRc = ctrlApiCallHandler.createResourceGroup(
                new RscGrpPojo(
                    null,
                    rscGrp.name,
                    rscGrp.description,
                    Collections.emptyMap(), // currently props are not supported on creation
                    Collections.emptyList(), // currently VlmGrps are not supported on creation
                    autoSelectFilter
                )
            );
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    private AutoSelectFilterApi selectFilterToApi(AutoSelectFilter select_filter)
    {
        AutoSelectFilterApi autoSelectFilter = null;
        if (select_filter != null)
        {
            List<String> storPoolList = select_filter.storage_pool_list;
            if (
                (storPoolList == null || storPoolList.isEmpty()) &&
                    select_filter.storage_pool != null &&
                    !select_filter.storage_pool.isEmpty()
            )
            {
                storPoolList = Collections.singletonList(select_filter.storage_pool);
            }
            autoSelectFilter = new AutoSelectFilterPojo(
                select_filter.place_count,
                select_filter.node_name_list,
                storPoolList,
                select_filter.not_place_with_rsc,
                select_filter.not_place_with_rsc_regex,
                select_filter.replicas_on_same,
                select_filter.replicas_on_different,
                select_filter.layer_stack == null ? null :
                    LinstorParsingUtils.asDeviceLayerKind(select_filter.layer_stack),
                select_filter.provider_list == null ? null :
                    LinstorParsingUtils.asProviderKind(select_filter.provider_list),
                select_filter.diskless_on_remaining
            );
        }

        return autoSelectFilter;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{rscGrpName}")
    public void modifyResourceGroup(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscGrpName") String rscGrpName,
        String jsonData
    )
        throws IOException
    {
        JsonGenTypes.ResourceGroupModify modifyData = objectMapper.readValue(
            jsonData,
            JsonGenTypes.ResourceGroupModify.class
        );
        AutoSelectFilterApi modifyAutoSelectFilter = selectFilterToApi(modifyData.select_filter);
        Flux<ApiCallRc> flux = ctrlRscGrpApiCallHandler.modify(
            rscGrpName,
            modifyData.description,
            modifyData.override_props,
            new HashSet<>(modifyData.delete_props),
            new HashSet<>(modifyData.delete_namespaces),
            modifyAutoSelectFilter
        )
            .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_RSC_GRP, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
    }

    @DELETE
    @Path("{rscGrpName}")
    public Response deleteResourceGroup(
        @Context Request request,
        @PathParam("rscGrpName") String rscGrpName
    )
    {
        return requestHelper.doInScope(
            requestHelper.createContext(ApiConsts.API_DEL_RSC_GRP, request),
            () -> ApiCallRcRestUtils.toResponse(
                ctrlApiCallHandler.deleteResourceGroup(rscGrpName),
                Response.Status.OK
            ),
            true
        );
    }

    @POST
    @Path("{rscGrpName}/spawn")
    @Consumes(MediaType.APPLICATION_JSON)
    public void spawnResourceDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscGrpName") String rscGrpName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.ResourceGroupSpawn rscGrpSpwn = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ResourceGroupSpawn.class
            );
            byte[] rscDfnExtName = rscGrpSpwn.resource_definition_external_name != null ?
                rscGrpSpwn.resource_definition_external_name.getBytes(StandardCharsets.UTF_8) :
                null;

            Flux<ApiCallRc> flux = ctrlRscGrpApiCallHandler.spawn(
                rscGrpName,
                rscGrpSpwn.resource_definition_name,
                rscDfnExtName,
                rscGrpSpwn.volume_sizes,
                rscGrpSpwn.partial,
                rscGrpSpwn.definitions_only
            )
                .subscriberContext(requestHelper.createContext(ApiConsts.API_SPAWN_RSC_DFN, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED));
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @GET
    @Path("{rscGrpName}/query-max-volume-size")
    public void queryMaxVolumeSize(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscGrpName") String rscGrpName
    )
    {
        Mono<Response> flux = ctrlRscGrpApiCallHandler.queryMaxVlmSize(
            rscGrpName
        )
            .subscriberContext(requestHelper.createContext(ApiConsts.API_QRY_MAX_VLM_SIZE, request))
            .flatMap(apiCallRcWith ->
            {
                Response resp;
                if (apiCallRcWith.hasApiCallRc())
                {
                    resp = ApiCallRcRestUtils.toResponse(
                        apiCallRcWith.getApiCallRc(),
                        Response.Status.INTERNAL_SERVER_ERROR
                    );
                }
                else
                {
                    List<MaxVlmSizeCandidatePojo> maxVlmSizeCandidates = apiCallRcWith.getValue();
                    JsonGenTypes.MaxVolumeSizes maxVolumeSizesData = Json.pojoToMaxVolumeSizes(maxVlmSizeCandidates);
                    maxVolumeSizesData.default_max_oversubscription_ratio = FreeCapacityAutoPoolSelectorUtils.DEFAULT_MAX_OVERSUBSCRIPTION_RATIO;

                    try
                    {
                        resp = Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(maxVolumeSizesData))
                            .build();
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                    }
                }

                return Mono.just(resp);
            }).next();

        requestHelper.doFlux(asyncResponse, flux);
    }

}
