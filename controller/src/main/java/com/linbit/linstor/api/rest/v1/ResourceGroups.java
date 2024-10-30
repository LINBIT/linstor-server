package com.linbit.linstor.api.rest.v1;

import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.AutoSelectFilter;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes.ResourceGroup;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.logging.ErrorReporter;

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
import org.slf4j.MDC;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("v1/resource-groups")
@Produces(MediaType.APPLICATION_JSON)
public class ResourceGroups
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlRscGrpApiCallHandler ctrlRscGrpApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    public ResourceGroups(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscGrpApiCallHandler ctrlRscGrpApiCallHandlerRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscGrpApiCallHandler = ctrlRscGrpApiCallHandlerRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;
        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listManyResourceGroups(
        @Context Request request,
        @QueryParam("resource_groups") List<String> rscGrpNames,
        @QueryParam("props") List<String> propFilters,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResourceGroupsOneOrMany(request, null, rscGrpNames, propFilters, limit, offset);
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
        return listResourceGroupsOneOrMany(
                request, rscGrpName, Collections.singletonList(rscGrpName), Collections.emptyList(), limit, offset);
    }

    private Response listResourceGroupsOneOrMany(
        Request request,
        String singleRscGrp,
        List<String> rscGrpNames,
        List<String> propFilters,
        int limit,
        int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_RSC_GRP, request, () ->
        {
            Stream<ResourceGroupApi> rscGrpApiStream =
                    ctrlApiCallHandler.listResourceGroups(rscGrpNames, propFilters).stream();
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
        return requestHelper.doInScope(ApiConsts.API_CRT_RSC_GRP, request, () ->
        {
            JsonGenTypes.ResourceGroup rscGrp = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ResourceGroup.class
            );

            AutoSelectFilterApi autoSelectFilter = selectFilterToApi(rscGrp.select_filter);
            ApiCallRc apiCallRc = ctrlApiCallHandler.createResourceGroup(
                new RscGrpPojo(
                    null,
                    rscGrp.name,
                    rscGrp.description,
                    Collections.emptyMap(), // currently props are not supported on creation
                    Collections.emptyList(), // currently VlmGrps are not supported on creation
                    autoSelectFilter,
                    parsePeerSlots(rscGrp.peer_slots)
                )
            );
            return ApiCallRcRestUtils.toResponse(apiCallRc, Response.Status.CREATED);
        }, true);
    }

    private AutoSelectFilterApi selectFilterToApi(AutoSelectFilter selectFilter)
    {
        AutoSelectFilterApi autoSelectFilter = null;
        if (selectFilter != null)
        {
            List<String> storPoolList = selectFilter.storage_pool_list;
            if (
                (storPoolList == null || storPoolList.isEmpty()) &&
                selectFilter.storage_pool != null
            )
            {
                storPoolList = Collections.singletonList(selectFilter.storage_pool);
            }
            autoSelectFilter = new AutoSelectFilterBuilder()
                .setPlaceCount(selectFilter.place_count)
                .setAdditionalPlaceCount(selectFilter.additional_place_count)
                .setNodeNameList(selectFilter.node_name_list)
                .setStorPoolNameList(storPoolList)
                .setStorPoolDisklessNameList(selectFilter.storage_pool_diskless_list)
                .setDoNotPlaceWithRscList(selectFilter.not_place_with_rsc)
                .setDoNotPlaceWithRegex(selectFilter.not_place_with_rsc_regex)
                .setReplicasOnSameList(selectFilter.replicas_on_same)
                .setReplicasOnDifferentList(selectFilter.replicas_on_different)
                .setXReplicasOnDifferentMap(selectFilter.x_replicas_on_different_map)
                .setLayerStackList(selectFilter.layer_stack == null ? null :
                    LinstorParsingUtils.asDeviceLayerKind(selectFilter.layer_stack))
                .setDeviceProviderKinds(selectFilter.provider_list == null ? null :
                    LinstorParsingUtils.asProviderKind(selectFilter.provider_list))
                .setDisklessOnRemaining(selectFilter.diskless_on_remaining)
                .build();
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
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
                    modifyAutoSelectFilter,
                    parsePeerSlots(modifyData.peer_slots)
                );

            requestHelper.doFlux(
                ApiConsts.API_MOD_RSC_GRP,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
    }

    @DELETE
    @Path("{rscGrpName}")
    public Response deleteResourceGroup(
        @Context Request request,
        @PathParam("rscGrpName") String rscGrpName
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_DEL_RSC_GRP,
            request,
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
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.ResourceGroupSpawn rscGrpSpwn = objectMapper.readValue(
                jsonData,
                JsonGenTypes.ResourceGroupSpawn.class
            );
            byte[] rscDfnExtName = rscGrpSpwn.resource_definition_external_name != null ?
                rscGrpSpwn.resource_definition_external_name.getBytes(StandardCharsets.UTF_8) :
                null;

            AutoSelectFilterApi spawnAutoSelectFilter = selectFilterToApi(rscGrpSpwn.select_filter);
            Flux<ApiCallRc> flux = ctrlRscGrpApiCallHandler.spawn(
                rscGrpName,
                rscGrpSpwn.resource_definition_name,
                rscDfnExtName,
                rscGrpSpwn.volume_sizes,
                spawnAutoSelectFilter,
                rscGrpSpwn.partial,
                rscGrpSpwn.definitions_only,
                parsePeerSlots(rscGrpSpwn.peer_slots),
                rscGrpSpwn.volume_passphrases,
                rscGrpSpwn.resource_definition_props
            );

            requestHelper.doFlux(
                ApiConsts.API_SPAWN_RSC_DFN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED)
            );
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
        MDC.put(ErrorReporter.LOGID, ErrorReporter.getNewLogId());
        Mono<Response> flux = ctrlRscGrpApiCallHandler.queryMaxVlmSize(
            rscGrpName
        )
            .flatMap(apiCallRcWith ->
            {
                MDC.setContextMap(MDC.getCopyOfContextMap());
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
                    maxVolumeSizesData.default_max_oversubscription_ratio =
                        LinStor.OVERSUBSCRIPTION_RATIO_DEFAULT;

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

        requestHelper.doFlux(
            ApiConsts.API_QRY_MAX_VLM_SIZE,
            request,
            asyncResponse,
            flux
        );
    }

    @POST
    @Path("{rscGrpName}/query-size-info")
    public void querySizeInfo(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscGrpName") String rscGrpName,
        String jsonData
    )
        throws JsonProcessingException
    {
        String nonEmptyJsonData = jsonData == null || jsonData.isEmpty() ? "{}" : jsonData;
        JsonGenTypes.QuerySizeInfoRequest qsiReq = objectMapper.readValue(
            nonEmptyJsonData,
            JsonGenTypes.QuerySizeInfoRequest.class
        );

        Mono<Response> flux = ctrlRscGrpApiCallHandler.querySizeInfo(
            Json.querySizeInfoReqToPojo(rscGrpName, qsiReq)
        )
            .onErrorResume(
                ApiRcException.class,
                apiExc -> Flux.just(
                    new ApiCallRcWith<>(apiExc.getApiCallRc(), null)
                )
            )
            .flatMap(apiCallRcWith ->
            {
                Response resp;
                JsonGenTypes.QuerySizeInfoResponse qsiResp = Json.pojoToQuerySizeInfoResp(
                    apiCallRcWith.getValue(),
                    apiCallRcWith.getApiCallRc()
                );

                try
                {
                    resp = Response
                        .status(Response.Status.OK)
                        .entity(objectMapper.writeValueAsString(qsiResp))
                        .build();
                }
                catch (JsonProcessingException exc)
                {
                    exc.printStackTrace();
                    resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                }

                return Mono.just(resp);
            })
            .next();

        requestHelper.doFlux(
            ApiConsts.API_QRY_SIZE_INFO,
            request,
            asyncResponse,
            flux
        );
    }

    @GET
    @Path("properties/info")
    public Response listCtrlPropsInfo(
        @Context Request request
    )
    {
        return requestHelper.doInScope(
            ApiConsts.API_LST_PROPS_INFO, request,
            () -> Response.status(Response.Status.OK)
                .entity(
                    objectMapper
                        .writeValueAsString(
                            ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.RESOURCE_DEFINITION)
                        )
                )
                .build(),
            false
        );
    }

    @POST
    @Path("{rscGrpName}/adjust")
    public void adjust(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscGrpName") String rscGrpName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.ResourceGroupAdjust data;
            if (jsonData != null && !jsonData.isEmpty())
            {
                data = objectMapper.readValue(
                    jsonData,
                    JsonGenTypes.ResourceGroupAdjust.class
                );
            }
            else
            {
                data = new JsonGenTypes.ResourceGroupAdjust();
            }
            AutoSelectFilterApi adjustAutoSelectFilter = selectFilterToApi(data.select_filter);
            Flux<ApiCallRc> flux = ctrlRscGrpApiCallHandler
                .adjust(rscGrpName, adjustAutoSelectFilter);

            requestHelper.doFlux(
                ApiConsts.API_ADJUST_RSC_GRP,
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

    @POST
    @Path("adjustall")
    public void adjustAll(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.ResourceGroupAdjust data;
            if (jsonData != null && !jsonData.isEmpty())
            {
                data = objectMapper.readValue(
                    jsonData,
                    JsonGenTypes.ResourceGroupAdjust.class
                );
            }
            else
            {
                data = new JsonGenTypes.ResourceGroupAdjust();
            }
            AutoSelectFilterApi adjustAutoSelectFilter = selectFilterToApi(data.select_filter);
            Flux<ApiCallRc> flux = ctrlRscGrpApiCallHandler.adjustAll(adjustAutoSelectFilter);

            requestHelper.doFlux(
                ApiConsts.API_ADJUST_RSC_GRP,
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

    public static Short parsePeerSlots(Integer peerSlotsRef)
    {
        Short peerSlots = null;
        if (peerSlotsRef != null)
        {
            if (peerSlotsRef > MetaData.DRBD_MAX_PEERS)
            {
                throw new IllegalArgumentException(
                    "given peer_slots is larger then allowed: " + peerSlotsRef + ". Allowed: " + MetaData.DRBD_MAX_PEERS
                );
            }
            else if (peerSlotsRef < 0)
            {
                throw new IllegalArgumentException(
                    "peer_slots must not be smaller than 0. Received " + peerSlotsRef
                );
            }
            else
            {
                peerSlots = peerSlotsRef.shortValue();
            }
        }
        return peerSlots;
    }
}
