package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPhysicalStorageApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolCrtApiCallHandler;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("v1/physical-storage")
@Produces(MediaType.APPLICATION_JSON)
public class PhysicalStorage
{
    private final RequestHelper requestHelper;
    private final CtrlPhysicalStorageApiCallHandler physicalStorageApiCallHandler;
    private final CtrlStorPoolCrtApiCallHandler storPoolCrtApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public PhysicalStorage(
        RequestHelper requestHelperRef,
        CtrlPhysicalStorageApiCallHandler ctrlPhysicalStorageApiCallHandler,
        CtrlStorPoolCrtApiCallHandler ctrlStorPoolCrtApiCallHandler
    )
    {
        requestHelper = requestHelperRef;
        physicalStorageApiCallHandler = ctrlPhysicalStorageApiCallHandler;
        storPoolCrtApiCallHandler = ctrlStorPoolCrtApiCallHandler;
        objectMapper = new ObjectMapper();
    }

    @GET
    public void listPhysicalStorage(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            Mono<Response> answer = physicalStorageApiCallHandler.listPhysicalStorage()
                .subscriberContext(requestHelper.createContext(ApiConsts.API_LST_PHYS_STOR, request))
                .map(physicalStorageMap ->
                {
                    Response resp;
                    final List<JsonGenTypes.PhysicalStorage> physicalStorages =
                        CtrlPhysicalStorageApiCallHandler.groupPhysicalStorageByDevice(physicalStorageMap);

                    Stream<JsonGenTypes.PhysicalStorage> physicalStorageStream = physicalStorages.stream();
                    if (limit > 0)
                    {
                        physicalStorageStream = physicalStorages.stream().skip(offset).limit(limit);
                    }

                    try
                    {
                        resp = Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(physicalStorageStream.collect(Collectors.toList())))
                            .build();
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return resp;
                }).next();

            requestHelper.doFlux(asyncResponse, answer);
        });
    }

    @GET
    @Path("{nodeName}")
    public void getPhysicalStorage(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName
    )
    {
        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            Mono<Response> answer = physicalStorageApiCallHandler.getPhysicalStorage(nodeName)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_LST_PHYS_STOR, request))
                .map(lsBlkEntries -> {
                    List<JsonGenTypes.PhysicalStorageNode> result = lsBlkEntries
                        .stream()
                        .map(CtrlPhysicalStorageApiCallHandler::toPhysicalStorageNode)
                        .collect(Collectors.toList());

                    Response resp;
                    try
                    {
                        resp =  Response
                            .status(Response.Status.OK)
                            .entity(objectMapper.writeValueAsString(result))
                            .build();
                    }
                    catch (JsonProcessingException exc)
                    {
                        exc.printStackTrace();
                        resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                    }
                    return resp;
                }).next();
            requestHelper.doFlux(asyncResponse, answer);
        });
    }

    private Map<String, String> deviceProviderToStorPoolProperty(DeviceProviderKind kind, String pool)
    {
        HashMap<String, String> map = new HashMap<>();

        switch (kind)
        {
            case LVM_THIN:
                map.put(ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_NAME,
                    LvmThinDriverKind.VGName(pool) + "/" + LvmThinDriverKind.LVName(pool));
                break;
            case ZFS:
            case ZFS_THIN:
            case SPDK:
            case REMOTE_SPDK:
            case LVM:
                map.put(ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_NAME, pool);
                break;

            case EXOS: // fall-through for now

            case OPENFLEX_TARGET: // fall-through
            case DISKLESS: // fall-through
            case FILE: // fall-through
            case FILE_THIN: // fall-through
            case FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER: // fall-through
            default:
                //ignore
                break;
        }

        return map;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{nodeName}")
    public void createDevicePool(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        String jsonData
    )
    {
        try
        {
            JsonGenTypes.PhysicalStorageCreate createData = objectMapper
                .readValue(jsonData, JsonGenTypes.PhysicalStorageCreate.class);

            // check if with_storage_pool is given and if storage pool name is valid, before we do unneeded create/del
            if (createData.with_storage_pool != null)
            {
                LinstorParsingUtils.asStorPoolName(createData.with_storage_pool.name);
            }

            final String poolName = CtrlPhysicalStorageApiCallHandler.getDevicePoolName(
                createData.pool_name,
                createData.device_paths
            );

            final DeviceProviderKind deviceProviderKind = LinstorParsingUtils.asProviderKind(createData.provider_kind);
            Flux<ApiCallRc> responses = physicalStorageApiCallHandler.createDevicePool(
                nodeName,
                createData.device_paths,
                deviceProviderKind,
                LinstorParsingUtils.asRaidLevel(createData.raid_level),
                poolName,
                createData.vdo_enable,
                createData.vdo_logical_size_kib,
                createData.vdo_slab_size_kib
            ).subscriberContext(requestHelper.createContext(ApiConsts.API_CREATE_DEVICE_POOL, request));

            if (createData.with_storage_pool != null)
            {
                Map<String, String> storPoolProps = deviceProviderToStorPoolProperty(deviceProviderKind, poolName);
                storPoolProps.putAll(createData.with_storage_pool.props);
                Boolean externalLocking = createData.with_storage_pool.external_locking;
                responses = responses.concatWith(
                    storPoolCrtApiCallHandler.createStorPool(
                        nodeName,
                        createData.with_storage_pool.name,
                        deviceProviderKind,
                        createData.with_storage_pool.shared_space,
                        externalLocking == null ? false : externalLocking,
                        storPoolProps,
                        physicalStorageApiCallHandler.deleteDevicePool(
                            nodeName,
                            createData.device_paths,
                            deviceProviderKind,
                            poolName
                        )
                    ).subscriberContext(requestHelper.createContext(ApiConsts.API_CRT_STOR_POOL, request))
                );
            }

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
}
