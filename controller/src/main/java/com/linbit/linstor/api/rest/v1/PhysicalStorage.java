package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPhysicalStorageApiCallHandler;

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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("physical-storage")
@Produces(MediaType.APPLICATION_JSON)
public class PhysicalStorage
{
    private final RequestHelper requestHelper;
    private final CtrlPhysicalStorageApiCallHandler physicalStorageApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public PhysicalStorage(
        RequestHelper requestHelperRef,
        CtrlPhysicalStorageApiCallHandler ctrlPhysicalStorageApiCallHandler
    )
    {
        requestHelper = requestHelperRef;
        physicalStorageApiCallHandler = ctrlPhysicalStorageApiCallHandler;
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
                .flatMap(physicalStorageMap ->
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
                    return Mono.just(resp);
                }).next();

            requestHelper.doFlux(asyncResponse, answer);
        });
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

            Flux<ApiCallRc> responses = physicalStorageApiCallHandler.createDevicePool(
                nodeName,
                createData.device_paths,
                LinstorParsingUtils.asProviderKind(createData.provider_kind),
                LinstorParsingUtils.asRaidLevel(createData.raid_level),
                createData.pool_name,
                createData.vdo_enable,
                createData.vdo_logical_size_kib,
                createData.vdo_slab_size_kib
            ).subscriberContext(requestHelper.createContext(ApiConsts.API_CREATE_DEVICE_POOL, request));

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
