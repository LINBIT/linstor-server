package com.linbit.linstor.api.rest.v1;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceData;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceModifyData;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("resource-definitions/{rscName}/resources")
@Produces(MediaType.APPLICATION_JSON)
public class Resources
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandler;
    private final CtrlRscToggleDiskApiCallHandler ctrlRscToggleDiskApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public Resources(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandlerRef,
        CtrlRscToggleDiskApiCallHandler ctrlRscToggleDiskApiCallHandlerRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        ctrlRscDeleteApiCallHandler = ctrlRscDeleteApiCallHandlerRef;
        ctrlRscToggleDiskApiCallHandler = ctrlRscToggleDiskApiCallHandlerRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listResources(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listResources(request, rscName, null, limit, offset);
    }


    @GET
    @Path("{nodeName}")
    public Response listResources(
        @Context Request request,
        @PathParam("rscName") String rscName,
        @PathParam("nodeName") String nodeName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_LST_RSC, request), () ->
        {
            ArrayList<String> nodes = new ArrayList<>();
            if (nodeName != null && !nodeName.isEmpty())
            {
                nodes.add(nodeName);
            }
            ResourceList resourceList = ctrlApiCallHandler.listResource(rscName, nodes);
            Stream<Resource.RscApi> rscApiStream = resourceList.getResources().stream();
            if (limit > 0)
            {
                rscApiStream = rscApiStream.skip(offset).limit(limit);
            }

            final List<ResourceData> rscs = rscApiStream
                .map(rscApi -> new ResourceData(rscApi, resourceList.getSatelliteStates()))
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper,
                ApiConsts.FAIL_NOT_FOUND_RSC,
                String.format("Resource '%s' on", rscName),
                nodeName,
                rscs
            );
        }, false);
    }

    @GET
    @Path("{nodeName}/volumes")
    public void listVolumes(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("nodeName") String nodeName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        listVolumes(request, asyncResponse, rscName, nodeName, null, limit, offset);
    }

    @GET
    @Path("{nodeName}/volumes/{vlmNr}")
    public void listVolumes(
        @Context Request request,
        @Suspended AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("nodeName") String nodeName,
        @PathParam("vlmNr") Integer vlmNr,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        ArrayList<String> nodes = new ArrayList<>();
        ArrayList<String> rscNames = new ArrayList<>();
        nodes.add(nodeName);
        rscNames.add(rscName);

        Flux<ApiCallRcWith<ResourceList>> flux = ctrlVlmListApiCallHandler.listVlms(nodes, new ArrayList<>(), rscNames)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_LST_VLM, request));

        requestHelper.doFlux(
            asyncResponse,
            listVolumesApiCallRcWithToResponse(flux, rscName, nodeName, vlmNr, limit, offset)
        );
    }

    private Mono<Response> listVolumesApiCallRcWithToResponse(
        Flux<ApiCallRcWith<ResourceList>> apiCallRcWithFlux,
        final String rscName,
        final String nodeName,
        final Integer vlmNr,
        int limit,
        int offset
    )
    {
        return apiCallRcWithFlux.flatMap(apiCallRcWith ->
        {
            Response resp;
            if (apiCallRcWith.hasApiCallRc())
            {
                resp = ApiCallRcConverter.toResponse(
                    apiCallRcWith.getApiCallRc(),
                    Response.Status.INTERNAL_SERVER_ERROR
                );
            }
            else
            {
                ResourceList resourceList = apiCallRcWith.getValue();
                if (resourceList.isEmpty())
                {
                    resp = RequestHelper.notFoundResponse(
                        ApiConsts.FAIL_NOT_FOUND_RSC,
                        String.format("Resource '%s' not found on node '%s'.", rscName, nodeName)
                    );
                }
                else
                {
                    Stream<? extends Volume.VlmApi> vlmApiStream = resourceList.getResources()
                        .get(0).getVlmList().stream().filter(
                            vlmApi -> vlmNr == null || vlmApi.getVlmNr() == vlmNr
                        );

                    if (limit > 0)
                    {
                        vlmApiStream = vlmApiStream.skip(offset).limit(limit);
                    }

                    final List<Json.VolumeData> vlms = vlmApiStream.map(vlmApi ->
                    {
                        Json.VolumeData vlmData = new Json.VolumeData(vlmApi);

                        Json.VolumeStateData vlmState = null;
                        try
                        {
                            final ResourceName rscNameRes = new ResourceName(rscName);
                            final NodeName linNodeName = new NodeName(nodeName);
                            if (resourceList.getSatelliteStates().containsKey(linNodeName) &&
                                resourceList.getSatelliteStates().get(linNodeName)
                                    .getResourceStates().containsKey(rscNameRes))
                            {
                                SatelliteResourceState satResState = resourceList
                                    .getSatelliteStates()
                                    .get(linNodeName)
                                    .getResourceStates()
                                    .get(rscNameRes);

                                VolumeNumber vlmNumber = new VolumeNumber(vlmApi.getVlmNr());
                                if (satResState.getVolumeStates().containsKey(vlmNumber))
                                {
                                    vlmState = new Json.VolumeStateData();
                                    SatelliteVolumeState satVlmState = satResState.getVolumeStates().get(vlmNumber);
                                    vlmState.disk_state = satVlmState.getDiskState();
                                }
                            }
                        }
                        catch (InvalidNameException | ValueOutOfRangeException ignored)
                        {
                        }
                        vlmData.state = vlmState;
                        return vlmData;
                    })
                        .collect(Collectors.toList());

                    if (vlmNr != null && vlms.isEmpty())
                    {
                        resp = RequestHelper.notFoundResponse(
                            ApiConsts.FAIL_NOT_FOUND_VLM,
                            String.format("Volume '%d' of resource '%s' on node '%s' not found.",
                                vlmNr, rscName, nodeName)
                        );
                    }
                    else
                    {
                        try
                        {
                            resp = Response
                                .status(Response.Status.OK)
                                .entity(objectMapper.writeValueAsString(vlmNr != null ? vlms.get(0) : vlms))
                                .build();
                        }
                        catch (JsonProcessingException exc)
                        {
                            exc.printStackTrace();
                            resp = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
                        }
                    }
                }
            }

            return Mono.just(resp);
        }).next();
    }

    private class ResourceDataWithPayload implements Resource.RscWithPayloadApi
    {
        private final Json.ResourceCreateData rscPayload;

        ResourceDataWithPayload(Json.ResourceCreateData rsc, String rscName)
        {
            rscPayload = rsc;
            rscPayload.resource.name = rscName;
        }

        @Override
        public Resource.RscApi getRscApi()
        {
            return rscPayload.resource.toRscApi();
        }

        @Override
        public List<String> getLayerStack()
        {
            return rscPayload.layer_list;
        }

        @Override
        public Integer getDrbdNodeId()
        {
            return rscPayload.drbd_node_id;
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void createResource(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        try
        {
            List<Json.ResourceCreateData> rscDatas = Arrays.asList(
                objectMapper.readValue(jsonData, Json.ResourceCreateData[].class)
            );

            List<Resource.RscWithPayloadApi> rscWithPayloadApiList = rscDatas.stream()
                .map(resourceCreateData -> new ResourceDataWithPayload(resourceCreateData, rscName))
                .collect(Collectors.toList());

            Flux<ApiCallRc> flux = ctrlRscCrtApiCallHandler.createResource(rscWithPayloadApiList)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_CRT_RSC, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux, Response.Status.CREATED));
        }
        catch (IOException ioExc)
        {
            ApiCallRcConverter.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("{nodeName}")
    public void createResource(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("rscName") String rscName,
        @PathParam("nodeName") String nodeName,
        String jsonData
    )
    {
        try
        {
            // stuff single resource in a array and forward to the multiple resource creator
            Json.ResourceCreateData rscData = objectMapper.readValue(jsonData, Json.ResourceCreateData.class);
            rscData.resource.node_name = nodeName;
            ArrayList<Json.ResourceCreateData> rscDatas = new ArrayList<>();
            rscDatas.add(rscData);

            createResource(request, asyncResponse, rscName, objectMapper.writeValueAsString(rscDatas));
        }
        catch (IOException ioExc)
        {
            ApiCallRcConverter.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Path("{nodeName}")
    public Response modifyResource(
        @Context Request request,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName,
        String jsonData
    )
    {
        return requestHelper.doInScope(requestHelper.createContext(ApiConsts.API_MOD_RSC, request), () ->
        {
            ResourceModifyData modifyData = objectMapper.readValue(jsonData, ResourceModifyData.class);
            return ApiCallRcConverter.toResponse(
                ctrlApiCallHandler.modifyRsc(
                    null,
                    nodeName,
                    rscName,
                    modifyData.override_props,
                    modifyData.delete_props,
                    modifyData.delete_namespaces
                ),
                Response.Status.OK
            );
        }, true);
    }

    @DELETE
    @Path("{nodeName}")
    public void deleteResource(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName
    )
    {
        Flux<ApiCallRc> flux = ctrlRscDeleteApiCallHandler.deleteResource(nodeName, rscName)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_DEL_RSC, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }

    @PUT
    @Path("{nodeName}/toggle-disk/diskless")
    public void toggleDiskDiskless(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName
    )
    {
        toggleDiskDiskless(request, asyncResponse, nodeName, rscName, null);
    }

    @PUT
    @Path("{nodeName}/toggle-disk/diskless/{disklessPool}")
    public void toggleDiskDiskless(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName,
        @PathParam("disklessPool") String disklessPool
    )
    {
        Flux<ApiCallRc> flux = ctrlRscToggleDiskApiCallHandler.resourceToggleDisk(
                nodeName,
                rscName,
                disklessPool,
                null,
                true)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_TOGGLE_DISK, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }

    @PUT
    @Path("{nodeName}/toggle-disk/diskful")
    public void toggleDiskDiskful(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName
    )
    {
        toggleDiskDiskful(request, asyncResponse, nodeName, rscName, null);
    }

    @PUT
    @Path("{nodeName}/toggle-disk/diskful/{storagePool}")
    public void toggleDiskDiskful(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName,
        @PathParam("storagePool") String storagePool
    )
    {
        Flux<ApiCallRc> flux = ctrlRscToggleDiskApiCallHandler.resourceToggleDisk(
                nodeName,
                rscName,
                storagePool,
                null,
                false)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_TOGGLE_DISK, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }

    @PUT
    @Path("{nodeName}/migrate-disk/{fromNode}")
    public void migrateDisk(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("fromNode") String fromNode,
        @PathParam("rscName") String rscName
    )
    {
        migrateDisk(request, asyncResponse, nodeName, fromNode, rscName, null);
    }

    @PUT
    @Path("{nodeName}/migrate-disk/{fromNode}/{storagePool}")
    public void migrateDisk(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("fromNode") String fromNode,
        @PathParam("rscName") String rscName,
        @PathParam("storagePool") String storagePool
    )
    {
        Flux<ApiCallRc> flux = ctrlRscToggleDiskApiCallHandler.resourceToggleDisk(
            nodeName,
            rscName,
            storagePool,
            fromNode,
            false)
            .subscriberContext(requestHelper.createContext(ApiConsts.API_TOGGLE_DISK, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcConverter.mapToMonoResponse(flux));
    }
}
