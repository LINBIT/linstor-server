package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscToggleDiskApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.apis.ResourceWithPayloadApi;

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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;

@Path("resource-definitions/{rscName}/resources")
@Produces(MediaType.APPLICATION_JSON)
public class Resources
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandler;
    private final CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandler;
    private final CtrlRscToggleDiskApiCallHandler ctrlRscToggleDiskApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public Resources(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandlerRef,
        CtrlRscToggleDiskApiCallHandler ctrlRscToggleDiskApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        ctrlRscDeleteApiCallHandler = ctrlRscDeleteApiCallHandlerRef;
        ctrlRscToggleDiskApiCallHandler = ctrlRscToggleDiskApiCallHandlerRef;

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
            Stream<ResourceApi> rscApiStream = resourceList.getResources().stream();
            if (limit > 0)
            {
                rscApiStream = rscApiStream.skip(offset).limit(limit);
            }

            final List<JsonGenTypes.Resource> rscs = rscApiStream
                .map(rscApi -> Json.apiToResource(rscApi, resourceList.getSatelliteStates()))
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

    private class ResourceWithPayload implements ResourceWithPayloadApi
    {
        private final JsonGenTypes.ResourceCreate rscPayload;

        ResourceWithPayload(JsonGenTypes.ResourceCreate rsc, String rscName)
        {
            if (rsc.resource.flags.contains(ApiConsts.FLAG_DISKLESS))
            {
                for (String layer : rsc.layer_list)
                {
                    if (layer.equalsIgnoreCase("drbd"))
                    {
                        rsc.resource.flags.add(ApiConsts.FLAG_DRBD_DISKLESS);
                    }
                    if (layer.equalsIgnoreCase("nvme"))
                    {
                        rsc.resource.flags.add(ApiConsts.FLAG_NVME_INITIATOR);
                    }
                }
            }

            rscPayload = rsc;
            rscPayload.resource.name = rscName;
        }

        @Override
        public ResourceApi getRscApi()
        {
            return Json.resourceToApi(rscPayload.resource);
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
            List<JsonGenTypes.ResourceCreate> rscList = Arrays.asList(
                objectMapper.readValue(jsonData, JsonGenTypes.ResourceCreate[].class)
            );

            List<ResourceWithPayloadApi> rscWithPayloadApiList = rscList.stream()
                .map(resourceCreateData -> new ResourceWithPayload(resourceCreateData, rscName))
                .collect(Collectors.toList());

            Flux<ApiCallRc> flux = ctrlRscCrtApiCallHandler.createResource(rscWithPayloadApiList)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_CRT_RSC, request));

            requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.CREATED));
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
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
            JsonGenTypes.ResourceCreate rscData = objectMapper.readValue(jsonData, JsonGenTypes.ResourceCreate.class);
            rscData.resource.node_name = nodeName;
            ArrayList<JsonGenTypes.ResourceCreate> rscDatas = new ArrayList<>();
            rscDatas.add(rscData);

            createResource(request, asyncResponse, rscName, objectMapper.writeValueAsString(rscDatas));
        }
        catch (IOException ioExc)
        {
            ApiCallRcRestUtils.handleJsonParseException(ioExc, asyncResponse);
        }
    }

    @PUT
    @Path("{nodeName}")
    public void modifyResource(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName,
        String jsonData
    )
        throws IOException
    {
        JsonGenTypes.ResourceModify modifyData = objectMapper
            .readValue(jsonData, JsonGenTypes.ResourceModify.class);

        Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyRsc(
            null,
            nodeName,
            rscName,
            modifyData.override_props,
            new HashSet<>(modifyData.delete_props),
            new HashSet<>(modifyData.delete_namespaces)
        )
        .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_RSC, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
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

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
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

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
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

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
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

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux));
    }
}
