package com.linbit.linstor.api.rest.v1;

import com.linbit.InvalidNameException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceData;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceModifyData;
import com.linbit.linstor.api.rest.v1.serializer.Json.ResourceStateData;

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
    private final ObjectMapper objectMapper;

    @Inject
    public Resources(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlRscCrtApiCallHandler ctrlRscCrtApiCallHandlerRef,
        CtrlRscDeleteApiCallHandler ctrlRscDeleteApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlRscCrtApiCallHandler = ctrlRscCrtApiCallHandlerRef;
        ctrlRscDeleteApiCallHandler = ctrlRscDeleteApiCallHandlerRef;

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

            final List<ResourceData> rscs = rscApiStream.map(rscApi ->
                {
                    ResourceData rscData = new ResourceData();
                    rscData.name = rscApi.getName();
                    rscData.node_name = rscApi.getNodeName();
                    rscData.node_id = rscApi.getLocalRscNodeId();
                    rscData.flags = FlagsHelper.toStringList(Resource.RscFlags.class, rscApi.getFlags());
                    rscData.props = rscApi.getProps();
                    ResourceStateData rscState = null;
                    try
                    {
                        final ResourceName rscNameRes = new ResourceName(rscApi.getName());
                        final NodeName linNodeName = new NodeName(rscApi.getNodeName());
                        if (resourceList.getSatelliteStates().containsKey(linNodeName) &&
                            resourceList.getSatelliteStates().get(linNodeName)
                                .getResourceStates().containsKey(rscNameRes))
                        {
                            rscState = new ResourceStateData();
                            rscState.in_use = resourceList.getSatelliteStates().get(linNodeName)
                                .getResourceStates().get(rscNameRes).isInUse();
                        }
                    }
                    catch (InvalidNameException ignored)
                    {
                    }
                    rscData.state = rscState;
                    return rscData;
                })
                .collect(Collectors.toList());

            return Response
                .status(Response.Status.OK)
                .entity(objectMapper.writeValueAsString(rscs))
                .build();
        }, false);
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
            List<ResourceData> rscDatas = Arrays.asList(objectMapper.readValue(jsonData, ResourceData[].class));
            List<Resource.RscApi> rscApiList = new ArrayList<>();

            for (ResourceData rscData : rscDatas)
            {
                RscPojo rsc = new RscPojo(
                    rscName,
                    rscData.node_name,
                    FlagsHelper.fromStringList(Resource.RscFlags.class, rscData.flags),
                    rscData.props,
                    Boolean.TRUE.equals(rscData.override_node_id) ? rscData.node_id : null
                );
                rscApiList.add(rsc);
            }

            Flux<ApiCallRc> flux = ctrlRscCrtApiCallHandler.createResource(rscApiList)
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
            ResourceData rscData = objectMapper.readValue(jsonData, ResourceData.class);
            rscData.node_name = nodeName;
            ArrayList<ResourceData> rscDatas = new ArrayList<>();
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
}
