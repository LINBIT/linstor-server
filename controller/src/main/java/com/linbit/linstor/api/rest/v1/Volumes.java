package com.linbit.linstor.api.rest.v1;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Path("resource-definitions/{rscName}/resources/{nodeName}/volumes")
@Produces(MediaType.APPLICATION_JSON)
public class Volumes
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final ObjectMapper objectMapper;

    @Inject
    public Volumes(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
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
    @Path("{vlmNr}")
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

        RequestHelper.safeAsyncResponse(asyncResponse, () ->
        {
            Flux<ResourceList> flux = ctrlVlmListApiCallHandler.listVlms(nodes, new ArrayList<>(), rscNames)
                .subscriberContext(requestHelper.createContext(ApiConsts.API_LST_VLM, request));

            requestHelper.doFlux(
                asyncResponse,
                listVolumesApiCallRcWithToResponse(flux, rscName, nodeName, vlmNr, limit, offset)
            );
        });
    }

    private Mono<Response> listVolumesApiCallRcWithToResponse(
        Flux<ResourceList> resourceListFlux,
        final String rscName,
        final String nodeName,
        final Integer vlmNr,
        int limit,
        int offset
    )
    {
        return resourceListFlux.flatMap(resourceList ->
        {
            Response resp;
            if (resourceList.isEmpty())
            {
                resp = RequestHelper.notFoundResponse(
                    ApiConsts.FAIL_NOT_FOUND_RSC,
                    String.format("Resource '%s' not found on node '%s'.", rscName, nodeName)
                );
            }
            else
            {
                Stream<? extends VolumeApi> vlmApiStream = resourceList.getResources()
                    .get(0).getVlmList().stream().filter(
                        vlmApi -> vlmNr == null || vlmApi.getVlmNr() == vlmNr
                    );

                if (limit > 0)
                {
                    vlmApiStream = vlmApiStream.skip(offset).limit(limit);
                }

                final List<JsonGenTypes.Volume> vlms = vlmApiStream.map(vlmApi ->
                {
                    JsonGenTypes.Volume vlmData = Json.apiToVolume(vlmApi);

                    JsonGenTypes.VolumeState vlmState = null;
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
                                vlmState = new JsonGenTypes.VolumeState();
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

            return Mono.just(resp);
        }).next();
    }

    @PUT
    @Path("{vlmNr}")
    public void modifyVolume(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("nodeName") String nodeName,
        @PathParam("rscName") String rscName,
        @PathParam("vlmNr") Integer vlmNr,
        String jsonData
    )
        throws IOException
    {
        JsonGenTypes.VolumeModify modifyData = objectMapper
            .readValue(jsonData, JsonGenTypes.VolumeModify.class);

        Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyVlm(
            null,
            nodeName,
            rscName,
            vlmNr,
            modifyData.override_props,
            new HashSet<>(modifyData.delete_props),
            new HashSet<>(modifyData.delete_namespaces)
        )
        .subscriberContext(requestHelper.createContext(ApiConsts.API_MOD_VLM, request));

        requestHelper.doFlux(asyncResponse, ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK));
    }
}
