package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("key-value-store")
@Produces(MediaType.APPLICATION_JSON)
public class KeyValueStore
{
    private final RequestHelper requestHelper;
    private final ObjectMapper objectMapper;
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public KeyValueStore(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }


    @GET
    public Response listKVStore(
        @Context Request request
    )
    {
        return listKVStore(request, null);
    }

    @GET
    @Path("{instance}")
    public Response listKVStore(
        @Context Request request,
        @PathParam("instance") String instanceName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_KVS, request, () ->
        {
            List<Json.KeyValueStore> keyValueStoreList = ctrlApiCallHandler.listKvs().stream()
                .filter(kvsApi -> instanceName == null || kvsApi.getName().equalsIgnoreCase(instanceName))
                .map(Json.KeyValueStore::new)
                .collect(Collectors.toList());

            Response resp;
            if (instanceName != null && keyValueStoreList.isEmpty())
            {
                resp = requestHelper.notFoundResponse(
                    ApiConsts.FAIL_NOT_FOUND_KVS,
                    String.format("Could not find key value store instance '%s'.", instanceName)
                );
            }
            else
            {
                resp = Response
                    .status(Response.Status.OK)
                    .entity(objectMapper.writeValueAsString(keyValueStoreList))
                    .build();
            }

            return resp;
        }, false);
    }

    @PUT
    @Path("{instance}")
    public Response modifyKVStore(
        @Context Request request,
        @PathParam("instance") String instanceName,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_MOD_KVS, request, () ->
        {
            Json.ModifyKeyValueStore modifyKeyValueStore = objectMapper.readValue(
                jsonData,
                Json.ModifyKeyValueStore.class
            );

            return ApiCallRcConverter.toResponse(ctrlApiCallHandler.modifyKvs(
                null,
                instanceName,
                modifyKeyValueStore.override_props,
                modifyKeyValueStore.delete_props,
                modifyKeyValueStore.delete_namespaces
            ), Response.Status.OK);
        }, true);
    }

    @DELETE
    @Path("{instance}")
    public Response deleteKVStore(
        @Context Request request,
        @PathParam("instance") String instanceName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_DEL_KVS, request, () ->
            ApiCallRcConverter.toResponse(
                ctrlApiCallHandler.deleteKvs(null, instanceName), Response.Status.OK
            ), true);
    }
}
