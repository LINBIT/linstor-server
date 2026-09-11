package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;

@Path("v1/key-value-store")
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
        @PathParam("instance") @Nullable String instanceName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_KVS, request, () ->
        {
            List<JsonGenTypes.KeyValueStore> keyValueStoreList = ctrlApiCallHandler.listKvs().stream()
                .filter(kvsApi -> instanceName == null || kvsApi.getName().equalsIgnoreCase(instanceName))
                .map(Json::apiToKeyValueStore)
                .collect(Collectors.toList());

            Response resp;
            if (instanceName != null && keyValueStoreList.isEmpty())
            {
                resp = RequestHelper.notFoundResponse(
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
            JsonGenTypes.KeyValueStoreModify modifyKeyValueStore = objectMapper.readValue(
                jsonData,
                JsonGenTypes.KeyValueStoreModify.class
            );

            return ApiCallRcRestUtils.toResponse(ctrlApiCallHandler.modifyKvs(
                null,
                instanceName,
                modifyKeyValueStore.override_props,
                new HashSet<>(modifyKeyValueStore.delete_props),
                new HashSet<>(modifyKeyValueStore.delete_namespaces)
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
            ApiCallRcRestUtils.toResponse(
                ctrlApiCallHandler.deleteKvs(null, instanceName), Response.Status.OK
            ), true);
    }
}
