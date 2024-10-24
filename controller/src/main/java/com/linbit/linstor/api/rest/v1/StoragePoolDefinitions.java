package com.linbit.linstor.api.rest.v1;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.utils.ApiCallRcRestUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsInfoApiCallHandler;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
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
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Path("v1/storage-pool-definitions")
@Produces(MediaType.APPLICATION_JSON)
public class StoragePoolDefinitions
{
    private final RequestHelper requestHelper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final ObjectMapper objectMapper;
    private final CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandler;

    @Inject
    public StoragePoolDefinitions(
        RequestHelper requestHelperRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlPropsInfoApiCallHandler ctrlPropsInfoApiCallHandlerRef
    )
    {
        requestHelper = requestHelperRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlPropsInfoApiCallHandler = ctrlPropsInfoApiCallHandlerRef;

        objectMapper = new ObjectMapper();
    }

    @GET
    public Response listStoragePoolDefinitions(
        @Context Request request,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return listStoragePoolDefinitions(request, null, limit, offset);
    }

    @GET
    @Path("{storagePool}")
    public Response listStoragePoolDefinitions(
        @Context Request request,
        @PathParam("storagePool") String storagePoolName,
        @DefaultValue("0") @QueryParam("limit") int limit,
        @DefaultValue("0") @QueryParam("offset") int offset
    )
    {
        return requestHelper.doInScope(ApiConsts.API_LST_STOR_POOL_DFN, request, () ->
        {
            Stream<StorPoolDefinitionApi> storPoolDfnStream = ctrlApiCallHandler.listStorPoolDefinition()
                .stream()
                .filter(storApi -> storagePoolName == null || storApi.getName().equalsIgnoreCase(storagePoolName));

            if (limit > 0)
            {
                storPoolDfnStream = storPoolDfnStream.skip(offset).limit(limit);
            }

            final List<JsonGenTypes.StoragePoolDefinition> storPoolDfns = storPoolDfnStream
                .map(Json::storPoolDfnApiToStoragePoolDefinition)
                .collect(Collectors.toList());

            return RequestHelper.queryRequestResponse(
                objectMapper,
                ApiConsts.FAIL_NOT_FOUND_STOR_POOL_DFN,
                "StoragePoolDefinition",
                storagePoolName,
                storPoolDfns
            );
        }, false);
    }

    @POST
    public Response createStoragePoolDefinition(
        @Context Request request,
        String jsonData
    )
    {
        return requestHelper.doInScope(ApiConsts.API_CRT_STOR_POOL_DFN, request, () ->
        {
            JsonGenTypes.StoragePoolDefinition data = objectMapper
                .readValue(jsonData, JsonGenTypes.StoragePoolDefinition.class);

            return ApiCallRcRestUtils.toResponse(
                ctrlApiCallHandler.createStoragePoolDefinition(data.storage_pool_name, data.props),
                Response.Status.CREATED
            );
        }, true);
    }

    @PUT
    @Path("{storagePool}")
    public void modifyStoragePoolDefinition(
        @Context Request request,
        @Suspended final AsyncResponse asyncResponse,
        @PathParam("storagePool") String storagePoolName,
        String jsonData
    )
        throws IOException
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            JsonGenTypes.StoragePoolDefinitionModify data = objectMapper
                .readValue(jsonData, JsonGenTypes.StoragePoolDefinitionModify.class);

            Flux<ApiCallRc> flux = ctrlApiCallHandler.modifyStorPoolDfn(
                    null,
                    storagePoolName,
                    data.override_props,
                    new HashSet<>(data.delete_props),
                    new HashSet<>(data.delete_namespaces)
                );

            requestHelper.doFlux(
                ApiConsts.API_MOD_STOR_POOL_DFN,
                request,
                asyncResponse,
                ApiCallRcRestUtils.mapToMonoResponse(flux, Response.Status.OK)
            );
        }
    }

    @DELETE
    @Path("{storagePool}")
    public Response deleteStoragePoolDefinition(
        @Context Request request,
        @PathParam("storagePool") String storagePoolName
    )
    {
        return requestHelper.doInScope(ApiConsts.API_DEL_STOR_POOL_DFN, request, () ->
        {
            return ApiCallRcRestUtils.toResponse(
                ctrlApiCallHandler.deleteStoragePoolDefinition(storagePoolName),
                Response.Status.OK
            );
        }, true);
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
                            ctrlPropsInfoApiCallHandler.listFilteredProps(LinStorObject.STOR_POOL_DFN)
                        )
                )
                .build(),
            false
        );
    }
}
