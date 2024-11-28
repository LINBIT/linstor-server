package com.linbit.linstor.layer.storage.spdk.utils;

import com.linbit.ImplementationError;
import com.linbit.SizeConv;
import com.linbit.SizeConv.SizeUnit;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.storage.spdk.SpdkCommands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.utils.PropsUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
public class SpdkRemoteCommands implements SpdkCommands<JsonNode>
{
    private static final String KEY_HOST = StorageConstants.CONFIG_REMOTE_SPDK_API_HOST_KEY;
    private static final String KEY_PORT = StorageConstants.CONFIG_REMOTE_SPDK_API_PORT_KEY;
    private static final String KEY_USER_NAME = StorageConstants.CONFIG_REMOTE_SPDK_USER_NAME_KEY;
    private static final String KEY_USER_NAME_ENV = StorageConstants.CONFIG_REMOTE_SPDK_USER_NAME_ENV_KEY;
    private static final String KEY_USER_PW = StorageConstants.CONFIG_REMOTE_SPDK_USER_PW_KEY;
    private static final String KEY_USER_PW_ENV = StorageConstants.CONFIG_REMOTE_SPDK_USER_PW_ENV_KEY;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final StltConfigAccessor stltConfigAccessor;

    private final RestHttpClient restClient;
    private ReadOnlyProps localNodeProps;

    @Inject
    public SpdkRemoteCommands(
        @DeviceManagerContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        StltConfigAccessor stltConfigAccessorRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        stltConfigAccessor = stltConfigAccessorRef;

        restClient = new RestHttpClient(errorReporterRef);
    }

    public void setLocalNodeProps(ReadOnlyProps localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    @Override
    public Iterator<JsonNode> getJsonElements(JsonNode node)
        throws StorageException
    {
        return node.get("result").elements();
    }

    @Override
    public JsonNode lvs() throws StorageException, AccessDeniedException
    {
        return request("bdev_get_bdevs").getData();
    }

    @Override
    public JsonNode lvsByName(String name) throws StorageException, AccessDeniedException
    {
        return request(
            "bdev_get_bdevs",
            map("name", name)
        ).getData();
    }

    @Override
    public JsonNode getLvolStores() throws StorageException, AccessDeniedException
    {
        return request(
            "bdev_lvol_get_lvstores"
        ).getData();
    }

    @Override
    public JsonNode createFat(
        String volumeGroup,
        String vlmId,
        long size,
        String... additionalParameters
    )
        throws StorageException, AccessDeniedException
    {
        return create(volumeGroup, vlmId, size, false, additionalParameters);
    }

    private JsonNode create(
        String volumeGroup,
        String vlmId,
        long size,
        boolean thin,
        String... additionalParameters
    )
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("lvs_name", volumeGroup);
        params.put("lvol_name", vlmId);
        params.put(
            "size",
            SizeConv.convert(size, SizeUnit.UNIT_KiB, SizeUnit.UNIT_B)
        );
        params.put("thin_provision", thin);

        return request("bdev_lvol_create", params).getData();
    }

    @Override
    public JsonNode createSnapshot(
        String fullQualifiedVlmIdRef,
        String snapName
    )
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("lvol_name", fullQualifiedVlmIdRef);
        params.put("snapshot_name", snapName);

        return request("bdev_lvol_snapshot", params).getData();
    }

    @Override
    public JsonNode restoreSnapshot(String fullQualifiedSnapName, String newVlmId)
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("snapshot_name", fullQualifiedSnapName);
        params.put("clone_name", newVlmId);

        return request("bdev_lvol_clone", params).getData();
    }

    @Override
    public JsonNode decoupleParent(String fullQualifiedIdentifierRef) throws StorageException, AccessDeniedException
    {
        return request(
            "bdev_lvol_decouple_parent",
            map("name", fullQualifiedIdentifierRef)
        ).getData();
    }

    @Override
    public JsonNode clone(String fullQualifiedSourceSnapNameRef, String lvTargetIdRef)
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("snapshot_name", fullQualifiedSourceSnapNameRef);
        params.put("clone_name", lvTargetIdRef);
        return request(
            "bdev_lvol_clone",
            params
        ).getData();
    }

    @Override
    public JsonNode delete(String volumeGroup, String vlmId)
        throws StorageException, AccessDeniedException
    {
        return request(
            "bdev_lvol_delete",
            map("name", volumeGroup + File.separator + vlmId)
        ).getData();
    }

    @Override
    public JsonNode resize(String volumeGroup, String vlmId, long size)
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("name", volumeGroup + File.separator + vlmId);
        params.put(
            "size",
            SizeConv.convert(size, SizeUnit.UNIT_KiB, SizeUnit.UNIT_B)
        );
        return request("bdev_lvol_resize", params).getData();
    }

    @Override
    public JsonNode rename(String volumeGroup, String vlmCurrentId, String vlmNewId)
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("old_name", volumeGroup + File.separator + vlmCurrentId);
        params.put("new_name", vlmNewId);
        return request("bdev_lvol_rename", params).getData();
    }

    @Override
    public void ensureTransportExists(String type)
        throws StorageException, AccessDeniedException
    {
        JsonNode response = request("nvmf_get_transports").getData();
        Iterator<JsonNode> resultElements = getJsonElements(response);

        if (!typeExists(type, resultElements))
        {
            request("nvmf_create_transport", map("trtype", type));
        }
    }

    public static boolean typeExists(String type, Iterator<JsonNode> resultElements)
    {
        boolean requiredTypeFound = false;
        while (resultElements.hasNext())
        {
            JsonNode result = resultElements.next();
            if (result.get("trtype").asText().equalsIgnoreCase(type))
            {
                requiredTypeFound = true;
                break;
            }
        }
        return requiredTypeFound;
    }

    @Override
    public JsonNode getNvmfSubsystems() throws StorageException, AccessDeniedException
    {
        return request("nvmf_get_subsystems").getData();
    }

    @Override
    public JsonNode nvmSubsystemCreate(String subsystemNameRef) throws StorageException, AccessDeniedException
    {
        Map<String, Object> data = map("nqn", subsystemNameRef);
        data.put("allow_any_host", true);
        return request("nvmf_create_subsystem", data).getData();
    }

    @Override
    public JsonNode nvmfSubsystemAddListener(
        String subsystemNameRef,
        String transportTypeRef,
        String addressRef,
        String addressType,
        String portRef
    )
        throws StorageException, AccessDeniedException
    {

        Map<String, Object> data = map("nqn", subsystemNameRef);

        HashMap<String, Object> listenAddressMap = map("trtype", transportTypeRef);
        listenAddressMap.put("traddr", addressRef);
        listenAddressMap.put("adrfam", addressType);
        listenAddressMap.put("trsvcid", portRef);

        data.put("listen_address", listenAddressMap);
        return request("nvmf_subsystem_add_listener", data).getData();
    }

    @Override
    public JsonNode nvmfSubsystemAddNs(String nqn, String bdevName)
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> data = map("nqn", nqn);

        HashMap<String, Object> namespaceMap = map("bdev_name", bdevName);
        data.put("namespace", namespaceMap);

        return request("nvmf_subsystem_add_ns", data).getData();
    }

    @Override
    public JsonNode nvmfDeleteSubsystem(String subsystemNameRef) throws StorageException, AccessDeniedException
    {
        return request(
            "delete_nvmf_subsystem",
            map("nqn", subsystemNameRef)
        ).getData();
    }

    @Override
    public JsonNode nvmfSubsystemRemoveNamespace(String subsystemNameRef, int namespaceNrRef)
        throws StorageException, AccessDeniedException
    {
        HashMap<String, Object> params = map("nqn", subsystemNameRef);
        params.put("nsid", namespaceNrRef);

        return request("nvmf_subsystem_remove_ns", getPrioProps(null), params).getData();
    }

    private RestResponse<JsonNode> request(String method, PriorityProps prioProps, Map<String, Object> params)
        throws StorageException
    {
        return request(errorReporter, restClient, method, prioProps, params);
    }

    private RestResponse<JsonNode> request(String method) throws StorageException, AccessDeniedException
    {
        return request(method, getPrioProps(null), null);
    }

    private RestResponse<JsonNode> request(String method, Map<String, Object> params)
        throws StorageException, AccessDeniedException
    {
        return request(errorReporter, restClient, method, getPrioProps(null), params);
    }

    private static RestResponse<JsonNode> request(
        ErrorReporter errorReporter,
        RestClient restClient,
        String method,
        PriorityProps prioProps,
        Map<String, Object> params
    )
        throws StorageException
    {
        RestResponse<JsonNode> response;
        HashMap<String, Object> data = new HashMap<>();
        data.put("jsonrpc", "2.0");
        data.put("method", method);
        data.put("id", 42);
        if (params != null)
        {
            data.put("params", params);
        }

        String url = getUrl(prioProps);
        String jsonString;
        try
        {
            jsonString = OBJECT_MAPPER.writeValueAsString(data);
        }
        catch (JsonProcessingException exc1)
        {
            throw new ImplementationError("Exception while serializing request payload", exc1);
        }
        try
        {
            errorReporter.logTrace("Sending request to %s, data: %s", url, jsonString);
            response = restClient.execute(
                null,
                RestOp.POST, // everything is a POST request
                url,
                getHeaders(prioProps),
                jsonString,
                Collections.singletonList(HttpHeader.HTTP_OK),
                JsonNode.class
            );

            JsonNode responseData = response.getData();
            JsonNode errorNode = responseData.get("error");
            if (errorNode != null)
            {
                throw new StorageException(
                    "Request failed: URL: " + url + ", data: " + jsonString + ", ErrorCode: " + errorNode.get("code") +
                        ", Error message: " + errorNode.get("message")
                );
            }

        }
        catch (IOException exc)
        {
            throw new StorageException("Request failed: URL " + url + ", data: " + jsonString, exc);
        }
        return response;

    }

    private static String getUrl(PriorityProps prioProps)
    {
        StringBuilder urlBuilder = new StringBuilder("http://"); // for now...
        urlBuilder.append(get(prioProps, KEY_HOST));
        String port = get(prioProps, KEY_PORT);
        if (port != null)
        {
            urlBuilder.append(":").append(port);
        }
        return urlBuilder.toString();
    }

    private static Map<String, String> getHeaders(PriorityProps prioPropsRef)
    {
        return HttpHeader.newBuilder()
            .setAuth(
                PropsUtils.getPropOrEnv(
                    prioPropsRef,
                    KEY_USER_NAME,
                    StorageConstants.NAMESPACE_STOR_DRIVER,
                    KEY_USER_NAME_ENV,
                    StorageConstants.NAMESPACE_STOR_DRIVER
                ),
                PropsUtils.getPropOrEnv(
                    prioPropsRef,
                    KEY_USER_PW,
                    StorageConstants.NAMESPACE_STOR_DRIVER,
                    KEY_USER_PW_ENV,
                    StorageConstants.NAMESPACE_STOR_DRIVER
                )
            )
            .setJsonContentType()
            .build();
    }

    private static String get(PriorityProps prioPropsRef, String key)
    {
        return prioPropsRef.getProp(key, StorageConstants.NAMESPACE_STOR_DRIVER);
    }

    private PriorityProps getPrioProps(StorPool storPoolRef) throws AccessDeniedException
    {
        PriorityProps prioProps = new PriorityProps();
        if (storPoolRef != null)
        {
            prioProps.addProps(storPoolRef.getProps(sysCtx), "Storage pool: " + storPoolRef.getName());
        }
        prioProps.addProps(localNodeProps, "Local node");
        prioProps.addProps(stltConfigAccessor.getReadonlyProps(), "Controller");

        return prioProps;
    }

    private static HashMap<String, Object> map(String key, String value)
    {
        HashMap<String, Object> data = new HashMap<>();
        data.put(key, value);
        return data;
    }

}
