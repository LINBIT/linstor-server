package com.linbit.linstor.layer.storage.exos.rest;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestBaseResponse;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestBaseResponse.ExosStatus;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeView;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection.ExosRestPool;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestVolumesCollection;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestVolumesCollection.ExosRestVolume;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.HttpHeader.Builder;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class ExosRestClient
{
    private static final MessageDigest SHA_256;

    private static final String API_LOGIN = "/login";
    private static final String HEADER_SESSION_KEY = "sessionKey";
    private static final String HEADER_DATATYPE = "datatype";
    private static final String HEADER_DATATYPE_VAL_JSON = "json";

    private static final long LOGIN_TIMEOUT = 29 * 60 * 1000; // 29 min in ms

    static final String KEY_API_HOST = ApiConsts.KEY_STOR_POOL_EXOS_API_HOST;
    static final String KEY_API_PORT = ApiConsts.KEY_STOR_POOL_EXOS_API_PORT;
    static final String KEY_API_USER = ApiConsts.KEY_STOR_POOL_EXOS_API_USER;
    static final String KEY_API_PASS = ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD;
    static final String VLM_TYPE = ApiConsts.KEY_STOR_POOL_EXOS_VLM_TYPE;


    private final RestHttpClient restClient;
    private final AccessContext sysCtx;
    private final StltConfigAccessor stltConfigAccessor;
    private final ErrorReporter errorReporter;

    private String currentSessionKey = null;

    static
    {
        try
        {
            SHA_256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Props localNodeProps;

    private long lastLoginTimestamp = -1;

    @Inject
    public ExosRestClient(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        StltConfigAccessor stltConfigAccessorRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        stltConfigAccessor = stltConfigAccessorRef;
        restClient = new RestHttpClient(errorReporterRef);
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    public Map<String, ExosRestVolume> getVlmInfo(
        BiFunction<StorPool, ExosRestVolume, String> toStringFunc,
        HashSet<StorPool> storPools
    )
        throws InvalidKeyException, AccessDeniedException, StorageException
    {
        HashMap<String, ExosRestVolume> ret = new HashMap<>();
        HashMap<StorPool, ExosRestVolumesCollection> volumes = getVolumes(storPools);
        for (Entry<StorPool, ExosRestVolumesCollection> entry : volumes.entrySet())
        {
            StorPool storPool = entry.getKey();
            for (ExosRestVolume exosVlm : entry.getValue().volumes)
            {
                ret.put(toStringFunc.apply(storPool, exosVlm), exosVlm);
            }
        }
        return ret;
    }

    public HashMap<StorPool, ExosRestVolumesCollection> getVolumes(Set<StorPool> storPoolSet)
        throws AccessDeniedException, InvalidKeyException, StorageException
    {
        HashMap<StorPool, ExosRestVolumesCollection> ret = new HashMap<>();
        for (StorPool sp : storPoolSet) {
            PriorityProps prioProps = getprioProps(sp);
            RestResponse<ExosRestVolumesCollection> simpleGetRequest = simpleGetRequest(
                "/show/volumes/pool/" + getProp(prioProps, ApiConsts.KEY_STOR_POOL_NAME),
                prioProps,
                ExosRestVolumesCollection.class
            );
            ret.put(sp, simpleGetRequest.getData());
        }
        return ret;
    }

    public ExosRestPool getPool(StorPool storPoolRef)
        throws InvalidKeyException, StorageException, AccessDeniedException
    {
        PriorityProps prioProps = getprioProps(storPoolRef);
        String poolName = getProp(prioProps, ApiConsts.KEY_STOR_POOL_NAME);

        ExosRestPoolCollection poolsCollection = simpleGetRequest(
            "/show/pools/" + poolName,
            prioProps,
            ExosRestPoolCollection.class
        ).getData();

        return find(
            poolsCollection.pools,
            pool -> pool.name.equals(poolName),
            "DiskGroup with pool name '" + poolName + "' not found"
        );
    }

    public ExosRestVolume createVolume(
        StorPool storPoolRef,
        String nameRef,
        long sizeInKib,
        List<String> additionalOptionsList
    )
        throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getprioProps(storPoolRef);
        String poolName = getProp(prioProps, ApiConsts.KEY_STOR_POOL_NAME);

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("/create/volume/pool/").append(poolName)
            .append("/size/").append(sizeInKib).append("KiB/")
            .append(nameRef);
        if (!additionalOptionsList.isEmpty())
        {
            for (String additionalOption : additionalOptionsList)
            {
                urlBuilder.append(additionalOption).append("/");
            }
            urlBuilder.setLength(urlBuilder.length() - 1);
        }

        try
        {
            simpleGetRequest(
                urlBuilder.toString(),
                prioProps,
                ExosRestBaseResponse.class
            );
        }
        catch (StorageException exc)
        {
            if (exc.getMessage().contains("The volume was not mapped"))
            {
                deleteVolume(storPoolRef, nameRef);
                throw exc;
            }
        }

        return find(
            simpleGetRequest("/show/volumes/" + nameRef, prioProps, ExosRestVolumesCollection.class).getData().volumes,
            vlm -> vlm.volumeName.equals(nameRef),
            "Volume '" + nameRef + "' not found"
        );
    }

    public void expandVolume(StorPool storPool, String vlmName, long newSizeInKib)
        throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getprioProps(storPool);

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("/expand/volume/size/").append(newSizeInKib).append("KiB/")
            .append(vlmName);

        simpleGetRequest(urlBuilder.toString(), prioProps, ExosRestBaseResponse.class);
    }

    public void deleteVolume(StorPool storPoolRef, String nameRef) throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getprioProps(storPoolRef);

        simpleGetRequest(
            "/delete/volumes/" + nameRef,
            prioProps,
            ExosRestBaseResponse.class
        );
    }

    public ExosVolumeView showMaps(String exosVlmNameRef) throws StorageException, AccessDeniedException
    {
        return simpleGetRequest("/show/maps/" + exosVlmNameRef, getprioProps(), ExosRestMaps.class)
            .getData().volumeView[0];
    }

    public void unmap(String exosVlmNameRef, @Nullable List<String> initiatorIds)
        throws StorageException, AccessDeniedException
    {
        StringBuilder url = new StringBuilder("/unmap/volume/");
        if (initiatorIds != null)
        {
            url.append(String.join(",", initiatorIds)).append("/");
        }
        url.append(exosVlmNameRef);

        simpleGetRequest(url.toString(), getprioProps(), ExosRestBaseResponse.class);
    }

    public void map(String exosVlmNameRef, int lunRef, List<String> initiatorIdsRef)
        throws StorageException, AccessDeniedException
    {
        StringBuilder url = new StringBuilder("/map/volume/")
            .append("access/rw/")
            .append("initiator/").append(String.join(",", initiatorIdsRef))
            .append("/lun/").append(lunRef)
            .append("/").append(exosVlmNameRef);

        simpleGetRequest(url.toString(), getprioProps(), ExosRestBaseResponse.class);
    }

    public ExosRestControllers showControllers() throws StorageException, AccessDeniedException
    {
        return simpleGetRequest("/show/controllers", getprioProps(), ExosRestControllers.class).getData();
    }

    private <T extends ExosRestBaseResponse> RestResponse<T> simpleGetRequest(
        String relativeUrl,
        PriorityProps prioProps,
        Class<T> responseClass
    )
        throws StorageException
    {
        return simpleGetRequest(relativeUrl, prioProps, responseClass, false);
    }

    private <T extends ExosRestBaseResponse> RestResponse<T> simpleGetRequest(
        String relativeUrl,
        PriorityProps prioProps,
        Class<T> responseClass,
        boolean inLogin
    )
        throws StorageException
    {
        if (!inLogin)
        {
            ensureLoggedIn(prioProps);
        }
        if (!relativeUrl.startsWith("/"))
        {
            relativeUrl = "/" + relativeUrl;
        }
        String url = getBaseUrl(prioProps) + relativeUrl;
        errorReporter.logTrace("Sending GET request: %s", url);
        RestResponse<T> response;
        try
        {
            response = restClient.execute(
                null,
                RestOp.GET,
                url,
                getHeaders(prioProps),
                null,
                Arrays.asList(HttpHeader.HTTP_OK, HttpHeader.HTTP_FORBIDDEN),
                responseClass
            );
            if (response.getStatusCode() == HttpHeader.HTTP_FORBIDDEN)
            {
                lastLoginTimestamp = -1;
                ensureLoggedIn(prioProps);
                response = restClient.execute(
                    null,
                    RestOp.GET,
                    url,
                    getHeaders(prioProps),
                    null,
                    Arrays.asList(HttpHeader.HTTP_OK),
                    responseClass
                );
            }
        }
        catch (IOException exc)
        {
            throw new StorageException("GET request failed: " + url, exc);
        }

        T data = response.getData();
        for (ExosStatus status : data.status)
        {
            if (status.responseTypeNumeric == ExosStatus.STATUS_ERROR)
            {
                throw new StorageException("Get request failed: " + url + "\nResponse: " + status.response);
            }
        }

        return response;
    }

    private String getBaseUrl(PriorityProps prioPropsRef)
    {
        StringBuilder sb = new StringBuilder();
        String host = getProp(prioPropsRef, KEY_API_HOST);
        if (!host.startsWith("http"))
        {
            sb.append("http://");
        }
        sb.append(host);
        sb.append(":").append(getProp(prioPropsRef, KEY_API_PORT, "80"));
        sb.append("/api");
        return sb.toString();
    }

    private void ensureLoggedIn(PriorityProps prioProps) throws StorageException
    {
        long now = System.currentTimeMillis();
        if (lastLoginTimestamp + LOGIN_TIMEOUT < now)
        {
            currentSessionKey = null;
            String url = API_LOGIN + "/" + getLoginCredentials(prioProps);
            RestResponse<ExosRestBaseResponse> loginResponse = simpleGetRequest(
                url,
                prioProps,
                ExosRestBaseResponse.class,
                true
            );
            currentSessionKey = loginResponse.getData().status[0].response;
        }
        lastLoginTimestamp = now;
    }

    private String getLoginCredentials(PriorityProps prioPropsRef)
    {
        String username = prioPropsRef.getProp(KEY_API_USER, ApiConsts.NAMESPC_STORAGE_DRIVER);
        String password = prioPropsRef.getProp(KEY_API_PASS, ApiConsts.NAMESPC_STORAGE_DRIVER);

        SHA_256.reset();
        byte[] encodedhash = SHA_256.digest((username + "_" + password).getBytes(StandardCharsets.UTF_8));
        String sha256 = DatatypeConverter.printHexBinary(encodedhash);
        return sha256.toLowerCase(); // Exos expects login string lower-case
    }

    private Map<String, String> getHeaders(PriorityProps prioPropsRef)
    {
        Builder builder = HttpHeader.newBuilder()
            .setJsonContentType();
        if (currentSessionKey != null)
        {
            builder.put(HEADER_SESSION_KEY, currentSessionKey);
        }
        builder.put(HEADER_DATATYPE, HEADER_DATATYPE_VAL_JSON);
        return builder.build();
    }

    private PriorityProps getprioProps(StorPool sp) throws AccessDeniedException
    {
        return new PriorityProps(sp.getProps(sysCtx), localNodeProps, stltConfigAccessor.getReadonlyProps());
    }

    private PriorityProps getprioProps() throws AccessDeniedException
    {
        return new PriorityProps(localNodeProps, stltConfigAccessor.getReadonlyProps());
    }

    private String getProp(PriorityProps prioProps, String key)
    {
        return getProp(prioProps, key, null);
    }

    private String getProp(PriorityProps prioProps, String key, String dflt)
    {
        return prioProps.getProp(key, ApiConsts.NAMESPC_STORAGE_DRIVER, dflt);
    }

    private <T> T find(T[] array, Predicate<T> predicate, String errorMsg) throws StorageException
    {
        T foundElement = null;
        for (T element : array)
        {
            if (predicate.test(element))
            {
                foundElement = element;
                break;
            }
        }
        if (foundElement == null)
        {
            throw new StorageException(errorMsg);
        }

        return foundElement;
    }
}
