package com.linbit.linstor.layer.storage.exos.rest;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.layer.storage.exos.ExosProvider;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestBaseResponse;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestBaseResponse.ExosStatus;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestInitiators;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeView;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection.ExosRestPool;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPorts;
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
import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import io.sentry.util.Objects;

public class ExosRestClient
{
    private static final MessageDigest SHA_256;

    private static final String API_LOGIN = "/login";
    private static final String HEADER_SESSION_KEY = "sessionKey";
    private static final String HEADER_DATATYPE = "datatype";
    private static final String HEADER_DATATYPE_VAL_JSON = "json";

    private static final long LOGIN_TIMEOUT = 29 * 60 * 1000; // 29 min in ms

    static final String KEY_API_IP = ApiConsts.KEY_STOR_POOL_EXOS_API_IP;
    static final String KEY_API_PORT = ApiConsts.KEY_STOR_POOL_EXOS_API_PORT;
    static final String KEY_API_USER = ApiConsts.KEY_STOR_POOL_EXOS_API_USER;
    static final String KEY_API_PASS = ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD;
    static final String VLM_TYPE = ApiConsts.KEY_STOR_POOL_EXOS_VLM_TYPE;

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

    private final RestHttpClient restClient;
    private final AccessContext sysCtx;
    private final StltConfigAccessor stltConfigAccessor;
    private final ErrorReporter errorReporter;
    private final String enclosureName;
    private final Map<String, Long> lastLoginTimestamp = new HashMap<>();
    private final Map<String, String> currentSessionKey = new HashMap<>();

    private Props localNodeProps;

    private final String baseEnclosureKey;

    public ExosRestClient(
        AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        StltConfigAccessor stltConfigAccessorRef,
        String enclosureNameRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        stltConfigAccessor = stltConfigAccessorRef;
        Objects.requireNonNull(enclosureNameRef);
        enclosureName = enclosureNameRef;
        restClient = new RestHttpClient(errorReporterRef);

        baseEnclosureKey = ApiConsts.NAMESPC_EXOS + "/" + enclosureName + "/";
        lastLoginTimestamp.put("A", -1L);
        lastLoginTimestamp.put("B", -1L);
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    public String getEnclosureName()
    {
        return enclosureName;
    }

    // public Map<String, ExosRestVolume> getVlmInfo(
    // BiFunction<StorPool, ExosRestVolume, String> toStringFunc,
    // HashMap<StorPool, String> storPoolsWithNames
    // )
    // throws InvalidKeyException, AccessDeniedException, StorageException
    // {
    // HashMap<String, ExosRestVolume> ret = new HashMap<>();
    // HashMap<StorPool, ExosRestVolumesCollection> volumes = getVolumes(storPoolsWithNames);
    // for (Entry<StorPool, ExosRestVolumesCollection> entry : volumes.entrySet())
    // {
    // StorPool storPool = entry.getKey();
    // for (ExosRestVolume exosVlm : entry.getValue().volumes)
    // {
    // ret.put(toStringFunc.apply(storPool, exosVlm), exosVlm);
    // }
    // }
    // return ret;
    // }

    public HashMap<StorPool, ExosRestVolumesCollection> getVolumes(HashMap<StorPool, String> storPoolsWithNamesRef)
        throws AccessDeniedException, InvalidKeyException, StorageException
    {
        HashMap<StorPool, ExosRestVolumesCollection> ret = new HashMap<>();
        for (Entry<StorPool, String> entry : storPoolsWithNamesRef.entrySet())
        {
            StorPool sp = entry.getKey();
            PriorityProps prioProps = getprioProps(sp);
            RestResponse<ExosRestVolumesCollection> simpleGetRequest = simpleGetRequest(
                "/show/volumes/pool/" + entry.getValue(),
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
        String poolSn = storPoolRef.getProps(sysCtx).getProp(ExosProvider.EXOS_POOL_SERIAL_NUMBER);

        ExosRestPoolCollection poolsCollection = simpleGetRequest(
            "/show/pools",
            prioProps,
            ExosRestPoolCollection.class
        ).getData();

        return find(
            poolsCollection.pools,
            pool -> pool.serialNumber.equals(poolSn),
            "Pool with serial number '" + poolSn + "' not found"
        );
    }

    public ExosRestVolume createVolume(
        StorPool storPoolRef,
        String poolName,
        String nameRef,
        long sizeInKib,
        List<String> additionalOptionsList
    )
        throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getprioProps(storPoolRef);

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

        simpleGetRequest(
            urlBuilder.toString(),
            prioProps,
            ExosRestBaseResponse.class
        );

        return find(
            simpleGetRequest("/show/volumes/" + nameRef, prioProps, ExosRestVolumesCollection.class).getData().volumes,
            vlm -> vlm.volumeName.equals(nameRef),
            "Volume '" + nameRef + "' not found"
        );
    }

    public void expandVolume(StorPool storPool, String vlmName, long additionalSizeInKib)
        throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getprioProps(storPool);

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("/expand/volume/size/")
            .append(additionalSizeInKib).append("KiB/")
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

    public void map(String exosVlmNameRef, String lunRef, String portRef, List<String> initiatorIdsRef)
        throws StorageException, AccessDeniedException
    {
        StringBuilder url = new StringBuilder("/map/volume/")
            .append("access/rw/")
            .append("initiator/").append(String.join(",", initiatorIdsRef))
            .append("/lun/").append(lunRef)
            .append("/port/").append(portRef)
            .append("/").append(exosVlmNameRef);

        simpleGetRequest(url.toString(), getprioProps(), ExosRestBaseResponse.class);
    }

    public ExosRestControllers showControllers() throws StorageException, AccessDeniedException
    {
        return simpleGetRequest("/show/controllers", getprioProps(), ExosRestControllers.class).getData();
    }

    public ExosRestPorts showPorts() throws StorageException, AccessDeniedException
    {
        return simpleGetRequest("/show/ports", getprioProps(), ExosRestPorts.class).getData();
    }

    public ExosRestInitiators showInitiators() throws StorageException, AccessDeniedException
    {
        return simpleGetRequest("/show/initiator", getprioProps(), ExosRestInitiators.class).getData();
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
        if (!relativeUrl.startsWith("/"))
        {
            relativeUrl = "/" + relativeUrl;
        }

        StorageException exc = null;
        RestResponse<T> response = null;
        String url = null;
        Map<String, String> headers = null;
        for (String ctrl : new String[] { "A", "B" })
        {
            if (!inLogin)
            {
                ensureLoggedIn(prioProps, ctrl);
            }
            headers = getHeaders(prioProps, ctrl);

            String baseUrl = getBaseUrl(prioProps, ctrl);
            if (baseUrl != null)
            {
                url = baseUrl + relativeUrl;
                errorReporter.logTrace("Sending GET request: %s", url);
                try
                {
                    response = restClient.execute(
                        null,
                        RestOp.GET,
                        url,
                        headers,
                        null,
                        Arrays.asList(HttpHeader.HTTP_OK, HttpHeader.HTTP_FORBIDDEN),
                        responseClass
                    );
                    if (!inLogin && response.getStatusCode() == HttpHeader.HTTP_FORBIDDEN)
                    {
                        errorReporter.logTrace("Received FORBIDDEN (403) response, trying to re-login...");
                        lastLoginTimestamp.put(ctrl, -1L);
                        ensureLoggedIn(prioProps, ctrl);
                        response = restClient.execute(
                            null,
                            RestOp.GET,
                            url,
                            headers,
                            null,
                            Arrays.asList(HttpHeader.HTTP_OK),
                            responseClass
                        );
                    }
                    exc = null;
                    break;
                }
                catch (IOException ioExc)
                {
                    if (exc != null)
                    {
                        errorReporter.reportError(exc); // report both errors
                    }
                    exc = new StorageException(
                        "GET request failed: " + url,
                        null,
                        null,
                        null,
                        getDetails(headers),
                        ioExc
                    );
                }
            }
            else
            {
                errorReporter.logTrace(
                    "URL for controller %s in enclosure %s was not set via properties. skipping.",
                    ctrl,
                    enclosureName
                );
            }
        }
        if (exc != null)
        {
            throw exc;
        }

        if (response == null) {
            throw new StorageException(
                String.format(
                    "Neither controller A nor controller B was configured for enclosure %s!",
                    enclosureName
                )
            );
        }

        T data = response.getData();
        for (ExosStatus status : data.status)
        {
            if (status.responseTypeNumeric == ExosStatus.STATUS_ERROR)
            {
                throw new StorageException(
                    "GET request failed: " + url + "\nResponse: " + status.response,
                    null,
                    null,
                    null,
                    getDetails(headers)
                );
            }
        }

        return response;
    }

    private String getDetails(Map<String, String> headersRef)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Headers:\n");
        for (Entry<String, String> entry : headersRef.entrySet())
        {
            sb.append("  ").append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    private String getBaseUrl(PriorityProps prioPropsRef, String ctrlName)
    {
        String ret = null;
        String baseKey = baseEnclosureKey + ctrlName + "/";
        String host = getProp(prioPropsRef, baseKey + KEY_API_IP);
        if (host != null)
        {
            StringBuilder sb = new StringBuilder();
            if (!host.startsWith("http"))
            {
                sb.append("http://");
            }
            sb.append(host);
            sb.append(":").append(getProp(prioPropsRef, baseKey + KEY_API_PORT, "80"));
            sb.append("/api");
            ret = sb.toString();
        }
        return ret;
    }

    private void ensureLoggedIn(PriorityProps prioProps, String ctrlName) throws StorageException
    {
        long now = System.currentTimeMillis();
        if (lastLoginTimestamp.get(ctrlName) + LOGIN_TIMEOUT < now)
        {
            currentSessionKey.remove(ctrlName);
            String url = API_LOGIN + "/" + getLoginCredentials(prioProps, ctrlName);
            RestResponse<ExosRestBaseResponse> loginResponse = simpleGetRequest(
                url,
                prioProps,
                ExosRestBaseResponse.class,
                true
            );
            currentSessionKey.put(ctrlName, loginResponse.getData().status[0].response);
        }
        lastLoginTimestamp.put(ctrlName, now);
    }

    private String getLoginCredentials(PriorityProps prioPropsRef, String ctrlName) throws StorageException
    {
        String baseCtrlKey = baseEnclosureKey;

        String username = prioPropsRef.getProp(baseCtrlKey + KEY_API_USER);
        if (username == null)
        {
            throw new StorageException("No username defined for enclosure " + enclosureName);
        }

        String password = prioPropsRef.getProp(baseCtrlKey + KEY_API_PASS);
        if (password == null)
        {
            throw new StorageException("No password defined for enclosure " + enclosureName);
        }

        SHA_256.reset();
        byte[] encodedhash = SHA_256.digest((username + "_" + password).getBytes(StandardCharsets.UTF_8));
        String sha256 = DatatypeConverter.printHexBinary(encodedhash);
        return sha256.toLowerCase(); // Exos expects login string lower-case
    }

    private Map<String, String> getHeaders(PriorityProps prioPropsRef, String ctrl)
    {
        Builder builder = HttpHeader.newBuilder()
            .setJsonContentType();
        if (currentSessionKey != null)
        {
            builder.put(HEADER_SESSION_KEY, currentSessionKey.get(ctrl));
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
        return prioProps.getProp(key, "", dflt);
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
