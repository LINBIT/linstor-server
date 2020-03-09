package com.linbit.linstor.storage.layer.provider.openflex.rest;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.provider.openflex.rest.requests.OpenflexPostVolume;
import com.linbit.linstor.storage.layer.provider.openflex.rest.responses.OpenflexJob;
import com.linbit.linstor.storage.layer.provider.openflex.rest.responses.OpenflexPool;
import com.linbit.linstor.storage.layer.provider.openflex.rest.responses.OpenflexStatus;
import com.linbit.linstor.storage.layer.provider.openflex.rest.responses.OpenflexStatus.OpenflexStatusHealth;
import com.linbit.linstor.storage.layer.provider.openflex.rest.responses.OpenflexVolume;
import com.linbit.linstor.storage.layer.provider.openflex.rest.responses.OpenflexVolumesCollection;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;

public class OpenflexRestClient
{
    static final String API_HOST = StorageConstants.CONFIG_OF_API_HOST_KEY;
    static final String API_PORT = StorageConstants.CONFIG_OF_API_PORT_KEY;
    static final String STOR_DEV = StorageConstants.CONFIG_OF_STOR_DEV_KEY;
    static final String STOR_DEV_HOST = StorageConstants.CONFIG_OF_STOR_DEV_HOST_KEY;
    static final String STOR_POOL = StorageConstants.CONFIG_OF_STOR_POOL_KEY;
    static final String USER_NAME = StorageConstants.CONFIG_OF_USER_NAME_KEY;
    static final String USER_PW = StorageConstants.CONFIG_OF_USER_PW_KEY;
    static final String JOB_WAIT_MAX_COUNT = StorageConstants.CONFIG_OF_JOB_WAIT_MAX_COUNT;
    static final String JOB_WAIT_DELAY = StorageConstants.CONFIG_OF_JOB_WAIT_DELAY;

    private static final String DEFAULT_JOB_WAIT_DELAY_MS = "200";
    private static final String DEFAULT_JOB_WAIT_MAX_COUNT = "20";

    private final RestHttpClient restClient;
    private final AccessContext sysCtx;
    private final StltConfigAccessor stltConfigAccessor;
    private final ErrorReporter errorReporter;

    private final Map<String, String> etagCache;

    private Props localNodeProps;

    @Inject
    public OpenflexRestClient(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        StltConfigAccessor stltConfigAccessorRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        stltConfigAccessor = stltConfigAccessorRef;
        restClient = new RestHttpClient(errorReporterRef);

        restClient.setRetryCountOnStatusCode(HttpHeader.HTTP_INTERNAL_SERVER_ERROR, 3);
        restClient.setRetryDelayOnStatusCode(HttpHeader.HTTP_INTERNAL_SERVER_ERROR, 200);

        restClient.setRetryCountOnStatusCode(HttpHeader.HTTP_SERVICE_UNAVAILABLE, 3);
        restClient.setRetryDelayOnStatusCode(HttpHeader.HTTP_SERVICE_UNAVAILABLE, 200);

        etagCache = new TreeMap<>();
    }

    public OpenflexPool getPool(StorPool storPoolRef) throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getPrioProps(storPoolRef);
        String poolUrl = OpenflexUrlBuilder.getPool(prioProps).toString();
        return simpleGetRequest(poolUrl, prioProps, OpenflexPool.class).getData();
    }

    public OpenflexVolumesCollection getAllVolumes() throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getPrioProps(null);
        String vlmCollectionUrl = OpenflexUrlBuilder.getVolumeCollection(prioProps).toString();
        return simpleGetRequest(vlmCollectionUrl, prioProps, OpenflexVolumesCollection.class).getData();
    }

    public OpenflexVolume postVolume(StorPool storPool, String name, long capacity)
        throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getPrioProps(storPool);
        String vlmCollectionUrl = OpenflexUrlBuilder.getVolumeCollection(prioProps).toString();

        OpenflexPostVolume postVlm = new OpenflexPostVolume();
        postVlm.Name = name;
        postVlm.Capacity = capacity;
        postVlm.PoolId = get(prioProps, STOR_POOL);

        String jobLocation = simplePostRequestGetLocation(vlmCollectionUrl, prioProps, postVlm);
        return waitForJob(jobLocation, prioProps, OpenflexVolume.class);
    }

    public void deleteVolume(StorPool storPool, String ofVlmId) throws AccessDeniedException, StorageException
    {
        PriorityProps prioProps = getPrioProps(storPool);
        String vlmUrl = OpenflexUrlBuilder.getVolume(prioProps, ofVlmId).toString();

        simpleDeleteRequest(vlmUrl, prioProps);
    }

    private <T> T waitForJob(String jobUrl, PriorityProps prioProps, Class<T> responseDataClass) throws StorageException
    {
        long jobWaitDelay = Long.parseLong(get(prioProps, JOB_WAIT_DELAY, DEFAULT_JOB_WAIT_DELAY_MS));
        long jobWaitMaxCount = Long.parseLong(get(prioProps, JOB_WAIT_MAX_COUNT, DEFAULT_JOB_WAIT_MAX_COUNT));
        OpenflexJob job = simpleGetRequest(jobUrl, prioProps, OpenflexJob.class).getData();
        while (!job.isfinished() && jobWaitMaxCount-- > 0)
        {
            try
            {
                Thread.sleep(jobWaitDelay);
            }
            catch (InterruptedException exc)
            {
                throw new StorageException("Interrupted while waiting for job to finish", exc);
            }
            job = simpleGetRequest(jobUrl, prioProps, OpenflexJob.class).getData();
        }
        T ret;
        if (job.isfinished())
        {
            for (OpenflexStatusHealth health : job.Status.Health)
            {
                if (health.ID == OpenflexStatus.CRITICAL_FAILURE)
                {
                    throw new StorageException("Job ended with critical failure. " + jobUrl);
                }
            }
            if (job.Affected != null)
            {
                ret = simpleGetRequest(job.Affected, prioProps, responseDataClass).getData();
            }
            else
            {
                // we just have deleted the openflex-resource
                ret = null;
            }
        }
        else
        {
            throw new StorageException(
                "Job did not finish after " + jobWaitMaxCount + "x " + jobWaitDelay + "ms\njob url: " + jobUrl
            );
        }
        return ret;
    }

    private <T> RestResponse<T> simpleGetRequest(String url, PriorityProps prioProps, Class<T> responseClass)
        throws StorageException
    {
        RestResponse<T> response;
        try
        {
            errorReporter.logTrace("Sending GET request: %s", url);
            response = restClient.execute(
                null,
                RestOp.GET,
                url,
                getHeaders(prioProps),
                null,
                Collections.singletonList(HttpHeader.HTTP_OK),
                responseClass
            );

            etagCache.put(url, response.getHeaders().get(HttpHeader.ETAG_KEY));
        }
        catch (IOException exc)
        {
            throw new StorageException("GET request failed: " + url, exc);
        }
        return response;
    }

    private String simplePostRequestGetLocation(String url, PriorityProps prioProps, Object postData)
        throws StorageException
    {
        return simplePostRequest(url, prioProps, postData).getHeaders().get(HttpHeader.LOCATION_KEY);
    }

    private RestResponse<Object> simplePostRequest(String url, PriorityProps prioProps, Object postData)
        throws StorageException
    {
        String jsonString;
        try
        {
            jsonString = RestHttpClient.OBJECT_MAPPER.writeValueAsString(postData);
        }
        catch (JsonProcessingException exc)
        {
            throw new StorageException("Failed to serialize json object: " + postData, exc);
        }
        RestResponse<Object> restResponse;
        try
        {
            errorReporter.logTrace("Sending POST request: %s, json: %s", url, jsonString);
            restResponse = restClient.execute(
                null,
                RestOp.POST,
                url,
                getHeaders(prioProps),
                jsonString,
                Collections.singletonList(HttpHeader.HTTP_ACCEPTED),
                null
            );
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "POST request failed: " + url + " " + jsonString,
                exc
            );
        }
        return restResponse;
    }

    private void simpleDeleteRequest(String url, PriorityProps prioProps)
        throws StorageException
    {
        RestResponse<Object> restResponse = null;
        try
        {
            errorReporter.logTrace("Sending DELETE request: %s", url);

            Map<String, String> headers = getHeaders(prioProps);
            for (int retryCount = 2; retryCount > 0; retryCount--)
            {
                headers.put(HttpHeader.IF_MATCH, etagCache.get(url));
                restResponse = restClient.execute(
                    null,
                    RestOp.DELETE,
                    url,
                    headers,
                    null,
                    Arrays.asList(HttpHeader.HTTP_ACCEPTED, HttpHeader.HTTP_PRECONDITION_FAILED),
                    null
                );
                if (restResponse.getStatusCode() == HttpHeader.HTTP_PRECONDITION_FAILED)
                {
                    simpleGetRequest(url, prioProps, null); // recaches ETag
                }
                else
                if (restResponse.getStatusCode() == HttpHeader.HTTP_ACCEPTED)
                {
                    break;
                }
            }

            if (restResponse.getStatusCode() != HttpHeader.HTTP_ACCEPTED)
            {
                throw new StorageException(
                    "Failed to delete volume. DELETE status code " + restResponse.getStatusCode()
                );
            }
            waitForJob(restResponse.getHeaders().get(HttpHeader.LOCATION_KEY), prioProps, null);
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "POST request failed: " + url,
                exc
            );
        }
    }

    private Map<String, String> getHeaders(PriorityProps prioPropsRef)
    {
        return HttpHeader.newBuilder()
            .setAuth(get(prioPropsRef, USER_NAME), get(prioPropsRef, USER_PW))
            .setJsonContentType()
            .build();
    }

    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    public PriorityProps getPrioProps(StorPool storPoolRef) throws AccessDeniedException
    {
        PriorityProps prioProps = new PriorityProps();
        if (storPoolRef != null)
        {
            prioProps.addProps(storPoolRef.getProps(sysCtx), "Storage pool: " + storPoolRef.getName());
        }
        errorReporter.logDebug(
            "localNodeProps entry-count: " +
                (localNodeProps == null ? "null" : localNodeProps.size())
        );
        prioProps.addProps(localNodeProps, "Local node");
        prioProps.addProps(stltConfigAccessor.getReadonlyProps(), "Controller");

        return prioProps;
    }

    public void checkProps(StorPool storPoolRef) throws StorageException, AccessDeniedException
    {
        List<String> missingKeys = new ArrayList<>();

        PriorityProps prioProps = getPrioProps(storPoolRef);

        checkKey(prioProps, API_HOST, missingKeys);
        checkKey(prioProps, API_PORT, missingKeys);
        checkKey(prioProps, API_PORT, missingKeys);
        checkKey(prioProps, STOR_DEV, missingKeys);
        checkKey(prioProps, STOR_POOL, missingKeys);
        checkKey(prioProps, USER_NAME, missingKeys);
        checkKey(prioProps, USER_PW, missingKeys);

        if (!missingKeys.isEmpty())
        {
            StringBuilder sb = new StringBuilder("The following properties have to be set: ");
            for (String missingKey : missingKeys)
            {
                sb.append("\n").append(StorageConstants.NAMESPACE_STOR_DRIVER).append("/").append(missingKey);
            }
            throw new StorageException(sb.toString());
        }
    }

    private void checkKey(PriorityProps prioPropsRef, String key, List<String> missingKeys)
    {
        String value = get(prioPropsRef, key);
        if (value == null)
        {
            missingKeys.add(key);
        }
    }

    static String get(PriorityProps prioPropsRef, String key, String defaultValue)
    {
        return prioPropsRef.getProp(key, StorageConstants.NAMESPACE_STOR_DRIVER, defaultValue);
    }

    static String get(PriorityProps prioPropsRef, String key)
    {
        return prioPropsRef.getProp(key, StorageConstants.NAMESPACE_STOR_DRIVER);
    }

    public boolean isEtagKnown(String url)
    {
        return etagCache.containsKey(url);
    }

    public void refreshEtag(String selfRef, StorPool storPoolRef) throws StorageException, AccessDeniedException
    {
        simpleGetRequest(selfRef, getPrioProps(storPoolRef), null);
    }

}
