package com.linbit.linstor.layer.storage.ebs.rest;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestHttpClient;

import java.io.IOException;
import java.util.Arrays;

public class AwsRestClient
{
    private static final String AMAZON_AWS_DOMAIN = "amazonaws.com";
    private static final int DFLT_SUPPORT_CHECK_TIMEOUT_IN_MS = 500;

    private final AccessContext sysCtx;
    private final ErrorReporter errorReporter;
    private final RestHttpClient restClient;

    public AwsRestClient(AccessContext sysCtxRef, ErrorReporter errorReporterRef)
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        restClient = new RestHttpClient(errorReporterRef);
    }

    public static boolean isRunningInEc2(ErrorReporter errorReporterRef)
    {
        String result;
        try
        {
            result = new RestHttpClient(
                errorReporterRef,
                DFLT_SUPPORT_CHECK_TIMEOUT_IN_MS,
                DFLT_SUPPORT_CHECK_TIMEOUT_IN_MS,
                DFLT_SUPPORT_CHECK_TIMEOUT_IN_MS
            ).execute(
                null,
                RestOp.GET,
                "http://169.254.169.254/latest/meta-data/services/domain",
                null,
                null,
                Arrays.asList(HttpHeader.HTTP_OK),
                String.class
            ).getData();
        }
        catch (StorageException | IOException exc)
        {
            result = null;
        }
        return AMAZON_AWS_DOMAIN.equals(result);
    }

    public String getLocalEc2InstanceId() throws StorageException
    {
        return simpleGetRequest("http://169.254.169.254/latest/meta-data/instance-id");
    }

    private String simpleGetRequest(String url) throws StorageException
    {
        String ret;
        try
        {
            ret = restClient.execute(
                null,
                RestOp.GET,
                url,
                null,
                null,
                Arrays.asList(HttpHeader.HTTP_OK),
                String.class
            ).getData();
        }
        catch (IOException exc)
        {
            throw new StorageException("GET request failed: " + url, exc);
        }
        return ret;
    }
}
