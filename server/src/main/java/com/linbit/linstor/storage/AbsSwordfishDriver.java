package com.linbit.linstor.storage;

import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DATA;
import static com.linbit.linstor.storage.utils.SwordfishConsts.KIB;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_BASE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_STORAGE_SERVICES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_VOLUMES;

import com.linbit.ImplementationError;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.RestClient.RestOp;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class AbsSwordfishDriver implements StorageDriver
{
    protected final ErrorReporter errorReporter;
    protected final StorageDriverKind storageDriverKind;
    protected final RestClient restClient;

    private final Map<String, String> volumeStates;

    protected String sfUrl;
    protected String userName;
    protected String userPw;

    public AbsSwordfishDriver(
        ErrorReporter errorReporterRef,
        StorageDriverKind storageDriverKindRef,
        RestClient restClientRef
    )
    {
        errorReporter = errorReporterRef;
        storageDriverKind = storageDriverKindRef;
        restClient = restClientRef;

        volumeStates = new HashMap<>();
    }

    @Override
    public StorageDriverKind getKind()
    {
        return storageDriverKind;
    }


    @Override
    public void createSnapshot(String ignore, String snapshotName) throws StorageException
    {
        throw new StorageException("Swordfish driver cannot create snapshots");
    }

    @Override
    public void restoreSnapshot(
        String sourceIdentifier,
        String snapshotName,
        String targetIdentifier,
        String cryptKey,
        Props vlmDfnProps
    )
        throws StorageException
    {
        throw new StorageException("Swordfish driver cannot create or restore snapshots");
    }

    @Override
    public void deleteSnapshot(String identifier, String snapshotName) throws StorageException
    {
        throw new StorageException("Swordfish driver cannot create or delete snapshots");
    }

    @Override
    public boolean snapshotExists(String volumeIdentifier, String snapshotName) throws StorageException
    {
        throw new ImplementationError("Snapshots of swordfish volumes are not supported");
    }

    @Override
    public void setConfiguration(
        String storPoolNameStr,
        Map<String, String> storPoolNamespace,
        Map<String, String> nodeNamespace,
        Map<String, String> stltNamespace
    )
        throws StorageException
    {
        boolean requiresHostPort = sfUrl == null;

        String tmpHostPort = stltNamespace.get(StorageConstants.CONFIG_SF_URL_KEY);
        String tmpUserName = stltNamespace.get(StorageConstants.CONFIG_SF_USER_NAME_KEY);
        String tmpUserPw = stltNamespace.get(StorageConstants.CONFIG_SF_USER_PW_KEY);

        if (tmpHostPort == null || tmpHostPort.isEmpty())
        {
            tmpHostPort = nodeNamespace.get(StorageConstants.CONFIG_SF_URL_KEY);
        }
        if (tmpUserName == null || tmpUserName.isEmpty())
        {
            tmpUserName = nodeNamespace.get(StorageConstants.CONFIG_SF_USER_NAME_KEY);
        }
        if (tmpUserPw == null || tmpUserPw.isEmpty())
        {
            tmpUserPw = nodeNamespace.get(StorageConstants.CONFIG_SF_USER_PW_KEY);
        }

        StringBuilder failErrorMsg = new StringBuilder();
        appendIfEmptyButRequired(
            "Missing swordfish host:port specification as a single value such as \n" +
                "https://127.0.0.1:1234\n" +
                "This property has to be set globally:\n\n" +
                "linstor controller set-property " +
                ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + StorageConstants.CONFIG_SF_URL_KEY +
                " <value>\n",
            failErrorMsg,
            tmpHostPort,
            requiresHostPort
        );

        if (!failErrorMsg.toString().trim().isEmpty())
        {
            throw new StorageException(failErrorMsg.toString());
        }

        if (tmpHostPort != null)
        {
            while (tmpHostPort.endsWith("/"))
            {
                tmpHostPort = tmpHostPort.substring(0, tmpHostPort.length() - 1); // cut the trailing '/'
            }
            if (!tmpHostPort.startsWith("http"))
            {
                tmpHostPort = "http://" + tmpHostPort;
            }
            if (tmpHostPort.endsWith("/redfish/v1"))
            {
                tmpHostPort = tmpHostPort.substring(0, tmpHostPort.lastIndexOf("/redfish/v1"));
            }
            sfUrl = tmpHostPort + "/";
        }
        if (tmpUserName != null)
        {
            userName = tmpUserName;
        }
        if (tmpUserPw != null)
        {
            userPw = tmpUserPw;
        }
    }

    @SuppressWarnings("unchecked")
    protected long getSpace(
        RestResponse<Map<String, Object>> restResponse,
        String key
    )
        throws StorageException
    {
        Map<String, Object> poolData = restResponse.getData();
        Long space = null;
        try
        {
            Map<String, Object> capacity = (Map<String, Object>) poolData.get(JSON_KEY_CAPACITY);
            Map<String, Object> capacityData = (Map<String, Object>) capacity.get(JSON_KEY_DATA);
            Object spaceObj = capacityData.get(key);
            if (spaceObj instanceof Integer)
            {
                space = ((Integer) spaceObj).longValue();
            }
            else
            if (spaceObj instanceof Long)
            {
                space = (Long) spaceObj;
            }
        }
        catch (ClassCastException ccExc)
        {
            throw new StorageException("Unexpected json structure in response", ccExc);
        }
        catch (NullPointerException npExc)
        {
            throw new StorageException("Unexpected null entry in response", npExc);
        }
        if (space == null)
        {
            throw new StorageException("Could not retrieve requested space");
        }
        // linstor uses kb as internal unit, swordfish gives us bytes
        return space / KIB;
    }

    protected SizeComparison compareVolumeSizeImpl(
        String sfStorSvcId,
        String sfVlmId,
        long requiredSize

    )
        throws StorageException
    {
        SizeComparison ret;
        RestResponse<Map<String, Object>> response = getSwordfishResource(buildVlmOdataId(sfStorSvcId, sfVlmId));

        if (response.getStatusCode() == HttpHeader.HTTP_OK)
        {
            long actualSize = getLong(response.getData().get(JSON_KEY_CAPACITY_BYTES)) / KIB;
            if (actualSize >= requiredSize)
            {
                ret = SizeComparison.WITHIN_TOLERANCE; // no upper bound (yet)... TODO
            }
            else
            {
                ret = SizeComparison.TOO_SMALL;
            }
        }
        else
        {
            throw new StorageException(
                "Could not determine size of swordfish volume: '" + buildVlmOdataId(sfStorSvcId, sfVlmId) + "'\n" +
                "GET returned status code: " + response.getStatusCode()
            );
        }
        return ret;
    }

    protected boolean sfVolumeExists(String sfStorSvcId, String sfVlmId) throws StorageException
    {
        boolean exists = false;
        try
        {
            RestResponse<Map<String, Object>> resp = getSwordfishResource(buildVlmOdataId(sfStorSvcId, sfVlmId));
            exists = resp.getStatusCode() == HttpHeader.HTTP_OK;
        }
        catch (SfRscException sfExc)
        {
            if (sfExc.statusCode != HttpHeader.HTTP_NOT_FOUND)
            {
                throw sfExc;
            }
        }
        return exists;
    }


    protected RestResponse<Map<String, Object>> getSwordfishResource(String odataId)
        throws StorageException
    {
        RestResponse<Map<String, Object>> rscInfo;
        try
        {
            rscInfo = restClient.execute(
                RestOp.GET,
                sfUrl + odataId,
                getDefaultHeader().build(),
                (String) null,
                Arrays.asList(HttpHeader.HTTP_OK)
            );
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
        return rscInfo;
    }

    protected HttpHeader.Builder getDefaultHeader()
    {
        HttpHeader.Builder httpHeaderBuilder = HttpHeader.newBuilder();
        httpHeaderBuilder.setJsonContentType();
        if (userName != null && !userName.isEmpty())
        {
            httpHeaderBuilder.setAuth(userName, userPw);
        }
        return httpHeaderBuilder;
    }

    protected void setState(String linstorVlmId, String state)
    {
        volumeStates.put(linstorVlmId, state);
    }

    @Override
    public String getVolumeState(String linstorVlmId)
    {
        return volumeStates.get(linstorVlmId);
    }

    protected String buildVlmOdataId(String storSvc, String sfVlmId)
    {
        return SF_BASE + SF_STORAGE_SERVICES + "/" + storSvc + SF_VOLUMES + "/" + sfVlmId;
    }

    protected long getLong(Object object)
    {
        long ret;
        if (object instanceof Integer)
        {
            ret = ((Integer) object).longValue();
        }
        else
        {
            ret = (long) object;
        }
        return ret;
    }

    protected Long getLong(String description, StringBuilder failErrorMsg, String str)
    {
        Long ret = null;
        if (str != null && !str.isEmpty())
        {
            try
            {
                ret = Long.parseLong(str);
            }
            catch (NumberFormatException nfe)
            {
                failErrorMsg
                    .append("Configured ")
                    .append(description)
                    .append(" value (")
                    .append(str)
                    .append(") is not a number\n");
            }
        }
        return ret;
    }

    protected void appendIfEmptyButRequired(
        String errorMsg,
        StringBuilder errorMsgBuilder,
        String str,
        boolean reqStr
    )
    {
        if ((str == null || str.isEmpty()) && reqStr)
        {
            errorMsgBuilder.append(errorMsg);
        }
    }

    protected static class SfRscException extends StorageException
    {
        private static final long serialVersionUID = -843817422915518713L;
        private final int statusCode;

        public SfRscException(String message, int statusCodeRef, Exception nestedException)
        {
            super(message, nestedException);
            statusCode = statusCodeRef;
        }

        public SfRscException(String message, int statusCodeRef)
        {
            super(message);
            statusCode = statusCodeRef;
        }
    }
}
