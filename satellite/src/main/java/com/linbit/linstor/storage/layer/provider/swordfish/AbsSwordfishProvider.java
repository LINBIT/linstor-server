package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageDriver;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.SwordfishConsts;
import com.linbit.linstor.storage2.layer.data.State;
import com.linbit.linstor.storage2.layer.data.categories.VlmLayerData;

import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DATA;
import static com.linbit.linstor.storage.utils.SwordfishConsts.KIB;
import javax.inject.Provider;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbsSwordfishProvider implements DeviceProvider
{
    private static final String STATE_FAILED = "Failed";
    public static final String STATE_REMOVE = "INTERNAL_REMOVE";

    protected final StltConfigAccessor stltConfigAccessor;
    protected Props localNodeProps;

    protected final RestClient restClient;
    protected final AccessContext sysCtx;
    protected final ErrorReporter errorReporter;
    protected final Provider<NotificationListener> notificationListenerProvider;
    protected final String typeDescr;
    protected final String createdMsg;
    protected final String deletedMsg;
    protected final Collection<StorPool> changedStorPools;
    protected final VolumeDiskStateEvent vlmDiskStateEvent;


    /*
     * The current Swordfish-driver only allows one swordfish host per linstor-instance
     * If have multiple swordfish hosts, you should also start multiple linstor-instances
     * (that means a dedicated linstor-cluster for each swordfish-cluster)
     */
    protected String sfUrl;
    protected String userName;
    protected String userPw;
    protected Integer retryCount;
    protected Long retryDelay;

    public AbsSwordfishProvider(
        @DeviceManagerContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        RestClient restClientRef,
        Provider<NotificationListener> notificationListenerProviderRef,
        StltConfigAccessor stltConfigAccessorRef,
        VolumeDiskStateEvent vlmDiskStateEventRef,
        String typeDescrRef,
        String createdMsgRef,
        String deletedMsgRef
    )
    {
        sysCtx = sysCtxRef;
        errorReporter = errorReporterRef;
        restClient = restClientRef;
        notificationListenerProvider = notificationListenerProviderRef;
        stltConfigAccessor = stltConfigAccessorRef;
        vlmDiskStateEvent = vlmDiskStateEventRef;
        typeDescr = typeDescrRef;
        createdMsg = createdMsgRef;
        deletedMsg = deletedMsgRef;

        changedStorPools = new HashSet<>();
        restClient.addFailHandler(this::handleUnexpectedReturnCode);
    }

    private void handleUnexpectedReturnCode(RestResponse<Map<String, Object>> response)
    {
        try
        {
            Volume vlm = response.getVolume();
            if (vlm != null)
            {
                // could be null if we were requesting free space or total capacity (no vlm involved)
                clearAndSet(vlm, SfVlmDataStlt.FAILED);
            }
        }
        catch (AccessDeniedException | SQLException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    public void clearCache() throws StorageException
    {
        // no-op
    }

    @Override
    public Collection<StorPool> getAndForgetChangedStorPools()
    {
        Set<StorPool> copy = new HashSet<>(changedStorPools);
        changedStorPools.clear();
        return copy;
    }

    @Override
    public void prepare(List<Volume> volumes, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    @Override
    public void process(List<Volume> volumes, List<SnapshotVolume> list, ApiCallRcImpl apiCallRc)
        throws AccessDeniedException, SQLException, StorageException
    {
        List<Volume> createList = new ArrayList<>();
        List<Volume> deleteList = new ArrayList<>();

        try
        {
            for (Volume vlm : volumes)
            {
                Props props = DeviceLayerUtils.getInternalNamespaceStorDriver(
                    vlm.getVolumeDefinition().getProps(sysCtx)
                );
                if (vlm.getFlags().isSet(sysCtx, VlmFlags.DELETE))
                {
                    deleteList.add(vlm);
                    errorReporter.logInfo(
                        "Deleting / Detaching volume %s/%d",
                        vlm.getResourceDefinition().getName().displayValue,
                        vlm.getVolumeDefinition().getVolumeNumber().value
                    );
                }
                else
                if (props.getProp(SwordfishConsts.DRIVER_SF_VLM_ID_KEY) == null)
                {
                    createList.add(vlm);
                    errorReporter.logInfo(
                        "Creating / Attaching volume %s/%d",
                        vlm.getResourceDefinition().getName().displayValue,
                        vlm.getVolumeDefinition().getVolumeNumber().value
                    );
                }
                else
                {
                    errorReporter.logInfo(
                        "No-op for volume %s/%d",
                        vlm.getResourceDefinition().getName().displayValue,
                        vlm.getVolumeDefinition().getVolumeNumber().value
                    );
                }
            }

            create(createList, apiCallRc);
            delete(deleteList, apiCallRc);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key", exc);
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

    protected boolean sfResourceExists(String odata) throws StorageException
    {
        boolean exists = false;
        if (odata != null && !odata.isEmpty())
        {
            // if swordfish resource does not exist, the statusCode would be HTTP_NOT_FOUND (404)
            exists = getSwordfishResource(null, odata, true).getStatusCode() == HttpHeader.HTTP_OK;
        }
        return exists;
    }

    private void create(List<Volume> volmes, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : volmes)
        {
            ensureVlmLayerDataExists(vlm);

            createImpl(vlm);
            changedStorPools.add(vlm.getStorPool(sysCtx));
            addCreatedMsg(vlm, apiCallRc);
        }
    }

    private void delete(List<Volume> volmes, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : volmes)
        {
            ensureVlmLayerDataExists(vlm);

            deleteImpl(vlm);
            changedStorPools.add(vlm.getStorPool(sysCtx));
            addDeletedMsg(vlm, apiCallRc);
            vlm.delete(sysCtx);
        }
    }

    private void ensureVlmLayerDataExists(Volume vlm) throws AccessDeniedException, SQLException
    {
        VlmLayerData layerData = vlm.getLayerData(sysCtx);
        if (layerData == null)
        {
            layerData = new SfVlmDataStlt(vlm.getVolumeDefinition().getLayerData(sysCtx, SfVlmDfnDataStlt.class));
            vlm.setLayerData(sysCtx, layerData);
        }
    }


    protected RestResponse<Map<String, Object>> getSwordfishResource(
        Volume vlm,
        String odataId,
        boolean allowNotFound
    )
        throws StorageException
    {
        RestResponse<Map<String, Object>> rscInfo;
        try
        {
            List<Integer> expectedRcs = new ArrayList<>();
            expectedRcs.add(HttpHeader.HTTP_OK);
            if (allowNotFound)
            {
                expectedRcs.add(HttpHeader.HTTP_NOT_FOUND);
            }
            rscInfo = restClient.execute(
                null,
                vlm,
                RestOp.GET,
                sfUrl + odataId,
                getDefaultHeader().build(),
                (String) null,
                expectedRcs
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

    private void addCreatedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlm.getResourceDefinition().getName().displayValue;
        int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.CREATED,
                String.format(
                    "Volume number %d of resource '%s' [%s] created",
                    vlmNr,
                    rscName,
                    typeDescr
                )
            )
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    private void addDeletedMsg(Volume vlm, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlm.getResourceDefinition().getName().displayValue;
        int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.DELETED,
                String.format(
                    "Volume number %d of resource '%s' [%s] deleted",
                    vlmNr,
                    rscName,
                    typeDescr
                )
            )
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName)
            .putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr))
            .build()
        );
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        PriorityProps prioProps = new PriorityProps(
            DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getProps(sysCtx)
            ),
            stltConfigAccessor.getReadonlyProps(StorageConstants.NAMESPACE_STOR_DRIVER)
        );

        try
        {
            String tmpUrl = prioProps.getProp(StorageConstants.CONFIG_SF_URL_KEY);
            String tmpUserName = prioProps.getProp(StorageConstants.CONFIG_SF_USER_NAME_KEY);
            String tmpUserPw = prioProps.getProp(StorageConstants.CONFIG_SF_USER_PW_KEY);

            if (tmpUrl == null && sfUrl == null)
            {
                throw new StorageException(
                    "Missing swordfish host:port specification as a single value such as \n" +
                        "https://127.0.0.1:1234\n" +
                        "This property has to be set globally:\n\n" +
                        "linstor controller set-property " +
                        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + StorageConstants.CONFIG_SF_URL_KEY +
                        " <value>\n"
                    );
            }
            if (tmpUrl != null)
            {
                while (tmpUrl.endsWith("/"))
                {
                    tmpUrl = tmpUrl.substring(0, tmpUrl.length() - 1); // cut the trailing '/'
                }
                if (!tmpUrl.startsWith("http"))
                {
                    tmpUrl = "http://" + tmpUrl;
                }
                if (tmpUrl.endsWith("/redfish/v1"))
                {
                    tmpUrl = tmpUrl.substring(0, tmpUrl.lastIndexOf("/redfish/v1"));
                }
                sfUrl = tmpUrl + "/";
            }
            if (tmpUserName != null)
            {
                userName = tmpUserName;
            }
            if (tmpUserPw != null)
            {
                userPw = tmpUserPw;
            }

            int tmpRetryCount;
            {
                String retryCountStr = prioProps.getProp(StorageConstants.CONFIG_SF_RETRY_COUNT_KEY);
                if (retryCountStr != null && !retryCountStr.trim().isEmpty())
                {
                    tmpRetryCount = Integer.parseInt(retryCountStr);
                }
                else
                {
                    tmpRetryCount = StorageConstants.CONFIG_SF_RETRY_COUNT_DEFAULT;
                }
            }

            long tmpRetryDelay;
            {
                String retryDelayStr = prioProps.getProp(StorageConstants.CONFIG_SF_RETRY_DELAY_KEY);
                if (retryDelayStr != null && !retryDelayStr.trim().isEmpty())
                {
                    tmpRetryDelay = Integer.parseInt(retryDelayStr);
                }
                else
                {
                    tmpRetryDelay = StorageConstants.CONFIG_SF_RETRY_DELAY_DEFAULT;
                }
            }

            restClient.setRetryCountOnStatusCode(HttpHeader.HTTP_SERVICE_UNAVAILABLE, tmpRetryCount);
            restClient.setRetryDelayOnStatusCode(HttpHeader.HTTP_SERVICE_UNAVAILABLE, tmpRetryDelay);

        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded props key", exc);
        }
    }

    protected void clearAndSet(Volume vlm, State state) throws AccessDeniedException, SQLException
    {
        SfVlmDataStlt vlmData = (SfVlmDataStlt) vlm.getLayerData(sysCtx);
        List<State> states = vlmData.states;
        states.clear();

        if (state.equals(SfVlmDataStlt.INTERNAL_REMOVE))
        {
            vlmDiskStateEvent.get().closeStream(ObjectIdentifier.volumeDefinition(
                vlm.getResourceDefinition().getName(),
                vlm.getVolumeDefinition().getVolumeNumber()
            ));
        }
        else
        {
            vlmDiskStateEvent.get().triggerEvent(
                ObjectIdentifier.volumeDefinition(
                    vlm.getResourceDefinition().getName(),
                    vlm.getVolumeDefinition().getVolumeNumber()
                ),
                state.toString()
            );
            states.add(state);
        }
    }

    protected long prioStorDriverPropsAsLong(String key, Long dfltValue, Props... props) throws InvalidKeyException
    {
        String strVal = prioStorDriverProps(key, props);
        long ret = dfltValue;
        if (strVal != null && strVal.isEmpty())
        {
            ret = Long.parseLong(strVal);
        }
        return ret;
    }

    private String prioStorDriverProps(String key, Props... props) throws InvalidKeyException
    {
        return new PriorityProps(props).getProp(key, StorageDriver.STORAGE_NAMESPACE);
    }


    protected abstract void createImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException;

    protected abstract void deleteImpl(Volume vlm)
        throws StorageException, AccessDeniedException, SQLException;

    @Override
    public abstract long getPoolCapacity(StorPool storPool)
        throws StorageException, AccessDeniedException;

    @Override
    public abstract long getPoolFreeSpace(StorPool storPool)
        throws StorageException, AccessDeniedException;


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
