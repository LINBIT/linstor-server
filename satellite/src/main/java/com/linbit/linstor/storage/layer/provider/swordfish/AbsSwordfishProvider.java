package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestClient;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.SwordfishConsts;

import javax.inject.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DATA;
import static com.linbit.linstor.storage.utils.SwordfishConsts.KIB;


// TODO: create custom SwordFish communication objects and use a JSON serializer / deserializer
public abstract class AbsSwordfishProvider<LAYER_DATA extends VlmProviderObject> implements DeviceProvider
{
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
    protected final DeviceProviderKind kind;

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
        DeviceProviderKind kindRef,
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
        kind = kindRef;
        typeDescr = typeDescrRef;
        createdMsg = createdMsgRef;
        deletedMsg = deletedMsgRef;

        changedStorPools = new HashSet<>();
        restClient.addFailHandler(this::handleUnexpectedReturnCode);
    }

    private void handleUnexpectedReturnCode(RestResponse<Map<String, Object>> response)
    {
        VlmProviderObject vlmData = response.getVolumeData();
        if (vlmData != null)
        {
            // could be null if we were requesting free space or total capacity (no vlm involved)
            clearAndSet(vlmData, SfTargetData.FAILED);
        }
    }

    @Override
    public void clearCache() throws StorageException
    {
        // no-op
    }

    @Override
    public Collection<StorPool> getChangedStorPools()
    {
        Set<StorPool> copy = new HashSet<>(changedStorPools);
        return copy;
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmData) throws AccessDeniedException, DatabaseException
    {
        // usable size was just updated (set) by the layer above us. nothing to do here
    }

    @Override
    public void prepare(List<VlmProviderObject> vlmDataList, List<SnapshotVolume> snapVlms)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        localNodeProps = localNodePropsRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(
        List<VlmProviderObject> rawVlmDataList,
        List<SnapshotVolume> ignoredSnapshotlist,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, DatabaseException, StorageException
    {
        List<LAYER_DATA> vlmDataList = (List<LAYER_DATA>) rawVlmDataList;

        List<LAYER_DATA> createList = new ArrayList<>();
        List<LAYER_DATA> deleteList = new ArrayList<>();

        try
        {
            for (LAYER_DATA vlmData : vlmDataList)
            {
                Volume vlm = vlmData.getVolume();
                Props props = DeviceLayerUtils.getInternalNamespaceStorDriver(
                    vlm.getVolumeDefinition().getProps(sysCtx)
                );
                if (vlm.getFlags().isSet(sysCtx, Volume.Flags.DELETE))
                {
                    deleteList.add(vlmData);
                    errorReporter.logInfo(
                        "Deleting / Detaching volume %s/%d",
                        vlmData.getRscLayerObject().getSuffixedResourceName(),
                        vlmData.getVlmNr().value
                    );
                }
                else
                if (props.getProp(SwordfishConsts.DRIVER_SF_VLM_ID_KEY) == null)
                {
                    createList.add(vlmData);
                    errorReporter.logInfo(
                        "Creating / Attaching volume %s/%d",
                        vlmData.getRscLayerObject().getSuffixedResourceName(),
                        vlmData.getVlmNr().value
                    );
                }
                else
                {
                    errorReporter.logInfo(
                        "No-op for volume %s/%d",
                        vlmData.getRscLayerObject().getSuffixedResourceName(),
                        vlmData.getVlmNr().value
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

    private void create(List<LAYER_DATA> vlmDataList, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_DATA vlmData : vlmDataList)
        {
            createImpl(vlmData);
            changedStorPools.add(vlmData.getStorPool());
            addCreatedMsg(vlmData, apiCallRc);
        }
    }

    private void delete(List<LAYER_DATA> deleteList, ApiCallRcImpl apiCallRc)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        for (LAYER_DATA vlmData : deleteList)
        {
            deleteImpl(vlmData);
            changedStorPools.add(vlmData.getStorPool());
            addDeletedMsg(vlmData, apiCallRc);
        }
    }

    protected RestResponse<Map<String, Object>> getSwordfishResource(
        VlmProviderObject vlmData,
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
                vlmData,
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

    private void addCreatedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.CREATED,
                String.format(
                    "Volume %d of resource '%s' [%s] created",
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

    private void addDeletedMsg(LAYER_DATA vlmData, ApiCallRcImpl apiCallRc)
    {
        String rscName = vlmData.getRscLayerObject().getSuffixedResourceName();
        int vlmNr = vlmData.getVlmNr().value;
        apiCallRc.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.MASK_VLM | ApiConsts.DELETED,
                String.format(
                    "Volume %d of resource '%s' [%s] deleted",
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

    @SuppressWarnings("unchecked")
    protected <T extends State> void clearAndSet(VlmProviderObject vlmData, T state)
    {
        List<T> states = (List<T>) vlmData.getStates();
        states.clear();

        Volume vlm = vlmData.getVolume();
        if (state.equals(SfTargetData.INTERNAL_REMOVE))
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
        return new PriorityProps(props).getProp(key, STORAGE_NAMESPACE);
    }

    protected abstract void createImpl(LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException;

    protected abstract void deleteImpl(LAYER_DATA vlmData)
        throws StorageException, AccessDeniedException, DatabaseException;

    @Override
    public abstract long getPoolCapacity(StorPool storPool)
        throws StorageException, AccessDeniedException;

    @Override
    public abstract long getPoolFreeSpace(StorPool storPool)
        throws StorageException, AccessDeniedException;

    protected abstract void setUsableSize(LAYER_DATA vlmData, long size) throws DatabaseException;

    protected static class SfRscException extends StorageException
    {
        private static final long serialVersionUID = -843817422915518713L;
        private final int statusCode;

        SfRscException(String message, int statusCodeRef, Exception nestedException)
        {
            super(message, nestedException);
            statusCode = statusCodeRef;
        }

        SfRscException(String message, int statusCodeRef)
        {
            super(message);
            statusCode = statusCodeRef;
        }
    }
}
