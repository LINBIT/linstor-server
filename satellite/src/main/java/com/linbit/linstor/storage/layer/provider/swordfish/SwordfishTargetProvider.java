package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.ImplementationError;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.swordfish.SfTargetData;
import com.linbit.linstor.storage.data.provider.swordfish.SfVlmDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.SwordfishConsts;
import com.linbit.linstor.storage.utils.RestClient.RestOp;

import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ALLOCATED_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CAPACITY_SOURCES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_GUARANTEED_BYTES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ODATA_ID;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_PROVIDING_POOLS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.KIB;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_BASE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_MONITOR;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_STORAGE_POOLS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_STORAGE_SERVICES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_TASKS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_TASK_SERVICE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_VOLUMES;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.jr.ob.impl.CollectionBuilder;
import com.fasterxml.jackson.jr.ob.impl.MapBuilder;

@Singleton
public class SwordfishTargetProvider extends AbsSwordfishProvider<SfTargetData>
{
    private static final long POLL_VLM_CRT_TIMEOUT_DEFAULT = 600;
    private static final long POLL_VLM_CTR_MAX_TRIES_DEFAULT = 100;

    private String sfStorSvcId;

    @Inject
    public SwordfishTargetProvider(
        @DeviceManagerContext AccessContext sysCtx,
        ErrorReporter errorReporter,
        Provider<NotificationListener> notificationListenerProvider,
        VolumeDiskStateEvent vlmDiskStateEvent,
        StltConfigAccessor stltConfigAccessor
    )
    {
        super(
            sysCtx,
            errorReporter,
            new RestHttpClient(errorReporter), // TODO: maybe use guice here?
            notificationListenerProvider,
            stltConfigAccessor,
            vlmDiskStateEvent,
            DeviceProviderKind.SWORDFISH_TARGET,
            "SFT",
            "created",
            "deleted"
        );
    }

    @Override
    protected void createImpl(SfTargetData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        try
        {
            SfVlmDfnData vlmDfnData = vlmData.getVlmDfnLayerObject();

            if (!sfResourceExists(vlmDfnData.getVlmOdata()))
            {
                createSfVlm(vlmData);
                // extract the swordfish id of that volume and persist if for later lookups
                errorReporter.logTrace("volume created with @odata.id: %s",  vlmDfnData.getVlmOdata());
            }
            else
            {
                errorReporter.logTrace("volume found with @odata.id: %s", vlmDfnData.getVlmOdata());
            }
            clearAndSet(vlmData, SfTargetData.CREATED);
            // volume exists
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (InterruptedException interruptedExc)
        {
            throw new StorageException("poll timeout interrupted", interruptedExc);
        }
        catch (IOException ioExc)
        {
            clearAndSet(vlmData, SfTargetData.IO_EXC);
            throw new StorageException("IO Exception", ioExc);
        }
    }

    @Override
    protected void deleteImpl(SfTargetData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        try
        {
            String vlmOdataId = vlmData.getVlmOdata();
            if (vlmOdataId != null)
            {
                // TODO health check on composed node

                // DELETE to volumes collection
                restClient.execute(
                    vlmData,
                    RestOp.DELETE,
                    sfUrl + vlmOdataId,
                    getDefaultHeader().noContentType().build(),
                    (String) null,
                    Arrays.asList(HttpHeader.HTTP_ACCEPTED, HttpHeader.HTTP_NOT_FOUND)
                );
            }

            // internal state to send a close event to the ctrl
            clearAndSet(vlmData, SfTargetData.INTERNAL_REMOVE);
            vlmData.getVlmDfnLayerObject().setExists(false);
        }
        catch (IOException ioExc)
        {
            clearAndSet(vlmData, SfTargetData.IO_EXC);
            throw new StorageException("IO Exception", ioExc);
        }
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return getSpace(
            getSwordfishPool(storPool),
            JSON_KEY_ALLOCATED_BYTES
        );
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return getSpace(
            getSwordfishPool(storPool),
            JSON_KEY_GUARANTEED_BYTES
        );
    }

    private void createSfVlm(SfTargetData vlmData)
        throws AccessDeniedException, StorageException, IOException, SQLException,
        InterruptedException, InvalidKeyException
    {
        VolumeDefinition vlmDfn = vlmData.getVlmDfnLayerObject().getVolumeDefinition();
        long sizeInKiB = vlmData.getAllocatedSize();
        StorPool storPool = vlmData.getStorPool();
        Props storPoolProps = storPool.getProps(sysCtx);

        // POST to volumes collection
        String volumeCollUrl = sfUrl + SF_BASE + SF_STORAGE_SERVICES + "/" + sfStorSvcId + SF_VOLUMES;
        String sfStorPoolId = getSfStorPoolId(storPool);
        RestResponse<Map<String, Object>> crtVlmResp = restClient.execute(
            vlmData,
            RestOp.POST,
            volumeCollUrl,
            getDefaultHeader().build(),
            MapBuilder.defaultImpl().start()
                .put(JSON_KEY_CAPACITY_BYTES, sizeInKiB * KIB) // convert to bytes
                .put(
                    JSON_KEY_CAPACITY_SOURCES,
                    CollectionBuilder.defaultImpl().start()
                        .add(
                            MapBuilder.defaultImpl().start()
                                .put(
                                    JSON_KEY_PROVIDING_POOLS,
                                    CollectionBuilder.defaultImpl().start()
                                        .add(
                                            MapBuilder.defaultImpl().start()
                                                .put(
                                                    JSON_KEY_ODATA_ID,
                                                    SF_BASE + SF_STORAGE_SERVICES + "/" + sfStorSvcId +
                                                        SF_STORAGE_POOLS + "/" + sfStorPoolId
                                                )
                                                .build()
                                        )
                                        .buildArray()
                                )
                                .build()
                        )
                        .buildArray()
                )
                .build(),
            Arrays.asList(HttpHeader.HTTP_ACCEPTED)
        );
        clearAndSet(vlmData, SfTargetData.CREATING);
        // volume should be now in "creating" state. we have to wait for the taskMonitor to return HTTP_CREATED

        String taskMonitorLocation = crtVlmResp.getHeaders().get(HttpHeader.LOCATION_KEY);
        // taskLocation should start with SF_BASE + SF_TASK_SERVICE + SF_TASKS followed by "/$taskId/Monitor"
        String taskId;
        try
        {
            taskId = taskMonitorLocation.substring(
                (SF_BASE + SF_TASK_SERVICE + SF_TASKS + "/").length(),
                taskMonitorLocation.indexOf(SF_MONITOR)
            );
        }
        catch (NumberFormatException | ArrayIndexOutOfBoundsException exc)
        {
            throw new StorageException(
                "Task-id could not be parsed. Task monitor url: " + taskMonitorLocation,
                exc
            );
        }
        errorReporter.logTrace(
            "volume creation response status code: %d, taskId: %s",
            crtVlmResp.getStatusCode(),
            taskId
        );

        // TODO: instead of parsing this value over and over again, maybe store
        // the values in StorPool itself (new layer data type?), and in this driver the
        // lower priority node- and stlt prop entries.

        long pollVlmCrtMaxTries = prioStorDriverPropsAsLong(
            StorageConstants.CONFIG_SF_POLL_RETRIES_VLM_CRT_KEY,
            POLL_VLM_CTR_MAX_TRIES_DEFAULT,
            storPoolProps,
            localNodeProps,
            stltConfigAccessor.getReadonlyProps()
        );
        long pollVlmCrtTimeout = prioStorDriverPropsAsLong(
            StorageConstants.CONFIG_SF_POLL_TIMEOUT_VLM_CRT_KEY,
            POLL_VLM_CRT_TIMEOUT_DEFAULT,
            storPoolProps,
            localNodeProps,
            stltConfigAccessor.getReadonlyProps()
        );
        String vlmLocation = null;
        long pollVlmCrtTries = 0;
        while (vlmLocation == null)
        {
            ++pollVlmCrtTries;
            errorReporter.logTrace("waiting %d ms before polling task monitor", pollVlmCrtTimeout);
            Thread.sleep(pollVlmCrtTimeout);

            RestResponse<Map<String, Object>> crtVlmTaskResp = restClient.execute(
                vlmData,
                RestOp.GET,
                sfUrl  + taskMonitorLocation,
                getDefaultHeader().noContentType().build(),
                (String) null,
                Arrays.asList(HttpHeader.HTTP_ACCEPTED, HttpHeader.HTTP_CREATED)
            );
            switch (crtVlmTaskResp.getStatusCode())
            {
                case HttpHeader.HTTP_ACCEPTED: // noop, task is still in progress
                    break;
                case HttpHeader.HTTP_CREATED: // task created successfully
                    vlmLocation = crtVlmTaskResp.getHeaders().get(HttpHeader.LOCATION_KEY);
                    break;
                default: // problem
                    throw new ImplementationError(
                        String.format(
                            "Received none-whitelisted REST status code. URL %s; status code: %d",
                            taskMonitorLocation,
                            crtVlmTaskResp.getStatusCode()
                        )
                    );
            }
            if (pollVlmCrtTries >= pollVlmCrtMaxTries && vlmLocation == null)
            {
                clearAndSet(vlmData, SfTargetData.CREATING_TIMEOUT);
                throw new StorageException(
                    String.format(
                        "Volume creation not finished after %d x %dms. \n" +
                        "GET %s did not contain volume-location in http header",
                        pollVlmCrtTries,
                        pollVlmCrtTimeout,
                        sfUrl  + taskMonitorLocation
                    )
                );
            }
        }
        clearAndSet(vlmData, SfTargetData.CREATED);
        vlmData.getVlmDfnLayerObject().setVlmOdata(vlmLocation);
        try
        {
            // FIXME: only for compatibility... remove once rework is completed
            vlmDfn.getProps(sysCtx).setProp(SwordfishConsts.ODATA, vlmLocation, ApiConsts.NAMESPC_STORAGE_DRIVER);
        }
        catch (InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private RestResponse<Map<String, Object>> getSwordfishPool(StorPool storPool)
        throws StorageException
    {
        RestResponse<Map<String, Object>> response;

        try
        {
            sfStorSvcId = localNodeProps.getProp(
                StorageConstants.CONFIG_SF_STOR_SVC_KEY,
                ApiConsts.NAMESPC_STORAGE_DRIVER
            );
            String sfStorPoolId = getSfStorPoolId(storPool);

            response = getSwordfishResource(
                null,
                SF_BASE + SF_STORAGE_SERVICES + "/" + sfStorSvcId + SF_STORAGE_POOLS + "/" + sfStorPoolId,
                false
            );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key", exc);
        }
        return response;
    }

    private String getSfStorPoolId(StorPool storPool)
    {
        String sfStorPoolId;
        try
        {
            sfStorPoolId = storPool.getProps(sysCtx).getProp(
                StorageConstants.CONFIG_SF_STOR_POOL_KEY,
                ApiConsts.NAMESPC_STORAGE_DRIVER
            );
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return sfStorPoolId;
    }

    @Override
    protected void setUsableSize(SfTargetData vlmData, long size) throws SQLException
    {
        // we don't have usable size... but we need allocated size
        vlmData.setAllocatedSize(size);
    }

    @Override
    public void updateAllocatedSize(VlmProviderObject vlmObjRef)
        throws AccessDeniedException, SQLException, StorageException
    {
        SfTargetData vlmData = (SfTargetData) vlmObjRef;
        vlmData.setAllocatedSize(
            vlmData.getParentAllocatedSizeOrElse(
                () -> vlmData.getVlmDfnLayerObject().getVolumeDefinition().getVolumeSize(sysCtx)
            )
        );
    }
}
