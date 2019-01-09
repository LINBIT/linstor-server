package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.layers.storage.SfInitiatorVlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.HttpHeader;
import com.linbit.linstor.storage.utils.RestHttpClient;
import com.linbit.linstor.storage.utils.RestResponse;
import com.linbit.linstor.storage.utils.RestClient.RestOp;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ALLOWABLE_VALUES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_CONNECTED_ENTITIES;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DURABLE_NAME;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_DURABLE_NAME_FORMAT;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ENDPOINTS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ENTITY_ROLE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_IDENTIFIERS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_INTEL_RACK_SCALE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_LINKS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_ODATA_ID;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_OEM;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_PARAMETERS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_KEY_RESOURCE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_VALUE_NQN;
import static com.linbit.linstor.storage.utils.SwordfishConsts.JSON_VALUE_TARGET;
import static com.linbit.linstor.storage.utils.SwordfishConsts.PATTERN_NQN;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_ACTIONS;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_ATTACH_RESOURCE_ACTION_INFO;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_BASE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_COMPOSED_NODE_ATTACH_RESOURCE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_COMPOSED_NODE_DETACH_RESOURCE;
import static com.linbit.linstor.storage.utils.SwordfishConsts.SF_NODES;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import com.fasterxml.jackson.jr.ob.impl.MapBuilder;

@Singleton
public class SwordfishInitiatorProvider extends AbsSwordfishProvider<SfInitiatorData>
{
    private static final long POLL_VLM_ATTACH_TIMEOUT_DEFAULT = 1000;
    private static final long POLL_VLM_ATTACH_MAX_TRIES_DEFAULT = 290;
    private static final long POLL_GREP_TIMEOUT_DEFAULT = 1000;
    private static final long POLL_GREP_MAX_TRIES_DEFAULT = 290;

    private String composedNodeId;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public SwordfishInitiatorProvider(
        @DeviceManagerContext AccessContext sysCtx,
        ErrorReporter errorReporter,
        Provider<NotificationListener> notificationListenerProvider,
        StltConfigAccessor stltConfigAccessor,
        VolumeDiskStateEvent vlmDiskStateEvent,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        super(
            sysCtx,
            errorReporter,
            new RestHttpClient(errorReporter), // TODO: maybe use guice here?
            notificationListenerProvider,
            stltConfigAccessor,
            vlmDiskStateEvent,
            DeviceProviderKind.SWORDFISH_INITIATOR,
            "SFI",
            "attached",
            "detached"
        );
        extCmdFactory = extCmdFactoryRef;
    }

    @Override
    protected void createImpl(SfInitiatorData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        try
        {
            if (!isSfVolumeAttached(vlmData))
            {
                waitUntilSfVlmIsAttachable(vlmData);
                attachSfVolume(vlmData);
            }
            else
            {
                clearAndSet(vlmData, SfInitiatorVlmProviderObject.ATTACHABLE);
            }

            // TODO implement health check on composed node

            String volumePath = getVolumePath(vlmData);

            vlmData.devicePath = volumePath;
            vlmData.allocatedSize = ProviderUtils.getAllocatedSize(vlmData, extCmdFactory.create());
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (InterruptedException exc)
        {
            throw new StorageException("poll timeout interrupted", exc);
        }
        catch (IOException exc)
        {
            clearAndSet(vlmData, SfInitiatorData.IO_EXC);
            throw new StorageException("IO Exception", exc);
        }
    }

    @Override
    protected void deleteImpl(SfInitiatorData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        try
        {
            detatchSfVlm(vlmData);
            vlmData.exists = false;

            // TODO health check on composed node
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO Exception", ioExc);
        }
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return Long.MAX_VALUE;
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef)
    {
        super.setLocalNodeProps(localNodePropsRef);
        Props storDriverNamespace = DeviceLayerUtils.getNamespaceStorDriver(localNodePropsRef);

        try
        {
            composedNodeId = storDriverNamespace.getProp(StorageConstants.CONFIG_SF_COMPOSED_NODE_NAME_KEY);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Invalid hardcoded key", exc);
        }
    }

    private boolean isSfVolumeAttached(SfInitiatorData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        return getSfVolumeEndpointDurableNameNqn(vlmData) != null;
    }

    @SuppressWarnings("unchecked")
    private String getSfVolumeEndpointDurableNameNqn(SfInitiatorData vlmData)
        throws StorageException, AccessDeniedException, SQLException
    {
        RestResponse<Map<String, Object>> sfVlmResp = getSfVlm(vlmData);

        Map<String, Object> sfVlmDataMap = sfVlmResp.getData();
        Map<String, Object> vlmLinks = (Map<String, Object>) sfVlmDataMap.get(JSON_KEY_LINKS);
        Map<String, Object> linksOem = (Map<String, Object>) vlmLinks.get(JSON_KEY_OEM);
        Map<String, Object> oemIntelRackscale = (Map<String, Object>) linksOem.get(JSON_KEY_INTEL_RACK_SCALE);
        ArrayList<Object> intelRackscaleEndpoints =
            (ArrayList<Object>) oemIntelRackscale.get(JSON_KEY_ENDPOINTS);

        String nqn = null;
        for (Object endpointObj : intelRackscaleEndpoints)
        {
            Map<String, Object> endpoint = (Map<String, Object>) endpointObj;

            String endpointOdataId = (String) endpoint.get(JSON_KEY_ODATA_ID);

            RestResponse<Map<String, Object>> endpointInfo = getSwordfishResource(
                vlmData,
                endpointOdataId,
                false
            );
            Map<String, Object> endpointData = endpointInfo.getData();

            List<Map<String, Object>> connectedEntities =
                (List<Map<String, Object>>) endpointData.get(JSON_KEY_CONNECTED_ENTITIES);
            for (Map<String, Object> connectedEntity : connectedEntities)
            {
                if (connectedEntity.get(JSON_KEY_ENTITY_ROLE).equals(JSON_VALUE_TARGET))
                {
                    List<Object> endpointIdentifiers =
                        (List<Object>) endpointData.get(JSON_KEY_IDENTIFIERS);
                    for (Object endpointIdentifier : endpointIdentifiers)
                    {
                        Map<String, Object> endpointIdMap = (Map<String, Object>) endpointIdentifier;

                        if (JSON_VALUE_NQN.equals(endpointIdMap.get(JSON_KEY_DURABLE_NAME_FORMAT)))
                        {
                            nqn = (String) endpointIdMap.get(JSON_KEY_DURABLE_NAME);
                            break;
                        }
                    }
                    if (nqn != null)
                    {
                        break;
                    }
                }
            }
        }
        return nqn;
    }

    @SuppressWarnings("unchecked")
    private void waitUntilSfVlmIsAttachable(SfInitiatorData vlmData)
        throws InterruptedException, IOException, StorageException, AccessDeniedException,
        InvalidKeyException, SQLException
    {
        String attachInfoAction = SF_BASE + SF_NODES + "/" + getComposedNodeId() + SF_ACTIONS +
            SF_ATTACH_RESOURCE_ACTION_INFO;
        boolean attachable = false;

        clearAndSet(vlmData, SfInitiatorData.WAITING_ATTACHABLE);

        ReadOnlyProps stltRoProps = stltConfigAccessor.getReadonlyProps();
        Props storPoolProps = vlmData.getVolume().getStorPool(sysCtx).getProps(sysCtx);
        long pollAttachVlmTimeout = prioStorDriverPropsAsLong(
            StorageConstants.CONFIG_SF_POLL_TIMEOUT_ATTACH_VLM_KEY,
            POLL_VLM_ATTACH_TIMEOUT_DEFAULT,
            storPoolProps,
            localNodeProps,
            stltRoProps
        );
        long pollAttachVlmMaxTries = prioStorDriverPropsAsLong(
            StorageConstants.CONFIG_SF_POLL_RETRIES_ATTACH_VLM_KEY,
            POLL_VLM_ATTACH_MAX_TRIES_DEFAULT,
            storPoolProps,
            localNodeProps,
            stltRoProps
        );

        int pollAttachRscTries = 0;
        while (!attachable)
        {
            errorReporter.logTrace(
                "waiting %d ms before polling node's Actions/AttachResourceActionInfo",
                pollAttachVlmTimeout
            );
            Thread.sleep(pollAttachVlmTimeout);

            RestResponse<Map<String, Object>> attachRscInfoResp = restClient.execute(
                null,
                vlmData,
                RestOp.GET,
                sfUrl  + attachInfoAction,
                getDefaultHeader().noContentType().build(),
                (String) null,
                Arrays.asList(HttpHeader.HTTP_OK)
            );
            Map<String, Object> attachRscInfoData = attachRscInfoResp.getData();
            ArrayList<Object> attachInfoParameters = (ArrayList<Object>) attachRscInfoData.get(JSON_KEY_PARAMETERS);
            for (Object attachInfoParameter : attachInfoParameters)
            {
                Map<String, Object> attachInfoParamMap = (Map<String, Object>) attachInfoParameter;
                ArrayList<Object> paramAllowableValues =
                    (ArrayList<Object>) attachInfoParamMap.get(JSON_KEY_ALLOWABLE_VALUES);
                if (paramAllowableValues != null)
                {
                    for (Object paramAllowableValue : paramAllowableValues)
                    {
                        if (paramAllowableValue instanceof Map)
                        {
                            Map<String, Object> paramAllowableValueMap = (Map<String, Object>) paramAllowableValue;
                            String attachableVlmId = (String) paramAllowableValueMap.get(JSON_KEY_ODATA_ID);
                            if (vlmData.vlmDfnData.vlmOdata.equals(attachableVlmId))
                            {
                                attachable = true;
                                break;
                            }
                        }
                    }
                }
            }

            if (++pollAttachRscTries >= pollAttachVlmMaxTries)
            {
                clearAndSet(vlmData, SfInitiatorData.WAITING_ATTACHABLE_TIMEOUT);
                throw new StorageException(
                    String.format(
                        "Volume could not be attached after %d x %dms. \n" +
                        "Volume did not show up in %s -> %s from GET %s",
                        pollAttachRscTries,
                        pollAttachVlmTimeout,
                        JSON_KEY_PARAMETERS,
                        JSON_KEY_ALLOWABLE_VALUES,
                        sfUrl  + attachInfoAction
                    )
                );
            }
        }
        clearAndSet(vlmData, SfInitiatorData.ATTACHABLE);
    }

    private void attachSfVolume(SfInitiatorData vlmData)
        throws IOException, StorageException
    {
        String attachAction = SF_BASE + SF_NODES + "/" + getComposedNodeId() + SF_ACTIONS +
            SF_COMPOSED_NODE_ATTACH_RESOURCE;

        restClient.execute(
            null,
            vlmData, // compatibility only...
            RestOp.POST,
            sfUrl + attachAction,
            getDefaultHeader().build(),
            MapBuilder.defaultImpl().start()
                .put(
                    JSON_KEY_RESOURCE,
                    MapBuilder.defaultImpl().start()
                        .put(JSON_KEY_ODATA_ID, vlmData.vlmDfnData.vlmOdata)
                        .build()
                )
                .build(),
            Arrays.asList(HttpHeader.HTTP_NO_CONTENT)
        );
        clearAndSet(vlmData, SfInitiatorData.ATTACHING);
    }

    public String getVolumePath(SfInitiatorData vlmData)
        throws StorageException, AccessDeniedException, InvalidKeyException, SQLException
    {
        ReadOnlyProps stltRoProps = stltConfigAccessor.getReadonlyProps();
        Props storPoolProps = vlmData.getVolume().getStorPool(sysCtx).getProps(sysCtx);
        long pollGrepNvmeUuidTimeout = prioStorDriverPropsAsLong(
            StorageConstants.CONFIG_SF_POLL_TIMEOUT_GREP_NVME_UUID_KEY,
            POLL_GREP_TIMEOUT_DEFAULT,
            storPoolProps,
            localNodeProps,
            stltRoProps
        );
        long pollGrepNvmeUuidMaxTries = prioStorDriverPropsAsLong(
            StorageConstants.CONFIG_SF_POLL_RETRIES_GREP_NVME_UUID_KEY,
            POLL_GREP_MAX_TRIES_DEFAULT,
            storPoolProps,
            localNodeProps,
            stltRoProps
        );

        String path = null;
        try
        {
            String nqnUuid = null;

            String nqn = getSfVolumeEndpointDurableNameNqn(vlmData);

            if (nqn != null)
            {
                nqnUuid = nqn.substring(nqn.lastIndexOf("uuid:") + "uuid:".length());

                int grepTries = 0;
                boolean grepFailed = false;
                boolean grepFound = false;
                while (!grepFailed && !grepFound)
                {
                    final ExtCmd extCmd = extCmdFactory.create();
                    OutputData outputData = extCmd.exec(
                        "/bin/bash",
                        "-c",
                        "grep -H --color=never " + nqnUuid +
                        " /sys/devices/virtual/nvme-fabrics/ctl/nvme*/subsysnqn"
                    );
                    if (outputData.exitCode == 0)
                    {
                        String outString = new String(outputData.stdoutData);
                        Matcher matcher = PATTERN_NQN.matcher(outString);
                        if (matcher.find())
                        {
                            // although nvme supports multiple namespace, the current implementation
                            // relies on podmanager's limitation of only supporting one namespace per
                            // nvme device
                            path = "/dev/nvme" + matcher.group(1) + "n1";
                            grepFound = true;

                            clearAndSet(vlmData, SfInitiatorData.ATTACHED);
                        }
                    }

                    if (++grepTries >= pollGrepNvmeUuidMaxTries)
                    {
                        clearAndSet(vlmData, SfInitiatorData.WAITING_ATTACHING_TIMEOUT);
                        grepFailed = true;
                    }
                    else
                    {
                        Thread.sleep(pollGrepNvmeUuidTimeout);
                    }
                }
            }
            if (path == null)
            {
                throw new StorageException("Could not extract system path of volume");
            }
        }
        catch (ClassCastException ccExc)
        {
            throw new StorageException("Unexpected json structure", ccExc);
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            throw new StorageException("External operation timed out", timeoutExc);
        }
        catch (InterruptedException interruptedExc)
        {
            throw new StorageException("Interrupted exception", interruptedExc);
        }
        catch (IOException ioExc)
        {
            throw new StorageException("IO exception occured", ioExc);
        }
        return path;
    }


    @SuppressWarnings("unchecked")
    private String detatchSfVlm(SfInitiatorData vlmData)
        throws IOException, StorageException
    {
        //        String sfStorSvcId = getSfStorSvcId(vlmDfnProps);
        //        String sfVlmId = getSfVlmId(vlmDfnProps, true);

        String vlmOdataId = vlmData.vlmDfnData.vlmOdata;

        if (vlmOdataId != null)
        {
            // detach volume from node
            String detachAction = SF_BASE + SF_NODES + "/" + getComposedNodeId() + SF_ACTIONS +
                SF_COMPOSED_NODE_DETACH_RESOURCE;
            // POST to Node/$id/Action/ComposedNode.DetachResource
            RestResponse<Map<String, Object>> detachVlmResp = restClient.execute(
                null,
                vlmData,
                RestOp.POST,
                sfUrl + detachAction,
                getDefaultHeader().build(),
                MapBuilder.defaultImpl().start()
                    .put(
                        JSON_KEY_RESOURCE,
                        MapBuilder.defaultImpl().start()
                            .put(JSON_KEY_ODATA_ID, vlmOdataId)
                            .build()
                    )
                    .build(),
                Arrays.asList(HttpHeader.HTTP_NO_CONTENT, HttpHeader.HTTP_NOT_FOUND, HttpHeader.HTTP_BAD_REQUEST)
            );
            clearAndSet(vlmData, SfInitiatorData.DETACHING);

            switch (detachVlmResp.getStatusCode())
            {
                case HttpHeader.HTTP_BAD_REQUEST:
                    Map<String, Object> errorMap = (Map<String, Object>) detachVlmResp.getData().get("error");
                    List<Map<String, Object>> extInfo = (List<Map<String, Object>>) errorMap
                        .get("@Message.ExtendedInfo");
                    boolean found = false;
                    for (Map<String, Object> sfErrMsgEntry : extInfo)
                    {
                        String errMsg = (String) sfErrMsgEntry.get("Message");
                        if (errMsg.contains("not attached"))
                        {
                            errorReporter.logWarning(
                                String.format(
                                    "Bad request to detach swordfish volume '%s' (not attached).",
                                    vlmOdataId
                                )
                            );
                            found = true;
                        }
                    }
                    if (!found)
                    {
                        throw new StorageException(
                            "Unexpected status code",
                            "A REST call returned the unexpected status code " + detachVlmResp.getStatusCode(),
                            null,
                            null,
                            detachVlmResp.toString(HttpHeader.HTTP_BAD_REQUEST)
                        );
                    }
                    break;
                case HttpHeader.HTTP_NO_CONTENT:
                case HttpHeader.HTTP_NOT_FOUND:
                default:
                    break;
            }

            // internal state to send a close event to the ctrl
            clearAndSet(vlmData, SfInitiatorData.INTERNAL_REMOVE);
        }
        else
        {
            errorReporter.logError("Swordfish volume id is null");
        }
        return vlmOdataId;
    }

    private String getComposedNodeId()
    {
        return composedNodeId;
    }

    private RestResponse<Map<String, Object>> getSfVlm(SfInitiatorData vlmData)
        throws StorageException
    {
        return getSwordfishResource(
            vlmData,
            vlmData.vlmDfnData.vlmOdata,
            false
        );
    }

    @Override
    protected void setUsableSize(SfInitiatorData vlmData, long size)
    {
        vlmData.usableSize = size;
    }
}
