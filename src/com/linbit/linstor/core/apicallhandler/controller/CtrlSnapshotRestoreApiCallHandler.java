package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDataFactory;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDataControllerFactory;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.OperationDescription;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.linbit.utils.StringUtils.firstLetterCaps;

@Singleton
public class CtrlSnapshotRestoreApiCallHandler extends CtrlRscCrtApiCallHandler
{
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final SnapshotDataControllerFactory snapshotDataFactory;
    private final ResponseConverter responseConverter;

    @Inject
    public CtrlSnapshotRestoreApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        @Named(ControllerCoreModule.SATELLITE_PROPS) Props stltConfRef,
        ResourceDataFactory resourceDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        SnapshotDataControllerFactory snapshotDataFactoryRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef,
            stltConfRef,
            resourceDataFactoryRef,
            volumeDataFactoryRef
        );
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        snapshotDataFactory = snapshotDataFactoryRef;
        responseConverter = responseConverterRef;
    }

    public ApiCallRc restoreSnapshot(
        List<String> nodeNameStrs,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = new ResponseContext(
            peer.get(),
            new ApiOperation(ApiConsts.MASK_CRT, new OperationDescription("restore", "restoring")),
            getSnapshotRestoreDescription(nodeNameStrs, toRscNameStr),
            getSnapshotRestoreDescriptionInline(nodeNameStrs, toRscNameStr),
            ApiConsts.MASK_SNAPSHOT,
            Collections.emptyMap()
        );

        try
        {
            ResourceDefinitionData fromRscDfn = loadRscDfn(fromRscNameStr, true);

            SnapshotName fromSnapshotName = LinstorParsingUtils.asSnapshotName(fromSnapshotNameStr);
            SnapshotDefinition fromSnapshotDfn = loadSnapshotDfn(fromRscDfn, fromSnapshotName);

            ResourceDefinitionData toRscDfn = loadRscDfn(toRscNameStr, true);

            if (toRscDfn.getResourceCount() != 0)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_RSC,
                    "Cannot restore to resource defintion which already has resources"
                ));
            }

            if (nodeNameStrs.isEmpty())
            {
                for (Snapshot snapshot : fromSnapshotDfn.getAllSnapshots(peerAccCtx.get()))
                {
                    restoreOnNode(fromSnapshotDfn, toRscDfn, snapshot.getNode());
                }
            }
            else
            {
                for (String nodeNameStr : nodeNameStrs)
                {
                    NodeData node = loadNode(nodeNameStr, true);
                    restoreOnNode(fromSnapshotDfn, toRscDfn, node);
                }
            }

            commit();

            if (toRscDfn.getVolumeDfnCount(peerAccCtx.get()) > 0)
            {
                responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(toRscDfn));
            }
            else
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "No volumes to restore."
                    )
                    .setDetails("The target resource definition has no volume definitions. " +
                                "The restored resources will be empty.")
                    .setCorrection("Restore the volume definitions to the target resource definition.")
                    .build()
                );
            }

            responseConverter.addWithOp(responses, context, ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.CREATED,
                    firstLetterCaps(getSnapshotRestoreDescriptionInline(nodeNameStrs, toRscNameStr)) + " restored " +
                        "from resource '" + fromRscNameStr + "', snapshot '" + fromSnapshotNameStr + "'."
                )
                .setDetails("Resource UUIDs: " +
                    toRscDfn.streamResource(peerAccCtx.get())
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", ")))
                .build()
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void restoreOnNode(SnapshotDefinition fromSnapshotDfn, ResourceDefinitionData toRscDfn, Node node)
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        Snapshot snapshot = loadSnapshot(node, fromSnapshotDfn);

        NodeId nodeId = getNextFreeNodeId(toRscDfn);

        ResourceData rsc = createResource(toRscDfn, node, nodeId, Collections.emptyList());

        Iterator<VolumeDefinition> toVlmDfnIter = getVlmDfnIterator(toRscDfn);
        while (toVlmDfnIter.hasNext())
        {
            VolumeDefinition toVlmDfn = toVlmDfnIter.next();
            VolumeNumber volumeNumber = toVlmDfn.getVolumeNumber();

            SnapshotVolumeDefinition fromSnapshotVlmDfn =
                fromSnapshotDfn.getSnapshotVolumeDefinition(peerAccCtx.get(), volumeNumber);

            if (fromSnapshotVlmDfn == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN,
                    "Snapshot does not contain required volume number " + volumeNumber
                ));
            }

            long snapshotVolumeSize = fromSnapshotVlmDfn.getVolumeSize(peerAccCtx.get());
            long requiredVolumeSize = toVlmDfn.getVolumeSize(peerAccCtx.get());
            if (snapshotVolumeSize != requiredVolumeSize)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    "Snapshot size does not match for volume number " + volumeNumber.value + "; " +
                        "snapshot size: " + snapshotVolumeSize + "KiB, " +
                        "required size: " + requiredVolumeSize + "KiB"
                ));
            }

            SnapshotVolume fromSnapshotVolume = snapshot.getSnapshotVolume(peerAccCtx.get(), volumeNumber);

            if (fromSnapshotVolume == null)
            {
                throw new ImplementationError("Expected snapshot volume missing");
            }

            StorPool storPool = fromSnapshotVolume.getStorPool(peerAccCtx.get());

            Volume vlm = createVolume(rsc, toVlmDfn, storPool, null);
            vlm.getProps(peerAccCtx.get()).setProp(ApiConsts.KEY_STOR_POOL_NAME, storPool.getName().displayValue);
            vlm.getProps(peerAccCtx.get()).setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE, fromSnapshotVlmDfn.getResourceName().displayValue);
            vlm.getProps(peerAccCtx.get()).setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT, fromSnapshotVlmDfn.getSnapshotName().displayValue);
        }
    }

    protected final Snapshot loadSnapshot(
        Node node,
        SnapshotDefinition snapshotDfn
    )
    {
        Snapshot snapshot;
        try
        {
            snapshot = snapshotDataFactory.load(peerAccCtx.get(), node, snapshotDfn);

            if (snapshot == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT,
                    "Snapshot '" + snapshotDfn.getName().displayValue +
                        "' of resource '" + snapshotDfn.getResourceName().displayValue +
                        "' on node '" + node.getName().displayValue + "' not found."
                ));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load snapshot '" + snapshotDfn.getName().displayValue +
                    "' of resource '" + snapshotDfn.getResourceName().displayValue +
                    "' on node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        return snapshot;
    }

    private static String getSnapshotRestoreDescription(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "Resource: " + toRscNameStr :
            "Nodes: " + String.join(", ", nodeNameStrs) + "; Resource: " + toRscNameStr;
    }

    private static String getSnapshotRestoreDescriptionInline(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "resource '" + toRscNameStr + "'" :
            "resource '" + toRscNameStr + "' on nodes '" + String.join(", ", nodeNameStrs) + "'";
    }
}
