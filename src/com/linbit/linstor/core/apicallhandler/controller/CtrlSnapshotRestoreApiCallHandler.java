package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
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
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CtrlSnapshotRestoreApiCallHandler extends CtrlRscCrtApiCallHandler
{
    private List<String> currentNodeNames;
    private String currentFromRscName;
    private String currentFromSnapshotName;
    private String currentToRscName;

    private final SnapshotDataControllerFactory snapshotDataFactory;

    @Inject
    public CtrlSnapshotRestoreApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer interComSerializer,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        @Named(ControllerCoreModule.SATELLITE_PROPS) Props stltConfRef,
        ResourceDataFactory resourceDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        SnapshotDataControllerFactory snapshotDataFactoryRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef,
            stltConfRef,
            resourceDataFactoryRef,
            volumeDataFactoryRef
        );
        snapshotDataFactory = snapshotDataFactoryRef;
    }

    public ApiCallRc restoreSnapshot(
        List<String> nodeNameStrs,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                nodeNameStrs,
                fromRscNameStr,
                fromSnapshotNameStr,
                toRscNameStr
            )
        )
        {
            ResourceDefinitionData fromRscDfn = loadRscDfn(fromRscNameStr, true);

            SnapshotName fromSnapshotName = asSnapshotName(fromSnapshotNameStr);
            SnapshotDefinition fromSnapshotDfn = loadSnapshotDfn(fromRscDfn, fromSnapshotName);

            ResourceDefinitionData toRscDfn = loadRscDfn(toRscNameStr, true);

            if (toRscDfn.getResourceCount() != 0)
            {
                throw asExc(
                    null,
                    "Cannot restore to resource defintion which already has resources",
                    ApiConsts.FAIL_EXISTS_RSC
                );
            }

            if (nodeNameStrs.isEmpty())
            {
                for (Snapshot snapshot : fromSnapshotDfn.getAllSnapshots(peerAccCtx))
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

            updateSatellites(toRscDfn);

            reportSuccess(
                getObjectDescriptionInlineFirstLetterCaps() + " restored " +
                    "from resource '" + fromRscNameStr + "', snapshot '" + fromSnapshotNameStr + "'.",
                "Resource UUIDs: " +
                    toRscDfn.streamResource(peerAccCtx)
                        .map(Resource::getUuid)
                        .map(UUID::toString)
                        .collect(Collectors.joining(", "))
            );
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeNameStrs, toRscNameStr),
                new HashMap(),
                new HashMap(),
                apiCallRc
            );
        }

        return apiCallRc;
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
                fromSnapshotDfn.getSnapshotVolumeDefinition(peerAccCtx, volumeNumber);

            if (fromSnapshotVlmDfn == null)
            {
                throw asExc(
                    null,
                    "Snapshot does not contain required volume number " + volumeNumber,
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT_VLM_DFN
                );
            }

            long snapshotVolumeSize = fromSnapshotVlmDfn.getVolumeSize(peerAccCtx);
            long requiredVolumeSize = toVlmDfn.getVolumeSize(peerAccCtx);
            if (snapshotVolumeSize != requiredVolumeSize)
            {
                throw asExc(
                    null,
                    "Snapshot size does not match for volume number " + volumeNumber.value + "; " +
                        "snapshot size: " + snapshotVolumeSize + "KiB, " +
                        "required size: " + requiredVolumeSize + "KiB",
                    ApiConsts.FAIL_INVLD_VLM_SIZE
                );
            }

            SnapshotVolume fromSnapshotVolume = snapshot.getSnapshotVolume(peerAccCtx, volumeNumber);

            if (fromSnapshotVolume == null)
            {
                throw new ImplementationError("Expected snapshot volume missing");
            }

            StorPool storPool = fromSnapshotVolume.getStorPool(peerAccCtx);

            Volume vlm = createVolume(rsc, toVlmDfn, storPool, null);
            vlm.getProps(peerAccCtx).setProp(ApiConsts.KEY_STOR_POOL_NAME, storPool.getName().displayValue);
            vlm.getProps(peerAccCtx).setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_RESOURCE, fromSnapshotVlmDfn.getResourceName().displayValue);
            vlm.getProps(peerAccCtx).setProp(
                ApiConsts.KEY_VLM_RESTORE_FROM_SNAPSHOT, fromSnapshotVlmDfn.getSnapshotName().displayValue);
        }
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        List<String> nodeNameStrs,
        String fromRscNameStr,
        String fromSnapshotNameStr,
        String toRscNameStr
        )
    {
        super.setContext(
            type,
            apiCallRc,
            true,
            new HashMap(),
            new HashMap()
        );
        currentNodeNames = nodeNameStrs;
        currentFromRscName = fromRscNameStr;
        currentFromSnapshotName = fromSnapshotNameStr;
        currentToRscName = toRscNameStr;
        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return currentNodeNames.isEmpty() ?
            "Resource: " + currentToRscName :
            "Nodes: " + String.join(", ", currentNodeNames) + "; Resource: " + currentToRscName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeNames, currentToRscName);
    }

    private String getObjectDescriptionInline(List<String> nodeNameStrs, String toRscNameStr)
    {
        return nodeNameStrs.isEmpty() ?
            "resource '" + toRscNameStr + "'" :
            "resource '" + toRscNameStr + "' on nodes '" + String.join(", ", nodeNameStrs) + "'";
    }

    protected final Snapshot loadSnapshot(
        Node node,
        SnapshotDefinition snapshotDfn
    )
        throws ApiCallHandlerFailedException
    {
        Snapshot snapshot;
        try
        {
            snapshot = snapshotDataFactory.load(peerAccCtx, node, snapshotDfn);

            if (snapshot == null)
            {
                throw asExc(
                    null, // throwable
                    "Snapshot '" + snapshotDfn.getName().displayValue +
                        "' of resource '" + snapshotDfn.getResourceName().displayValue +
                        "' on node '" + node.getName().displayValue + "' not found.", // error msg
                    null, // cause
                    null, // details
                    null, // correction
                    ApiConsts.FAIL_NOT_FOUND_SNAPSHOT
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading snapshot '" + snapshotDfn.getName().displayValue +
                    "' of resource '" + snapshotDfn.getResourceName().displayValue +
                    "' on node '" + node.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        return snapshot;
    }
}
