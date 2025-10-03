package com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.backupshipping.BackupShippingUtils;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.AbsRemote.RemoteType;
import com.linbit.linstor.core.repository.SystemConfProtectionRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Singleton
public class BackupNodeFinder
{
    private static final CategorySameNode CATEGORY_SAME_NODE = new CategorySameNode();
    private static final Version VERSION_THIN_SEND_RECV = new ExtToolsInfo.Version(0, 24);
    private static final Version VERSION_UTIL_LINUX = new ExtToolsInfo.Version(2, 24);

    private final Provider<AccessContext> peerAccCtx;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SystemConfProtectionRepository sysCfgRepo;

    @Inject
    public BackupNodeFinder(
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SystemConfProtectionRepository sysCfgRepoRef
    )
    {
        peerAccCtx = peerAccCtxRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        sysCfgRepo = sysCfgRepoRef;

    }

    /**
     * In case we already shipped a backup (full or incremental), we need to make sure that the chosen node
     * also has that snapshot created, otherwise we are not able to send an incremental backup.
     * This method could also be used to verify if the backed up DeviceProviderKind match with the node's snapshot
     * DeviceProviderKind, but as we are currently not supporting mixed DeviceProviderKinds we also neglect
     * this check here.<br/>
     * This method returns a list of Nodes, which are the nodes that are able and allowed to make the shipping.
     * Should this list be empty, it does NOT mean that there are no nodes that can do the shipping, since this case
     * throws an ApiRcException. Instead, it means that this is an incremental shipping whose prevSnap has yet to start
     * shipping and therefore it is still undetermined which node (or node-group) needs to start the shipping. This
     * means that the snap needs to be added to the prevNodeUndecidedQueue instead of the actual node queues
     *
     * @param rscDfn
     * @param currentSnapDfnRef The current snapshot definition. If non-null only nodes are considered that already
     *        have a snapshot of this given snapshot definitions. If this parameter is null, this check is skipped.
     * @param prevSnapDfnRef
     * @param requiredExtToolsRef
     *
     * @return List&lt;Node&gt;
     *
     * @throws AccessDeniedException
     */
    public Set<Node> findUsableNodes(
        ResourceDefinition rscDfn,
        @Nullable SnapshotDefinition currentSnapDfnRef,
        @Nullable SnapshotDefinition prevSnapDfnRef,
        AbsRemote remote,
        @Nullable String targetRscName
    )
        throws AccessDeniedException
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        AccessContext accCtx = peerAccCtx.get();
        /*
         * This map is needed to make sure only nodes with the exact same meta-layout across all volumes (aka the same
         * Category) are grouped together while looking for sets of nodes that can do the shipping.
         */
        Map<Category, Set<Node>> usableGroups = new HashMap<>();
        Set<Node> ret = new HashSet<>();

        Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
        RemoteType remoteType = remote.getType();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (canNodeShip(rsc, currentSnapDfnRef, accCtx))
            {
                ApiCallRcImpl backupShippingSupported = backupShippingSupported(rsc);
                if (backupShippingSupported.isEmpty())
                {
                    Node node = rsc.getNode();
                    boolean canTakeSnapshot = true;
                    if (prevSnapDfnRef != null)
                    {
                        canTakeSnapshot = prevSnapDfnRef.getAllSnapshots(accCtx)
                            .stream()
                            .anyMatch(snap -> snap.getNode().equals(node));
                    }
                    if (!canTakeSnapshot)
                    {
                        apiCallRc.addEntries(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.MASK_INFO,
                                "Cannot create snapshot on node '" + node.getName().displayValue +
                                    "', as the node does not have the required base snapshot for incremental backup"
                            )
                        );
                    }
                    else
                    {
                        canTakeSnapshot = CtrlBackupCreateApiCallHandler.hasNodeAllExtTools(
                            node,
                            remoteType.getRequiredExtTools(),
                            apiCallRc,
                            "Cannot use node '" + node.getName().displayValue +
                                "' as it does not support the tool(s): ",
                            peerAccCtx.get()
                        );
                    }

                    if (canTakeSnapshot)
                    {
                        Set<AbsRscLayerObject<Resource>> drbdRscData = LayerRscUtils.getRscDataByLayer(
                            rsc.getLayerData(accCtx),
                            DeviceLayerKind.DRBD
                        );
                        if (drbdRscData.size() > 1)
                        {
                            // really rethink following for-loop if this gets changed
                            throw new ImplementationError("Multiple DRBD layers are not supported.");
                        }
                        if (drbdRscData.isEmpty())
                        {
                            if (rscDfn.getNotDeletedDiskfulCount(accCtx) > 1)
                            {
                                throw new ImplementationError(
                                    "Shipping non-DRBD resources with more than one replica is not supported."
                                );
                            }
                            else
                            {
                                ret.add(node);
                            }
                        }
                        else
                        {
                            Set<Node> values = usableGroups.computeIfAbsent(
                                getCategoryForDrbdLayers(drbdRscData),
                                k -> new HashSet<>()
                            );
                            values.add(node);
                        }
                    }
                }
                else
                {
                    apiCallRc.addEntries(backupShippingSupported);
                }
            }
        }
        // tell client if we couldn't find any nodes
        if (usableGroups.isEmpty() && ret.isEmpty())
        {
            apiCallRc.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_ENOUGH_NODES,
                    "Backup shipping of resource '" + rscDfn.getName().displayValue +
                        "' cannot be started since there is no node available that supports backup shipping."
                ).setSkipErrorReport(true)
            );
            throw new ApiRcException(apiCallRc);
        }
        // if ret already has elements in it, we are dealing with a storage-only rsc with no replicas
        if (ret.isEmpty())
        {
            if (prevSnapDfnRef != null)
            {
                // get node that did last shipping
                String prevNodeStr;
                String remoteName = remote.getName().displayValue;
                if (remoteType == RemoteType.S3)
                {
                    prevNodeStr = prevSnapDfnRef.getSnapDfnProps(accCtx)
                        .getProp(
                            InternalApiConsts.KEY_BACKUP_SRC_NODE,
                            BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteName
                        );
                }
                else if (remoteType == RemoteType.LINSTOR)
                {
                    if (targetRscName == null)
                    {
                        throw new ImplementationError("targetRscName needs to be not null when using a linstor-remote");
                    }
                    prevNodeStr = prevSnapDfnRef.getSnapDfnProps(accCtx)
                        .getProp(
                            InternalApiConsts.KEY_BACKUP_SRC_NODE ,
                            BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteName
                        );
                }
                else
                {
                    throw new ImplementationError("Remote Type not allowed here");
                }
                Node prevNode = null;
                // if prevNode is null it is impossible to find out which node(s) need(s) to make the inc, so we return
                // an empty list instead
                if (prevNodeStr != null)
                {
                    prevNode = ctrlApiDataLoader.loadNode(prevNodeStr, false);
                    // if node in same-node-category, only return that node, else group
                    if (prevNode != null)
                    {
                        for (Entry<Category, Set<Node>> group : usableGroups.entrySet())
                        {
                            if (group.getValue().contains(prevNode))
                            {
                                if (group.getKey().equals(CATEGORY_SAME_NODE))
                                {
                                    ret.add(prevNode);
                                }
                                else
                                {
                                    ret.addAll(group.getValue());
                                }
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                ret = getNodesForFullBackup(usableGroups);
            }

        }
        return ret;
    }

    private boolean canNodeShip(Resource rsc, @Nullable SnapshotDefinition currentSnapDfnRef, AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean isSomeSortOfDiskless = rsc.getStateFlags()
            .isSomeSet(
                accCtx,
                Resource.Flags.DRBD_DISKLESS,
                Resource.Flags.NVME_INITIATOR,
                Resource.Flags.EBS_INITIATOR
            );
        boolean isNodeAvailable = !rsc.getNode()
            .getFlags()
            .isSomeSet(
                accCtx,
                Node.Flags.DELETE,
                Node.Flags.EVACUATE,
                Node.Flags.EVICTED
            ) && rsc.getNode().getPeer(accCtx).isOnline();
        PriorityProps prioProps = new PriorityProps(
            rsc.getNode().getProps(accCtx),
            sysCfgRepo.getCtrlConfForView(accCtx)
        );
        String maxBackups = prioProps.getProp(
            ApiConsts.KEY_MAX_CONCURRENT_BACKUPS_PER_NODE,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );

        boolean hasSnapshot = currentSnapDfnRef == null ||
            currentSnapDfnRef.getSnapshot(accCtx, rsc.getNode().getName()) != null;
        /*
         * This does NOT check for free shipping slots, that is not important here, since we only want to know if the
         * node is able to do a shipment, not if it is possible to start it right now
         */
        boolean isNodeAllowedToShip = maxBackups == null || maxBackups.isEmpty() || Integer.parseInt(maxBackups) != 0;
        return !isSomeSortOfDiskless && isNodeAvailable && isNodeAllowedToShip && hasSnapshot;
    }

    private Set<Node> getNodesForFullBackup(Map<Category, Set<Node>> usableGroups)
    {
        Set<Node> ret;
        if (usableGroups.size() == 1)
        {
            // return the only existing list
            ret = usableGroups.values().iterator().next();
        }
        else
        {
            // sort after size, excluding all types that need to be shipped on one node only (like zfs & luks)
            /*
             * while all nodes with zfs (or luks or somethig similar) are sorted into the same group, once the shipping
             * is started on a specific zfs node, only that node can make incremental backups (limitation by zfs),
             * therefore all zfs nodes actually count the same as a group with only 1 node
             */

            Map<Category, Set<Node>> usableGroupsSameNode = new TreeMap<>(usableGroups);
            usableGroupsSameNode.remove(CATEGORY_SAME_NODE);
            TreeMap<Integer, List<Set<Node>>> sizes = new TreeMap<>();
            for (Set<Node> val : usableGroupsSameNode.values())
            {
                sizes.computeIfAbsent(val.size(), k -> new ArrayList<>()).add(val);
            }
            int biggestGroupSize = sizes.lastKey();
            if (biggestGroupSize == 1)
            {
                // only include same-node-category if all other groups contain only 1 node
                ret = usableGroups.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
            }
            else
            {
                List<Set<Node>> biggestGroupList = sizes.get(biggestGroupSize);
                // since the groups are the same size, it does not matter which group does the shipping
                ret = biggestGroupList.stream().flatMap(Set::stream).collect(Collectors.toSet());
            }
        }
        return ret;
    }

    /**
     * Checks if the given rsc has the correct device-provider and ext-tools to be shipped as a backup
     */
    public ApiCallRcImpl backupShippingSupported(Resource rsc) throws AccessDeniedException
    {
        Set<StorPool> storPools = LayerVlmUtils.getStorPools(rsc, peerAccCtx.get());
        ApiCallRcImpl errors = new ApiCallRcImpl();
        for (StorPool sp : storPools)
        {
            DeviceProviderKind deviceProviderKind = sp.getDeviceProviderKind();
            if (deviceProviderKind.isBackupShippingSupported())
            {
                ExtToolsManager extToolsManager = rsc.getNode().getPeer(peerAccCtx.get()).getExtToolsManager();
                errors.addEntry(
                    getErrorRcIfNotSupported(
                        deviceProviderKind,
                        extToolsManager,
                        ExtTools.COREUTILS_LINUX,
                        "timeout from coreutils",
                        new ExtToolsInfo.Version(8, 5) // coreutils commit c403c31e8806b732e1164ef4a206b0eab71bca95
                    )
                );
                if (deviceProviderKind.equals(DeviceProviderKind.LVM_THIN))
                {
                    errors.addEntry(
                        getErrorRcIfNotSupported(
                            deviceProviderKind,
                            extToolsManager,
                            ExtTools.THIN_SEND_RECV,
                            "thin_send_recv",
                            VERSION_THIN_SEND_RECV
                        )
                    );
                }
            }
            else
            {
                errors.addEntry(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                        String.format(
                            "The storage pool kind %s does not support snapshot shipping",
                            deviceProviderKind.name()
                        )
                    )
                );
            }
        }
        return errors;
    }

    private Category getCategoryForDrbdLayers(Set<AbsRscLayerObject<Resource>> rscData)
    {
        Category ret = null;
        for (AbsRscLayerObject<Resource> drbdRscData : rscData)
        {
            List<AbsRscLayerObject<Resource>> luksRscData = LayerUtils.getChildLayerDataByKind(
                drbdRscData,
                DeviceLayerKind.LUKS
            );
            if (!luksRscData.isEmpty())
            {
                /*
                 * every luks-volume has a different password and therefore different encoding. If we'd allow different
                 * nodes to ship incremental luks backups, the result would be undecodable gibberish.
                 */
                ret = CATEGORY_SAME_NODE;
            }
            else
            {
                List<AbsRscLayerObject<Resource>> storageRscData = LayerUtils.getChildLayerDataByKind(
                    drbdRscData,
                    DeviceLayerKind.STORAGE
                );
                boolean zfsIncluded = false;
                for (AbsRscLayerObject<Resource> storLayerRscData : storageRscData)
                {
                    if (RscLayerSuffixes.shouldSuffixBeShipped(storLayerRscData.getResourceNameSuffix()))
                    {
                        for (VlmProviderObject<Resource> storVlm : storLayerRscData.getVlmLayerObjects().values())
                        {
                            DeviceProviderKind providerKind = storVlm.getProviderKind();
                            if (
                                providerKind.equals(DeviceProviderKind.ZFS) ||
                                    providerKind.equals(DeviceProviderKind.ZFS_THIN)
                            )
                            {
                                zfsIncluded = true;
                                break;
                            }
                        }
                        if (zfsIncluded)
                        {
                            break;
                        }
                    }
                }
                if (zfsIncluded)
                {
                    ret = CATEGORY_SAME_NODE;
                }
                else
                {
                    DrbdRscData<Resource> drbdLayer = (DrbdRscData<Resource>) drbdRscData;
                    Map<VolumeNumber, Boolean> key = new HashMap<>();
                    for (DrbdVlmData<Resource> drbdVlm : drbdLayer.getVlmLayerObjects().values())
                    {
                        key.put(drbdVlm.getVlmNr(), drbdVlm.isUsingExternalMetaData());
                    }
                    ret = new CategoryLvm(key);
                }
            }
        }
        return ret;
    }

    /**
     * Checks if the given ext-tool is supported and returns an error-rc instead of throwing an exception if not.
     */
    private @Nullable ApiCallRcEntry getErrorRcIfNotSupported(
        DeviceProviderKind deviceProviderKind,
        ExtToolsManager extToolsManagerRef,
        ExtTools extTool,
        String toolDescr,
        @Nullable ExtToolsInfo.Version version
    )
    {
        @Nullable ApiCallRcEntry errorRc;
        ExtToolsInfo info = extToolsManagerRef.getExtToolInfo(extTool);
        if (info == null || !info.isSupported())
        {
            errorRc = ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                String.format(
                    "%s based backup shipping requires support for %s",
                    deviceProviderKind.name(),
                    toolDescr
                ),
                true
            );
        }
        else if (version != null && !info.hasVersionOrHigher(version))
        {
            errorRc = ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_SNAPSHOT_SHIPPING_NOT_SUPPORTED,
                String.format(
                    "%s based backup shipping requires at least version %s for %s",
                    deviceProviderKind.name(),
                    version.toString(),
                    toolDescr
                ),
                true
            );
        }
        else
        {
            errorRc = null;
        }
        return errorRc;
    }
}
