package com.linbit.linstor.backupshipping;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.backups.BackupMetaDataPojo;
import com.linbit.linstor.api.pojo.backups.BackupMetaInfoPojo;
import com.linbit.linstor.api.pojo.backups.LuksLayerMetaPojo;
import com.linbit.linstor.api.pojo.backups.RscDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.RscMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmDfnMetaPojo;
import com.linbit.linstor.api.pojo.backups.VlmMetaPojo;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.utils.Base64;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BackupShippingUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final String BACKUP_TARGET_PROPS_NAMESPC = ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" +
        InternalApiConsts.KEY_BACKUP_TARGET;
    public static final String BACKUP_SOURCE_PROPS_NAMESPC = ApiConsts.NAMESPC_BACKUP_SHIPPING + "/" +
        InternalApiConsts.KEY_BACKUP_SOURCE;

    private BackupShippingUtils()
    {
        // utils-class, do not allow instance
    }

    public static String fillPojo(
        AccessContext accCtx,
        Snapshot snap,
        ReadOnlyProps stltProps,
        byte[] encKey,
        byte[] hash,
        byte[] salt,
        Map<Integer, List<BackupMetaInfoPojo>> backupsRef,
        String basedOnMetaNameRef,
        String remoteName
    )
        throws AccessDeniedException, JsonProcessingException, ParseException
    {
        return OBJECT_MAPPER.writeValueAsString(
            getBackupMetaDataPojo(
                accCtx,
                snap,
                stltProps,
                encKey,
                hash,
                salt,
                backupsRef,
                basedOnMetaNameRef,
                remoteName
            )
        );
    }

    public static BackupMetaDataPojo getBackupMetaDataPojo(
        AccessContext accCtx,
        Snapshot snap,
        ReadOnlyProps stltProps,
        byte[] encKey,
        byte[] hash,
        byte[] salt,
        Map<Integer, List<BackupMetaInfoPojo>> backupsRef,
        @Nullable String basedOnMetaNameRef,
        String remoteName
    )
        throws AccessDeniedException, ParseException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();

        String clusterId = stltProps.getProp(LinStor.PROP_KEY_CLUSTER_ID);

        String startTime = snapDfn.getSnapDfnProps(accCtx)
            .getProp(
                InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
                BackupShippingUtils.BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteName
            );
        long startTimestamp = Long.parseLong(startTime);

        PriorityProps rscDfnPrio = new PriorityProps(
            snapDfn.getRscDfnProps(accCtx),
            rscGrp.getProps(accCtx),
            snap.getNode().getProps(accCtx),
            stltProps
        );
        Map<String, String> rscDfnPropsRef = rscDfnPrio.renderRelativeMap("");
        rscDfnPropsRef = new HashMap<>(rscDfnPropsRef);
        long rscDfnFlagsRef = rscDfn.getFlags().getFlagsBits(accCtx);

        Map<Integer, VlmDfnMetaPojo> vlmDfnsRef = new TreeMap<>();
        Collection<SnapshotVolumeDefinition> vlmDfns = snapDfn.getAllSnapshotVolumeDefinitions(accCtx);
        for (SnapshotVolumeDefinition snapVlmDfn : vlmDfns)
        {
            PriorityProps vlmDfnPrio = new PriorityProps(
                snapVlmDfn.getVlmDfnProps(accCtx),
                rscGrp.getVolumeGroupProps(accCtx, snapVlmDfn.getVolumeNumber())
            );
            Map<String, String> vlmDfnPropsRef = vlmDfnPrio.renderRelativeMap("");
            vlmDfnPropsRef = new TreeMap<>(vlmDfnPropsRef);
            // necessary to get the gross-size-flag, even though flags might have changed in the meantime
            long vlmDfnFlagsRef = snapVlmDfn.getVolumeDefinition().getFlags().getFlagsBits(accCtx);
            long sizeRef = snapVlmDfn.getVolumeSize(accCtx);
            vlmDfnsRef
                .put(
                    snapVlmDfn.getVolumeNumber().value,
                    new VlmDfnMetaPojo(
                        // wrap in new hashmap, otherwise the pojo contains the actual propsContainer
                        new HashMap<>(snapVlmDfn.getSnapVlmDfnProps(accCtx).map()),
                        vlmDfnPropsRef,
                        vlmDfnFlagsRef,
                        sizeRef
                    )
                );
        }

        RscDfnMetaPojo rscDfnRef = new RscDfnMetaPojo(
            getSnapDfnProps(snapDfn.getSnapDfnProps(accCtx)),
            rscDfnPropsRef,
            rscDfnFlagsRef,
            vlmDfnsRef
        );

        long rscFlagsRef = 0;
        Map<Integer, VlmMetaPojo> vlmsRef = new TreeMap<>();
        Iterator<SnapshotVolume> vlmIt = snap.iterateVolumes();
        while (vlmIt.hasNext())
        {
            SnapshotVolume snapVlm = vlmIt.next();
            long vlmFlagsRef = 0;
            vlmsRef.put(
                snapVlm.getVolumeNumber().value,
                new VlmMetaPojo(
                    // wrap in new hashmap, otherwise the pojo contains the actual propsContainer
                    new HashMap<>(snapVlm.getSnapVlmProps(accCtx).map()),
                    new HashMap<>(snapVlm.getVlmProps(accCtx).map()),
                    vlmFlagsRef
                )
            );
        }

        RscMetaPojo rscRef = new RscMetaPojo(
            // wrap in new hashmap, otherwise the pojo contains the actual propsContainer
            new HashMap<>(snap.getSnapProps(accCtx).map()),
            new HashMap<>(snap.getRscProps(accCtx).map()),
            rscFlagsRef,
            vlmsRef
        );

        LuksLayerMetaPojo luksPojo = null;
        List<AbsRscLayerObject<Snapshot>> luksLayers = LayerUtils.getChildLayerDataByKind(
            snap.getLayerData(accCtx),
            DeviceLayerKind.LUKS
        );
        if (!luksLayers.isEmpty() && encKey != null && hash != null && salt != null)
        {
            luksPojo = new LuksLayerMetaPojo(
                Base64.encode(encKey),
                Base64.encode(hash),
                Base64.encode(salt)
            );
        }

        RscLayerDataApi layersRef = snap.getLayerData(accCtx).asPojo(accCtx);

        return new BackupMetaDataPojo(
            rscDfn.getName().displayValue,
            snap.getNodeName().displayValue,
            startTimestamp,
            System.currentTimeMillis(),
            basedOnMetaNameRef,
            layersRef,
            rscDfnRef,
            rscRef,
            luksPojo,
            backupsRef,
            clusterId,
            snapDfn.getUuid().toString()
        );
    }

    /**
     * This method not just returns a wrapped HashMap around the props container (which is necessary to avoid
     * race-conditions while we are serializing and another thread might want to update the props), but also makes
     * sure to replace all {@value InternalApiConsts#KEY_SHIPPING_STATUS}'s value  with
     * {@value InternalApiConsts#VALUE_UPLOADING_METADATA} (see {@link InternalApiConsts#VALUE_UPLOADING_METADATA})
     * in order to prevent confusion on the satellite when restoring this snapshot again.
     */
    private static Map<String, String> getSnapDfnProps(ReadOnlyProps roSnapDfnPropsRef)
    {
        HashMap<String, String> snapDfnProps = new HashMap<>(roSnapDfnPropsRef.map());
        HashSet<String> keys = new HashSet<>();
        final String backupShippingNamespace = ApiConsts.NAMESPC_BACKUP_SHIPPING + "/";
        for (String key : snapDfnProps.keySet())
        {
            if (key.startsWith(backupShippingNamespace) && key.endsWith(InternalApiConsts.KEY_SHIPPING_STATUS))
            {
                keys.add(key);
            }
        }
        for (String key : keys)
        {
            snapDfnProps.put(key, InternalApiConsts.VALUE_UPLOADING_METADATA);
        }
        return snapDfnProps;
    }

    public static String generateBackupName(LocalDateTime now)
    {
        return BackupConsts.BACKUP_PREFIX + BackupConsts.DATE_FORMAT.format(now);
    }

    public static String defaultEmpty(String str)
    {
        return (str == null) ? "" : str;
    }

    public static boolean isAnyShippingInProgress(SnapshotDefinition snapDfn, AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean ret = false;
        // first check if the snapDfn is a backup-target in progress, since this is faster
        ReadOnlyProps snapDfnTarget = snapDfn.getSnapDfnProps(accCtx)
            .getNamespaceOrEmpty(BACKUP_TARGET_PROPS_NAMESPC);
        @Nullable String targetStatus = snapDfnTarget
            .getProp(InternalApiConsts.KEY_SHIPPING_STATUS);
        ret = InternalApiConsts.VALUE_SHIPPING.equals(targetStatus);
        if (!ret)
        {
            // since we don't have an in-progress target, check for any in-progress source
            ReadOnlyProps snapDfnSource = snapDfn.getSnapDfnProps(accCtx)
                .getNamespaceOrEmpty(BACKUP_SOURCE_PROPS_NAMESPC);
            Iterator<String> namespcIter = snapDfnSource.iterateNamespaces();
            while (namespcIter.hasNext() && !ret)
            {
                String tmpRemoteName = namespcIter.next();
                @Nullable String sourceStatus = snapDfnSource
                    .getNamespaceOrEmpty(tmpRemoteName)
                    .getProp(InternalApiConsts.KEY_SHIPPING_STATUS);
                ret = InternalApiConsts.VALUE_SHIPPING.equals(sourceStatus);
            }
        }
        return ret;
    }

    public static boolean isAnyAbortInProgress(SnapshotDefinition snapDfn, AccessContext accCtx)
        throws AccessDeniedException
    {
        boolean ret = false;
        // first check if the snapDfn is a backup-target in progress, since this is faster
        ReadOnlyProps snapDfnTarget = snapDfn.getSnapDfnProps(accCtx)
            .getNamespaceOrEmpty(BACKUP_TARGET_PROPS_NAMESPC);
        @Nullable String targetStatus = snapDfnTarget
            .getProp(InternalApiConsts.KEY_SHIPPING_STATUS);
        ret = InternalApiConsts.VALUE_ABORTING.equals(targetStatus);
        if (!ret)
        {
            // since we don't have an in-progress target, check for any in-progress source
            ReadOnlyProps snapDfnSource = snapDfn.getSnapDfnProps(accCtx)
                .getNamespaceOrEmpty(BACKUP_SOURCE_PROPS_NAMESPC);
            Iterator<String> namespcIter = snapDfnSource.iterateNamespaces();
            while (namespcIter.hasNext() && !ret)
            {
                String tmpRemoteName = namespcIter.next();
                @Nullable String sourceStatus = snapDfnSource
                    .getNamespaceOrEmpty(tmpRemoteName)
                    .getProp(InternalApiConsts.KEY_SHIPPING_STATUS);
                ret = InternalApiConsts.VALUE_ABORTING.equals(sourceStatus);
            }
        }
        return ret;
    }

    public static @Nullable String getSourceShippingStatus(
        SnapshotDefinition snapDfn,
        String remoteName,
        AccessContext accCtx
    ) throws InvalidKeyException, AccessDeniedException
    {
        return snapDfn.getSnapDfnProps(accCtx)
            .getNamespaceOrEmpty(BACKUP_SOURCE_PROPS_NAMESPC)
            .getProp(InternalApiConsts.KEY_SHIPPING_STATUS, remoteName);
    }

    public static @Nullable String getTargetShippingStatus(
        SnapshotDefinition snapDfn,
        AccessContext accCtx
    ) throws InvalidKeyException, AccessDeniedException
    {
        return snapDfn.getSnapDfnProps(accCtx)
            .getProp(
                InternalApiConsts.KEY_SHIPPING_STATUS,
                BACKUP_TARGET_PROPS_NAMESPC
            );
    }

    public static boolean isReceivingFromRemote(
        SnapshotDefinition snapDfn,
        String remoteName,
        AccessContext accCtx
    ) throws InvalidKeyException, AccessDeniedException
    {
        return remoteName.equals(getBackupSrcRemote(snapDfn, accCtx));
    }

    public static boolean isBackupTarget(SnapshotDefinition snapDfn, Node nodeRef, AccessContext accCtx)
        throws AccessDeniedException
    {
        return (hasShippingStatus(snapDfn, null, InternalApiConsts.VALUE_PREPARE_SHIPPING, accCtx) ||
            hasShippingStatus(snapDfn, null, InternalApiConsts.VALUE_SHIPPING, accCtx) ||
            hasShippingStatus(snapDfn, null, InternalApiConsts.VALUE_ABORTING, accCtx)) &&
            isNodeTarget(snapDfn, nodeRef, accCtx);
    }

    public static boolean isNodeTarget(SnapshotDefinition snapDfn, Node nodeRef, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException
    {
        return nodeRef.getName().displayValue.equalsIgnoreCase(
            snapDfn.getSnapDfnProps(accCtx)
                .getProp(
                    InternalApiConsts.KEY_BACKUP_DST_NODE,
                    BACKUP_TARGET_PROPS_NAMESPC
                )
        );
    }

    public static boolean isNodeSource(
        SnapshotDefinition snapDfn,
        Node nodeRef,
        String remoteNameRef,
        boolean isS3ServiceRef,
        AccessContext accCtx
    )
        throws InvalidKeyException, AccessDeniedException
    {
        String propKey = InternalApiConsts.KEY_BACKUP_SRC_NODE;
        return nodeRef.getName().displayValue.equalsIgnoreCase(
            snapDfn.getSnapDfnProps(accCtx)
                .getProp(propKey, BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteNameRef)
        );
    }

    /**
     * This method gets the src-remote from which a backup-target will receive the shipment.
     * This will return the name of either an S3Remote or a StltRemote
     *
     * @param snapDfn
     * @param accCtx
     *
     * @return
     *
     * @throws AccessDeniedException
     * @throws InvalidKeyException
     */
    public static String getBackupSrcRemote(SnapshotDefinition snapDfn, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException
    {
        return snapDfn.getSnapDfnProps(accCtx)
            .getProp(
                InternalApiConsts.KEY_BACKUP_SRC_REMOTE,
                BACKUP_TARGET_PROPS_NAMESPC
            );
    }

    /**
     * This method gets the dst-remote to which a backup-source will send the shipment.
     * This will return the name of either an S3Remote or a StltRemote
     *
     * @param snapDfn
     * @param remoteName
     *     - this has to be the name of an S3Remote or a LinstorRemote
     * @param accCtx
     *
     * @return
     *
     * @throws AccessDeniedException
     * @throws InvalidKeyException
     */
    public static String getBackupDstRemote(SnapshotDefinition snapDfn, String remoteName, AccessContext accCtx)
        throws InvalidKeyException, AccessDeniedException
    {
        return snapDfn.getSnapDfnProps(accCtx)
            .getProp(
                InternalApiConsts.KEY_BACKUP_TARGET_REMOTE,
                BACKUP_SOURCE_PROPS_NAMESPC + "/" + remoteName
            );
    }

    /**
     * fetches the shipping status from the props and compares it to the status given
     *
     * @param snapDfn
     *     - the snapDfn to check
     * @param remoteName
     *     - nullable, if given, BACKUP_SOURCE namespace is checked, else BACKUP_TARGET
     * @param status
     *     - the status to check for
     * @param accCtx
     *
     * @return
     *
     * @throws AccessDeniedException
     * @throws InvalidKeyException
     */
    public static boolean hasShippingStatus(
        SnapshotDefinition snapDfn,
        @Nullable String remoteName,
        String status,
        AccessContext accCtx
    ) throws InvalidKeyException, AccessDeniedException
    {
        boolean ret = false;
        if (remoteName != null)
        {
            ret = status.equals(getSourceShippingStatus(snapDfn, remoteName, accCtx));
        }
        else
        {
            ret = status.equals(getTargetShippingStatus(snapDfn, accCtx));
        }
        return ret;
    }
}
