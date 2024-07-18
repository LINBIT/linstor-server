package com.linbit.linstor.backupshipping;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
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
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BackupShippingUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
        String basedOnMetaNameRef
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
                basedOnMetaNameRef
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
        String basedOnMetaNameRef
    )
        throws AccessDeniedException, ParseException
    {
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();

        String clusterId = stltProps.getProp(LinStor.PROP_KEY_CLUSTER_ID);

        String startTime = snapDfn.getProps(accCtx).getProp(
            InternalApiConsts.KEY_BACKUP_START_TIMESTAMP,
            ApiConsts.NAMESPC_BACKUP_SHIPPING
        );
        long startTimestamp = Long.parseLong(startTime);

        PriorityProps rscDfnPrio = new PriorityProps(
            snapDfn.getProps(accCtx),
            rscGrp.getProps(accCtx),
            snap.getNode().getProps(accCtx),
            stltProps
        );
        Map<String, String> rscDfnPropsRef = rscDfnPrio.renderRelativeMap("");
        rscDfnPropsRef = new TreeMap<>(rscDfnPropsRef);
        long rscDfnFlagsRef = rscDfn.getFlags().getFlagsBits(accCtx);

        Map<Integer, VlmDfnMetaPojo> vlmDfnsRef = new TreeMap<>();
        Collection<SnapshotVolumeDefinition> vlmDfns = snapDfn.getAllSnapshotVolumeDefinitions(accCtx);
        for (SnapshotVolumeDefinition snapVlmDfn : vlmDfns)
        {
            PriorityProps vlmDfnPrio = new PriorityProps(
                snapVlmDfn.getProps(accCtx),
                rscGrp.getVolumeGroupProps(accCtx, snapVlmDfn.getVolumeNumber())
            );
            Map<String, String> vlmDfnPropsRef = vlmDfnPrio.renderRelativeMap("");
            vlmDfnPropsRef = new TreeMap<>(vlmDfnPropsRef);
            // necessary to get the gross-size-flag, even though flags might have changed in the meantime
            long vlmDfnFlagsRef = snapVlmDfn.getVolumeDefinition().getFlags().getFlagsBits(accCtx);
            long sizeRef = snapVlmDfn.getVolumeSize(accCtx);
            vlmDfnsRef
                .put(snapVlmDfn.getVolumeNumber().value, new VlmDfnMetaPojo(vlmDfnPropsRef, vlmDfnFlagsRef, sizeRef));
        }

        RscDfnMetaPojo rscDfnRef = new RscDfnMetaPojo(rscDfnPropsRef, rscDfnFlagsRef, vlmDfnsRef);

        // wrap in new hashmap, otherwise the pojo contains the actual propsContainer
        Map<String, String> rscPropsRef = new HashMap<>(snap.getProps(accCtx).map());
        long rscFlagsRef = 0;

        Map<Integer, VlmMetaPojo> vlmsRef = new TreeMap<>();
        Iterator<SnapshotVolume> vlmIt = snap.iterateVolumes();
        while (vlmIt.hasNext())
        {
            SnapshotVolume vlm = vlmIt.next();
            Map<String, String> vlmPropsRef = vlm.getProps(accCtx).map();
            long vlmFlagsRef = 0;
            vlmsRef.put(vlm.getVolumeNumber().value, new VlmMetaPojo(vlmPropsRef, vlmFlagsRef));
        }

        RscMetaPojo rscRef = new RscMetaPojo(rscPropsRef, rscFlagsRef, vlmsRef);

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

    public static String generateBackupName(LocalDateTime now)
    {
        return BackupConsts.BACKUP_PREFIX + BackupConsts.DATE_FORMAT.format(now);
    }

    public static String defaultEmpty(String str)
    {
        return (str == null) ? "" : str;
    }
}
