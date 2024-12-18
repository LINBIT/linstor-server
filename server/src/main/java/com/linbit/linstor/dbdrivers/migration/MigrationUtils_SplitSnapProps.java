package com.linbit.linstor.dbdrivers.migration;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

@SuppressWarnings("checkstyle:typename")
public class MigrationUtils_SplitSnapProps
{

    public enum InstanceType
    {
        // CTRL and STLT do not need a trailing "/"
        CTRL("/CTRLCFG", "/CTRL", null),
        STLT("STLTCFG", "/STLT", null),

        NODES("/NODES/", "/NODES/", null),
        NODE_CONNS("/CONDFN/NODES/", "/NODE_CONNS/", null),
        RSC_GRPS("/RESOURCEGROUPS/", "/RSC_GRPS/", null),
        RSC_DFNS("/RESOURCEDEFINITIONS/", "/RSC_DFNS/", null),
        RSCS("/RESOURCES/", "/RSCS/", null),
        RSC_CONNS("/CONDFN/RESOURCES/", "/RSC_CONNS/", null),
        VLM_GRPS("/VOLUMEGROUPS/", "/VLM_GRPS/", null),
        VLM_DFNS("/VOLUMEDEFINITIONS/", "/VLM_DFNS/", null),
        VLMS("/VOLUMES/", "/VLMS/", null),
        VLM_CONNS("/CONDFN/VOLUME/", "/VLM_CONNS/", null),
        STOR_POOLS("/STORPOOLCONF/", "/STOR_POOLS/", null),
        STOR_POOL_DFNS("/STORPOOLDFNCONF/", "/STOR_POOL_DFNS/", null),

        SNAP("/SNAPSHOTS/", "/SNAPS_RSC/", "/SNAPS/"),
        SNAP_DFN("/SNAPSHOTDEFINITIONS/", "/SNAP_DFNS_RSC_DFN/", "/SNAP_DFNS/"),
        SNAP_VLM("/SNAPSHOTS/", "/SNAP_VLMS_VLM/", "/SNAP_VLMS/"),
        SNAP_VLM_DFN("/SNAPSHOTVOLUMEDEFINITIONS/", "/SNAP_VLM_DFNS_VLM_DFN/", "/SNAP_VLM_DFNS/"),

        KVS("/KEYVALUESTORES/", "/KVS/", "/KVS/");

        private final String origPrefix;
        private final String newDfltPrefix;
        private final @Nullable String newAlternativePrefix;

        private final List<Pattern> propKeyPatternsForAlternativePrefix;

        InstanceType(String origPrefixRef, String newDfltPrefixRef, @Nullable String newAlternativePrefixRef)
        {
            origPrefix = origPrefixRef;
            newDfltPrefix = newDfltPrefixRef;
            newAlternativePrefix = newAlternativePrefixRef;
            propKeyPatternsForAlternativePrefix = new ArrayList<>();
        }

        public String getOrigPrefix()
        {
            return origPrefix;
        }

        public String getNewDfltPrefix()
        {
            return newDfltPrefix;
        }

        public String getNewAlternativePrefix()
        {
            return newAlternativePrefix;
        }

        public List<Pattern> getPropKeyPatternsForAlternativePrefix()
        {
            return propKeyPatternsForAlternativePrefix;
        }
    }

    static
    {
        add(InstanceType.SNAP_DFN, "^BackupShipping/.*");
        add(InstanceType.SNAP_DFN, "^Schedule/BackupShippedBySchedule$");

        add(InstanceType.SNAP, "^Backup/SourceSnapDfnUUID$"); // this entry is wrong, but should not hurt
        add(InstanceType.SNAP, "^BackupShipping/.*");
        add(InstanceType.SNAP, "^Shipping/.*"); // this entry is wrong, but should not hurt
        add(InstanceType.SNAP, "^SnapshotShippingNamePrev$"); // this entry is wrong, but should not hurt
        add(InstanceType.SNAP, "^Satellite/EBS/EbsSnapId.*"); // this entry is wrong, but should not hurt

        // missing entries that were fixed in Migration*SplitSnapProps_FixSequenceNumber
        /*
         * SnapshotDefinition:
         *
         * "^SequenceNumber$"
         * "^Backup/SourceSnapDfnUUID$" // was wrongfully looked for in snap-props instead of snapDfn props
         * "^SnapshotShippingNamePrev$" // was wrongfully looked for in snap-props instead of snapDfn props
         * "^Shipping/.*" // was wrongfully looked for in snap-props instead of snapDfn props
         * "^Schedule/BackupStartTimestamp$"
         */

        /*
         * SnapshotVolumeDefinition
         *
         * "^Shipping/.*"
         */

        /*
         * SnapshotVolume
         *
         * "^Satellite/EBS/EbsSnapId.*" // was wrongfully looked for in snap-props instead of snapVlm props
         */
    }

    private static void add(InstanceType instanceTypeRef, String regexRef)
    {
        instanceTypeRef.propKeyPatternsForAlternativePrefix.add(Pattern.compile(regexRef));
    }

    /**
     * This method is intended to be used to migrate S3 backup's properties into the new schema. This method will not be
     * called during database-migration!
     *
     * @param instanceTypeRef Must be {@link InstanceType#SNAP}, {@link InstanceType#SNAP_DFN} or
     *     {@link InstanceType#SNAP_VLM_DFN}
     * @param inPropMapRef The original map of properties before we split the props into snap- and rsc-props
     * @param outSnapPropMapRef The map in which the snapshot-properties will be put into
     * @param outRscPropMapRef The map in which the resource-properties will be put into
     */
    public static void splitS3Props(
        InstanceType instanceTypeRef,
        Map<String, String> inPropMapRef,
        Map<String, String> outSnapPropMapRef,
        Map<String, String> outRscPropMapRef
    )
    {
        for (Entry<String, String> entry : inPropMapRef.entrySet())
        {
            final String propKey = entry.getKey();
            final Map<String, String> targetMap = useAlternativePrefix(instanceTypeRef, propKey) ?
                outSnapPropMapRef :
                outRscPropMapRef;
            targetMap.put(propKey, entry.getValue());
        }
    }

    public static boolean useAlternativePrefix(final InstanceType instanceType, String keyRef)
    {
        boolean useAlternativePrefix = false;
        for (Pattern pattern : instanceType.getPropKeyPatternsForAlternativePrefix())
        {
            if (pattern.matcher(keyRef).matches())
            {
                useAlternativePrefix = true;
                break;
            }
        }
        return useAlternativePrefix;
    }
}
