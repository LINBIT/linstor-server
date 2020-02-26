package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.transaction.EtcdTransaction;

import static com.linbit.linstor.dbdrivers.etcd.EtcdUtils.PK_DELIMITER;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@EtcdMigration(
    description = "Unify resources and snapshots",
    version = 32
)
// corresponds to Migration_2019_11_21_UnifyResourceAndSnapshot
public class Migration_05_UnifyResourcesAndSnapshots extends BaseEtcdMigration
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String TBL_RSC_DFN = "RESOURCE_DEFINITIONS";
    private static final String TBL_RSC = "RESOURCES";
    private static final String TBL_RSC_CON = "RESOURCE_CONNECTIONS";
    private static final String TBL_VLM_DFN = "VOLUME_DEFINITIONS";
    private static final String TBL_VLM = "VOLUMES";
    private static final String TBL_VLM_CON = "VOLUME_CONNECTIONS";
    private static final String TBL_LAYER_DRBD_RD = "LAYER_DRBD_RESOURCE_DEFINITIONS";
    private static final String TBL_LAYER_DRBD_R = "LAYER_DRBD_RESOURCES";
    private static final String TBL_LAYER_DRBD_VD = "LAYER_DRBD_VOLUME_DEFINITIONS";
    private static final String TBL_LAYER_DRBD_V = "LAYER_DRBD_VOLUMES";
    private static final String TBL_LAYER_IDS = "LAYER_RESOURCE_IDS";
    private static final String TBL_LAYER_SF_VD = "LAYER_SWORDFISH_VOLUME_DEFINITIONS";
    private static final String TBL_LAYER_LUKS_V = "LAYER_LUKS_VOLUMES";
    private static final String TBL_LAYER_S_V = "LAYER_STORAGE_VOLUMES";
    private static final String TBL_SNAP_DFN = "SNAPSHOT_DEFINITIONS";
    private static final String TBL_SNAP = "SNAPSHOTS";
    private static final String TBL_SNAP_VLM_DFN = "SNAPSHOT_VOLUME_DEFINITIONS";
    private static final String TBL_SNAP_VLM = "SNAPSHOT_VOLUMES";
    private static final String TBL_STOR_POOL = "NODE_STOR_POOL";
    private static final String TBL_OBJ_PROT = "SEC_OBJECT_PROTECTION";
    private static final String TBL_ACL = "SEC_ACL_MAP";

    private static final String CLM_UUID = "UUID";
    private static final String CLM_PARENT_UUID = "PARENT_UUID";
    private static final String CLM_NODE_NAME = "NODE_NAME";
    private static final String CLM_NODE_NAME_SRC = "NODE_NAME_SRC";
    private static final String CLM_NODE_NAME_DST = "NODE_NAME_DST";
    private static final String CLM_RSC_NAME = "RESOURCE_NAME";
    private static final String CLM_RSC_DSP_NAME = "RESOURCE_DSP_NAME";
    private static final String CLM_RSC_EXT_NAME = "RESOURCE_EXTERNAL_NAME";
    private static final String CLM_RSC_GRP_NAME = "RESOURCE_GROUP_NAME";
    private static final String CLM_RSC_NAME_SUFFIX = "RESOURCE_NAME_SUFFIX";
    private static final String CLM_RSC_FLAGS = "RESOURCE_FLAGS";
    private static final String CLM_VLM_NR = "VLM_NR";
    private static final String CLM_VLM_SIZE = "VLM_SIZE";
    private static final String CLM_VLM_FLAGS = "VLM_FLAGS";
    private static final String CLM_SNAP_NAME = "SNAPSHOT_NAME";
    private static final String CLM_SNAP_DSP_NAME = "SNAPSHOT_DSP_NAME";
    private static final String CLM_SNAP_FLAGS = "SNAPSHOT_FLAGS";
    private static final String CLM_LAYER_STACK = "LAYER_STACK";
    private static final String CLM_NODE_ID = "NODE_ID";
    private static final String CLM_STOR_POOL_NAME = "STOR_POOL_NAME";
    private static final String CLM_LAYER_RSC_ID = "LAYER_RESOURCE_ID";
    private static final String CLM_LAYER_RSC_NAME_SUFFIX = "LAYER_RESOURCE_SUFFIX";
    private static final String CLM_LAYER_KIND = "LAYER_RESOURCE_KIND";
    private static final String CLM_LAYER_PARENT_ID = "LAYER_RESOURCE_PARENT_ID";
    private static final String CLM_PEER_SLOTS = "PEER_SLOTS";
    private static final String CLM_AL_STRIPES = "AL_STRIPES";
    private static final String CLM_AL_STRIPE_SIZE = "AL_STRIPE_SIZE";
    private static final String CLM_FLAGS = "FLAGS";
    private static final String CLM_TCP_PORT = "TCP_PORT";
    private static final String CLM_TRANSPORT_TYPE = "TRANSPORT_TYPE";
    private static final String CLM_SECRET = "SECRET";
    private static final String CLM_VLM_MINOR_NR = "VLM_MINOR_NR";
    private static final String CLM_POOL_NAME = "POOL_NAME";
    private static final String CLM_ENCRYPTED_PASSWORD = "ENCRYPTED_PASSWORD";
    private static final String CLM_PROVIDER_KIND = "PROVIDER_KIND";
    private static final String CLM_VLM_ODATA = "VLM_ODATA";
    private static final String CLM_DRIVER_NAME = "DRIVER_NAME";
    private static final String CLM_OBJ_PATH = "OBJECT_PATH";
    private static final String CLM_CREATOR_ID_NAME = "CREATOR_IDENTITY_NAME";
    private static final String CLM_OWNER_ROLE_NAME = "OWNER_ROLE_NAME";
    private static final String CLM_SEC_TYPE_NAME = "SECURITY_TYPE_NAME";
    private static final String CLM_ROLE_NAME = "ROLE_NAME";
    private static final String CLM_ACCESS_TYPE = "ACCESS_TYPE";

    private static final String LAYER_KIND_DRBD = "DRBD";
    private static final String LAYER_KIND_LUKS = "LUKS";
    private static final String LAYER_KIND_NVME = "NVME";
    private static final String LAYER_KIND_STORAGE = "STORAGE";

    private static final String DFLT_SNAP_NAME = ""; // intentionally empty
    /**
     * RESOURCE_NAME_SUFFIX cannot be anything else for snapshots during this migration
     */
    private static final String DFLT_RSC_NAME_SUFFIX = ""; // intentionally empty
    private static final String DFLT_TRANSPORT_TYPE = "IP";

    // temporary containers to avoid unnecessary get requests
    private static final Map<Key, SnapInfo> SNAP_INFO_MAP = new HashMap<>();

    private static final String NULL = ":null";

    @Override
    public void migrate(EtcdTransaction tx) throws Exception
    {
        Function<String, String> simpleExtend = name -> name + PK_DELIMITER;
        Function<String, String> simpleDuplicate = name -> name.replace(PK_DELIMITER, PK_DELIMITER + PK_DELIMITER);
        addEmptySnapshotNameEntry(
            tx,
            new Pair<>(TBL_RSC_DFN, simpleExtend),
            new Pair<>(TBL_RSC, simpleExtend),
            new Pair<>(TBL_RSC_CON, simpleExtend),
            new Pair<>(TBL_VLM_DFN, simpleDuplicate),
            new Pair<>(TBL_VLM,
                name -> {
                    // nodeName, rscName, [snapName, ] vlmNr
                    String[] split = EtcdUtils.splitPks(name, false);
                    return join(PK_DELIMITER, split[0], split[1], DFLT_SNAP_NAME, split[2]);
            }),
            new Pair<>(TBL_VLM_CON,
                name -> {
                    // nodeNameSrc, nodeNameDst, rscName, [snapName, ] vlmNr
                    String[] split = EtcdUtils.splitPks(name, false);
                    return join(PK_DELIMITER, split[0], split[1], split[2], DFLT_SNAP_NAME, split[3]);
                }
            ),
            new Pair<>(TBL_LAYER_IDS, Function.identity()), // snapname is not part of PK
            new Pair<>(TBL_LAYER_DRBD_RD, simpleDuplicate),
            new Pair<>(TBL_LAYER_DRBD_VD,
                name -> {
                    // rscName, [snapName, ]rscNameSuffix, vlmNr
                    String[] split = EtcdUtils.splitPks(name, false);
                    return join(PK_DELIMITER, split[0], DFLT_SNAP_NAME, split[1], split[2]);
                }),
            new Pair<>(TBL_LAYER_SF_VD,
                name -> {
                    // rscNAme, [snapName, ] rscNameSuffix, vlmNr
                    String[] split = EtcdUtils.splitPks(name, false);
                    return join(PK_DELIMITER, split[0], DFLT_SNAP_NAME, split[1], split[2]);
                }
            )
        );
        addEmptySnapshotDisplayName(tx, new Pair<>(TBL_RSC_DFN, simpleExtend));

        copySnapshotDefinitionsIntoResourceDefinitions(tx);
        copySnapshotVolumeDefinitionsIntoVolumeDefinitions(tx);
        copySnapshotsIntoResources(tx);
        copySnapshotVolumesIntoVolumes(tx);

        createObjectProtectionForSnapshotDefinitions(tx);

        createLayerDataForSnapshots(tx);

        deleteSnapshotTables(tx);
    }

    private void addEmptySnapshotNameEntry(
        EtcdTransaction tx,
        Pair<String, Function<String, String>>... tables
    )
    {
        addEmptyColumn(tx, CLM_SNAP_NAME, tables);
    }

    private void addEmptySnapshotDisplayName(
        EtcdTransaction tx,
        Pair<String, Function<String, String>>... tables
    )
    {
        addEmptyColumn(tx, CLM_SNAP_DSP_NAME, tables);
    }

    private void addEmptyColumn(
        EtcdTransaction tx,
        String columnName,
        Pair<String, Function<String, String>>... pairsRef
    )
    {
        for (Pair<String, Function<String, String>> pair : pairsRef)
        {
            String key = "LINSTOR/" + pair.a;

            TreeMap<String, String> readTable = tx.get(key, true);
            Set<String> pks = EtcdUtils.getComposedPkList(readTable);

            for (String pk : pks)
            {
                tx.put(
                    buildKeyStr(
                        pair.a,
                        columnName,
                        pair.b.apply(pk)
                    ),
                    DFLT_SNAP_NAME
                );
            }
            for (Entry<String, String> oldEntry : readTable.entrySet())
            {
                String oldKey = oldEntry.getKey();
                String oldComposedKey = extractPrimaryKey(oldKey);
                String oldClm = getColumnName(oldKey);
                tx.delete(oldKey, false);
                tx.delete(buildKeyStr(pair.a, oldClm, oldComposedKey), false);
                tx.put(
                    buildKeyStr(
                        pair.a,
                        oldClm,
                        pair.b.apply(oldComposedKey)
                    ),
                    oldEntry.getValue()
                );
            }
        }
    }

    private void copySnapshotDefinitionsIntoResourceDefinitions(EtcdTransaction tx)
    {
        // we will need a reference from snapshotDfn to "parent" resourceDfn.uuid
        TreeMap<String, String> rscDfnNameToUuid = new TreeMap<>();
        {
            TreeMap<String, String> rscDfnTbl = tx.get("LINSTOR/" + TBL_RSC_DFN, true);
            for (Entry<String, String> entry : rscDfnTbl.entrySet())
            {
                String key = entry.getKey();
                if (key.endsWith(CLM_UUID))
                {
                    String pk = extractPrimaryKey(key); // only RESOURCE_NAME
                    rscDfnNameToUuid.put(pk, entry.getValue());
                }
            }
        }

        // copy data from SnapshotDefinition to ResourceDefinition
        TreeMap<String, String> snapDfnTbl = tx.get("LINSTOR/" + TBL_SNAP_DFN, true);
        for (Entry<String, String> entry : snapDfnTbl.entrySet())
        {
            String entryKey = entry.getKey();
            String entryValue = entry.getValue();

            String col = getColumnName(entryKey);

            String[] pk = EtcdUtils.splitPks(
                extractPrimaryKey(entryKey), // RESOURCE_NAME,SNAPSHOT_NAME
                false
            );

            boolean copyData = true;
            switch (col)
            {
                case CLM_UUID:
                    // additional to copy the UUID, write columns that did not exist in
                    // SnapshotDefinitions

                    tx.put(
                        buildKeyStr(TBL_RSC_DFN, CLM_PARENT_UUID, pk),
                        rscDfnNameToUuid.get(pk[0])
                    );

                    tx.put(
                        buildKeyStr(TBL_RSC_DFN, CLM_LAYER_STACK, pk),
                        "'[]'"
                    );
                    tx.put(
                        buildKeyStr(TBL_RSC_DFN, CLM_RSC_GRP_NAME, pk),
                        InternalApiConsts.DEFAULT_RSC_GRP_NAME.toUpperCase()
                    );
                    break;
                case CLM_SNAP_FLAGS:
                    // rename SNAPSHOT_FLAGS to RESOURCE_FLAGS
                    tx.put(
                        buildKeyStr(TBL_RSC_DFN, CLM_RSC_FLAGS, pk),
                        entryValue
                    );
                    copyData = false; // DO NOT copy old SNAPSHOT_FLAGS
                    break;
                default:
                    break;
            }
            if (copyData)
            {
                tx.put(buildKeyStr(TBL_RSC_DFN, col, pk), entryValue);
            }
        }
    }

    private void copySnapshotVolumeDefinitionsIntoVolumeDefinitions(EtcdTransaction tx)
    {
        TreeMap<String, String> snapVlmDfnTbl = tx.get("LINSTOR/" + TBL_SNAP_VLM_DFN, true);
        for (Entry<String, String> entry : snapVlmDfnTbl.entrySet())
        {
            String entryKey = entry.getKey();
            String entryValue = entry.getValue();

            // RESOURCE_NAME, SNAPSHOT_NAME, VLM_NR
            String[] composedPk = EtcdUtils.splitPks(extractPrimaryKey(entryKey), false);
            String col = getColumnName(entryKey);

            if (CLM_SNAP_FLAGS.equals(col))
            {
                // rename SNAPSHOT_FLAGS to VLM_FLAGS
                tx.put(
                    buildKeyStr(TBL_VLM_DFN, CLM_VLM_FLAGS, composedPk),
                    entryValue
                );
            }
            else
            {
                tx.put(buildKeyStr(TBL_VLM_DFN, col, composedPk), entryValue);
            }
        }
    }

    private void copySnapshotsIntoResources(EtcdTransaction tx)
    {
        TreeSet<String> doNotCopy = new TreeSet<>();
        doNotCopy.add(CLM_NODE_ID);
        doNotCopy.add(CLM_LAYER_STACK);

        TreeMap<String, String> snapVlmDfnTbl = tx.get("LINSTOR/" + TBL_SNAP, true);
        for (Entry<String, String> entry : snapVlmDfnTbl.entrySet())
        {
            String entryKey = entry.getKey();
            String entryValue = entry.getValue();

            // NODE_NAME, RESOURCE_NAME, SNAPSHOT_NAME
            String[] composedPk = EtcdUtils.splitPks(extractPrimaryKey(entryKey), false);
            String col = getColumnName(entryKey);

            boolean copyData = true;
            switch (col)
            {
                case CLM_SNAP_FLAGS:
                    // rename SNAPSHOT_FLAGS to RESOURCE_FLAGS
                    tx.put(
                        buildKeyStr(TBL_RSC, CLM_RSC_FLAGS, composedPk),
                        entryValue
                    );
                    break;
                case CLM_NODE_ID:
                    copyData = false;
                    SNAP_INFO_MAP.computeIfAbsent(
                        new Key(composedPk[0], composedPk[1], composedPk[2]),
                        SnapInfo::new
                    ).nodeId = entryValue;
                    break;
                case CLM_LAYER_STACK:
                    copyData = false;
                    SNAP_INFO_MAP.computeIfAbsent(
                        new Key(composedPk[0], composedPk[1], composedPk[2]),
                        SnapInfo::new
                    ).layerStack = entryValue;
                    break;
                default:
                    break;
            }

            if (copyData)
            {
                tx.put(buildKeyStr(TBL_RSC, col, composedPk), entryValue);
            }
        }
    }

    private void copySnapshotVolumesIntoVolumes(EtcdTransaction tx)
    {
        TreeMap<String, String> snapDfnTbl = tx.get("LINSTOR/" + TBL_SNAP_VLM, true);
        for (Entry<String, String> entry : snapDfnTbl.entrySet())
        {
            String entryKey = entry.getKey();
            String entryValue = entry.getValue();

            // NODE_NAME, RESOURCE_NAME, SNAPSHOT_NAME, VLM_NR
            String[] composedPk = EtcdUtils.splitPks(extractPrimaryKey(entryKey), false);
            String col = getColumnName(entryKey);

            boolean copyData = true;
            switch (col)
            {
                case CLM_UUID:
                    // additional to copy the UUID, write columns that did not exist in
                    // SnapshotVolumes

                    tx.put(
                        buildKeyStr(TBL_VLM, CLM_VLM_FLAGS, composedPk),
                        "0"
                    );
                    break;
                case CLM_SNAP_FLAGS:
                    // rename SNAPSHOT_FLAGS to VOLUME_FLAGS
                    tx.put(
                        buildKeyStr(TBL_VLM, CLM_VLM_FLAGS, composedPk),
                        entryValue
                    );
                    copyData = false; // DO NOT copy old SNAPSHOT_FLAGS
                    break;
                case CLM_STOR_POOL_NAME:
                    copyData = false;
                    SnapVlmInfo snapVlmInfo = new SnapVlmInfo();
                    snapVlmInfo.storPoolName = entryValue;
                    SNAP_INFO_MAP.get(
                        new Key(composedPk[0], composedPk[1], composedPk[2])
                    ).vlmMap.put(composedPk[3], snapVlmInfo);
                    break;
                default:
                    break;
            }
            if (copyData)
            {
                tx.put(buildKeyStr(TBL_VLM, col, composedPk), entryValue);
            }
        }
    }

    private void createObjectProtectionForSnapshotDefinitions(EtcdTransaction tx)
    {
        String[] pubRolesForObjProtColms = new String[]
        {
            CLM_CREATOR_ID_NAME,
            CLM_OWNER_ROLE_NAME,
            CLM_SEC_TYPE_NAME
        };

        TreeMap<String, String> snapDfnTbl = tx.get("LINSTOR/" + TBL_SNAP_DFN, true);
        for (Entry<String, String> entry : snapDfnTbl.entrySet())
        {
            String[] composedKey = EtcdUtils.splitPks(
                extractPrimaryKey(entry.getKey()), // RESOURCE_NAME,SNAPSHOT_NAME
                false
            );

            String objProtPath = "/snapshotdefinitions/" +
                composedKey[0] +
                EtcdUtils.PATH_DELIMITER + // NOT PK_DELIMITER...
                composedKey[1];
            for (String clm : pubRolesForObjProtColms)
            {
                tx.put(
                    buildKeyStr(
                        TBL_OBJ_PROT,
                        clm,
                        objProtPath
                    ),
                    "PUBLIC"
                );
            }

            tx.put(
                buildKeyStr(
                    TBL_ACL,
                    CLM_ROLE_NAME,
                    objProtPath
                ),
                "PUBLIC"
            );
            tx.put(
                buildKeyStr(
                    TBL_ACL,
                    CLM_ACCESS_TYPE,
                    objProtPath
                ),
                "15"
            );
        }
    }

    private void createLayerDataForSnapshots(EtcdTransaction tx)
        throws JsonMappingException, JsonProcessingException
    {
        HashMap<Key, String> storPoolToKind = new HashMap<>();
        {
            TreeMap<String, String> storPoolTbl = tx.get("LINSTOR/" + TBL_STOR_POOL, true);
            for (Entry<String, String> entry : storPoolTbl.entrySet())
            {
                String col = getColumnName(entry.getKey());
                if (CLM_DRIVER_NAME.equals(col))
                {
                    String pk = extractPrimaryKey(entry.getKey());
                    String[] composedKey = EtcdUtils.splitPks(pk, false);
                    storPoolToKind.put(new Key(composedKey[0], composedKey[1]), entry.getValue());
                }
            }
        }

        int nextLRI = -1;
        {
            Set<String> composedPkList = EtcdUtils.getComposedPkList(
                tx.get(
                    "LINSTOR/" + TBL_LAYER_IDS,
                    true
                )
            );
            for (String pk : composedPkList) {
                int lri = Integer.parseInt(pk);
                if (lri > nextLRI) {
                    nextLRI = lri;
                }
            }

            nextLRI++;
        }

        for (SnapInfo snapInfo : SNAP_INFO_MAP.values())
        {
            String nodeName = snapInfo.nodeName;
            String rscName = snapInfo.rscName;
            String snapName = snapInfo.snapName;
            String nodeId = snapInfo.nodeId;
            List<String> layerStack = OBJECT_MAPPER.readValue(snapInfo.layerStack, List.class);

            Integer parentId = null;
            for (String layer : layerStack)
            {
                switch (layer)
                {
                    case LAYER_KIND_DRBD:
                        createLayerDrbdRscDfnEntry(tx, rscName, snapName);
                        parentId = createLayerDrbdRscEntry(
                            tx, nextLRI++, parentId, nodeName, rscName, snapName, nodeId
                        );
                        for (Entry<String, SnapVlmInfo> entry : snapInfo.vlmMap.entrySet())
                        {
                            String vlmNr = entry.getKey();

                            createLayerDrbdVlmDfnEntry(tx, rscName, snapName, vlmNr);
                            createLayerDrbdVlmEntry(tx, parentId, vlmNr);
                        }
                        break;
                    case LAYER_KIND_LUKS:
                        // we did not save the password.
                        // we will create the luks layer data, but the snapshot will not be usable
                        parentId = createLayerRscIdEntry(
                            tx,
                            nextLRI++,
                            nodeName,
                            rscName,
                            snapName,
                            LAYER_KIND_LUKS,
                            parentId
                        );
                        for (Entry<String, SnapVlmInfo> entry : snapInfo.vlmMap.entrySet())
                        {
                            String vlmNr = entry.getKey();
                            String storPoolName = entry.getValue().storPoolName;
                            String storPoolKind = storPoolToKind.get(new Key(nodeName, storPoolName));

                            createLayerLuksVlmEntry(tx, parentId, vlmNr); // no key :(
                        }
                        break;
                    case LAYER_KIND_NVME:
                        parentId = createLayerRscIdEntry(
                            tx, nextLRI++, nodeName, rscName, snapName, LAYER_KIND_NVME, parentId
                        );
                        break;
                    case LAYER_KIND_STORAGE:
                        parentId = createLayerRscIdEntry(
                            tx,
                            nextLRI++,
                            nodeName,
                            rscName,
                            snapName,
                            LAYER_KIND_STORAGE,
                            parentId
                        );
                        for (Entry<String, SnapVlmInfo> entry : snapInfo.vlmMap.entrySet())
                        {
                            String vlmNr = entry.getKey();
                            String storPoolName = entry.getValue().storPoolName;
                            String storPoolKind = storPoolToKind.get(new Key(nodeName, storPoolName));

                            createLayerStorVlmEntry(tx, parentId, vlmNr, storPoolKind, nodeName, storPoolName);
                        }
                        break;
                    default:
                        throw new ImplementationError("Unknown layer kind: " + layer);
                }
            }
        }
    }

    private void createLayerDrbdRscDfnEntry(
        EtcdTransaction tx,
        String rscName,
        String snapName
    )
    {
        TreeMap<String, String> relativeMap = new TreeMap<>();

        relativeMap.put(CLM_PEER_SLOTS, "" + InternalApiConsts.DEFAULT_PEER_SLOTS); // we have nothing else saved
        relativeMap.put(CLM_AL_STRIPES, "" + InternalApiConsts.DEFAULT_AL_STRIPES);
        relativeMap.put(CLM_AL_STRIPE_SIZE, "" + InternalApiConsts.DEFAULT_AL_SIZE);
        relativeMap.put(CLM_TCP_PORT, NULL);
        relativeMap.put(CLM_TRANSPORT_TYPE, DFLT_TRANSPORT_TYPE);
        // no secret

        write(
            tx,
            TBL_LAYER_DRBD_RD,
            join(
                PK_DELIMITER,
                rscName,
                DFLT_RSC_NAME_SUFFIX,
                snapName
            ),
            relativeMap
        );
    }

    private void createLayerDrbdVlmDfnEntry(
        EtcdTransaction tx,
        String rscName,
        String snapName,
        String vlmNr
    )
    {
        TreeMap<String, String> relativeMap = new TreeMap<>();

        relativeMap.put(CLM_VLM_MINOR_NR, NULL);

        write(
            tx,
            TBL_LAYER_DRBD_VD,
            join(
                PK_DELIMITER,
                rscName,
                DFLT_RSC_NAME_SUFFIX,
                snapName,
                vlmNr
            ),
            relativeMap
        );
    }

    private String join(String delimiter, String... arr)
    {
        StringBuilder sb = new StringBuilder();
        for (String elem : arr)
        {
            sb.append(elem).append(delimiter);
        }
        if (sb.length() > 0)
        {
            sb.setLength(sb.length() - delimiter.length());
        }
        return sb.toString();
    }

    private Integer createLayerDrbdRscEntry(
        EtcdTransaction tx,
        int myId,
        Integer parentId,
        String nodeName,
        String rscName,
        String snapName,
        String nodeId
    )
    {
        createLayerRscIdEntry(tx, myId, nodeName, rscName, snapName, LAYER_KIND_DRBD, parentId);

        TreeMap<String, String> relativeMap = new TreeMap<>();

        relativeMap.put(CLM_PEER_SLOTS, "" + InternalApiConsts.DEFAULT_PEER_SLOTS); // we have nothing else saved
        relativeMap.put(CLM_AL_STRIPES, "" + InternalApiConsts.DEFAULT_AL_STRIPES);
        relativeMap.put(CLM_AL_STRIPE_SIZE, "" + InternalApiConsts.DEFAULT_AL_SIZE);
        relativeMap.put(CLM_FLAGS, "0"); // we have no flags saved
        if (nodeId != null)
        {
            // should never be null, but if it still is, at least we should not abort migration.
            relativeMap.put(CLM_NODE_ID, nodeId);
        }
        write(tx, TBL_LAYER_DRBD_R, myId + "", relativeMap);

        return myId;
    }

    private void createLayerDrbdVlmEntry(
        EtcdTransaction tx,
        Integer parentId,
        String vlmNr
    )
    {
        TreeMap<String, String> relativeMap = new TreeMap<>();
        relativeMap.put(CLM_NODE_NAME, ":null");
        relativeMap.put(CLM_POOL_NAME, ":null");

        write(
            tx,
            TBL_LAYER_DRBD_V,
            parentId.toString() + PK_DELIMITER + vlmNr,
            relativeMap
        );
    }

    private void createLayerLuksVlmEntry(
        EtcdTransaction tx,
        Integer parentIdRef,
        String vlmNr
    )
    {
        TreeMap<String, String> relativeMap = new TreeMap<>();
        relativeMap.put(CLM_VLM_NR, vlmNr);
        relativeMap.put(CLM_ENCRYPTED_PASSWORD, ""); // we did not save that before :(

        write(
            tx,
            TBL_LAYER_LUKS_V,
            parentIdRef.toString() + PK_DELIMITER + vlmNr,
            relativeMap
        );
    }

    private Integer createLayerRscIdEntry(
        EtcdTransaction tx,
        int myId,
        String nodeName,
        String rscName,
        String snapName,
        String layerKind,
        Integer parentId
    )
    {
        TreeMap<String, String> relativeMap = new TreeMap<>();
        relativeMap.put(CLM_NODE_NAME, nodeName);
        relativeMap.put(CLM_RSC_NAME, rscName);
        relativeMap.put(CLM_SNAP_NAME, snapName);
        relativeMap.put(CLM_LAYER_KIND, layerKind);
        if (parentId != null)
        {
            relativeMap.put(CLM_LAYER_PARENT_ID, parentId.toString());
        }
        relativeMap.put(CLM_LAYER_RSC_NAME_SUFFIX, DFLT_RSC_NAME_SUFFIX);

        write(tx, TBL_LAYER_IDS, "" + myId, relativeMap);

        return myId;
    }

    private void createLayerStorVlmEntry(
        EtcdTransaction tx,
        Integer parentId,
        String vlmNr,
        String storPoolKind,
        String nodeName,
        String storPoolName
    )
    {
        TreeMap<String, String> relativeMap = new TreeMap<>();
        relativeMap.put(CLM_PROVIDER_KIND, storPoolKind);
        relativeMap.put(CLM_NODE_NAME, nodeName);
        relativeMap.put(CLM_STOR_POOL_NAME, storPoolName);

        write(tx, TBL_LAYER_S_V, parentId + PK_DELIMITER + vlmNr, relativeMap);
    }

    private void write(
        EtcdTransaction tx,
        String table,
        String combinedPk,
        TreeMap<String, String> relativeMap
    )
    {
        for (Entry<String, String> entry : relativeMap.entrySet())
        {
            tx.put(
                join(
                    EtcdUtils.PATH_DELIMITER,
                    "LINSTOR",
                    table,
                    combinedPk,
                    entry.getKey()
                ),
                entry.getValue()
            );
        }
    }

    private void deleteSnapshotTables(EtcdTransaction tx)
    {
        String[] tablesToDelete = new String[]
        {
            TBL_SNAP_VLM,
            TBL_SNAP_VLM_DFN,
            TBL_SNAP,
            TBL_SNAP_DFN
        };
        for (String table : tablesToDelete)
        {
            tx.delete("LINSTOR/" + table, true);
        }
    }

    private static class Key
    {
        private final Object[] keys;
        private final int hash;

        private Key(Object... keysRef)
        {
            keys = keysRef;
            final int prime = 31;
            int result = 1;
            hash = prime * result + Arrays.deepHashCode(keys);
        }

        @Override
        public int hashCode()
        {
            return hash;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = obj != null && obj instanceof Key;
            if (eq)
            {
                eq = Arrays.deepEquals(keys, ((Key) obj).keys);
            }
            return eq;
        }
    }

    private static class SnapInfo
    {
        private String nodeName;
        private String rscName;
        private String snapName;
        private String nodeId;
        private String layerStack;

        private Map<String, SnapVlmInfo> vlmMap = new TreeMap<>();

        SnapInfo(Key key)
        {
            nodeName = (String) key.keys[0];
            rscName = (String) key.keys[1];
            snapName = (String) key.keys[2];
        }
    }

    private static class SnapVlmInfo
    {
        private String storPoolName;
    }

    private static class Pair<A, B>
    {
        A a;
        B b;

        Pair(A aRef, B bRef)
        {
            a = aRef;
            b = bRef;
        }
    }
}
