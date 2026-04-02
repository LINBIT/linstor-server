package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2025_12_31_NewUpTodateMechanic;
import com.linbit.linstor.dbcp.migration.Migration_2025_12_31_NewUpTodateMechanic.Result;
import com.linbit.linstor.dbcp.migration.Migration_2025_12_31_NewUpTodateMechanic.VlmDfnKey;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1.VolumeDefinitionsSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@K8sCrdMigration(
    description = "Set existing VlmDfns as DRBD_INITIALIZED",
    version = 30
)
public class Migration_30_v1_31_1_NewUpToDateMechanic extends BaseK8sCrdMigration
{
    public Migration_30_v1_31_1_NewUpToDateMechanic()
    {
        super(
            GenCrdV1_31_1.createMigrationContext()
        );
    }

    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        Map<VlmDfnKey, GenCrdV1_31_1.VolumeDefinitions> vlmDfnsCrds = getVlmDfnCrds(
            txFrom
        );
        Map<VlmDfnKey, Long> vlmDfns = convertVlmDfns(vlmDfnsCrds);
        Map<String, Map<String, String>> oldProps = getProps(txFrom);
        Map<VlmDfnKey, Result> migrationResult = Migration_2025_12_31_NewUpTodateMechanic.getMigrationResult(
            vlmDfns,
            oldProps
        );

        apply(txTo, migrationResult, vlmDfnsCrds);
        deleteObsoleteRscDfnPrimaryOnProp(txFrom, txTo);
        return null;
    }

    private Map<VlmDfnKey, GenCrdV1_31_1.VolumeDefinitions> getVlmDfnCrds(K8sCrdTransaction txFrom)
    {
        Map<VlmDfnKey, GenCrdV1_31_1.VolumeDefinitions> ret = new HashMap<>();

        Collection<GenCrdV1_31_1.VolumeDefinitions> vlmDfnCrdCollection;
        vlmDfnCrdCollection = txFrom.<GenCrdV1_31_1.VolumeDefinitions, GenCrdV1_31_1.VolumeDefinitionsSpec>getCrd(
            GenCrdV1_31_1.GeneratedDatabaseTables.VOLUME_DEFINITIONS
        ).values();
        for (GenCrdV1_31_1.VolumeDefinitions vlmDfnCrd : vlmDfnCrdCollection)
        {
            GenCrdV1_31_1.VolumeDefinitionsSpec vlmDfnSpec = vlmDfnCrd.getSpec();
            ret.put(
                new VlmDfnKey(
                    vlmDfnSpec.resourceName,
                    vlmDfnSpec.snapshotName,
                    vlmDfnSpec.vlmNr
                ),
                vlmDfnCrd
            );
        }
        return ret;
    }

    private Map<VlmDfnKey, Long /* VlmDfnFlags */> convertVlmDfns(
        Map<VlmDfnKey, GenCrdV1_31_1.VolumeDefinitions> vlmDfnsCrdsRef
    )
    {
        Map<VlmDfnKey, Long /* VlmDfnFlags */> ret = new HashMap<>();
        for (Map.Entry<VlmDfnKey, GenCrdV1_31_1.VolumeDefinitions> entry : vlmDfnsCrdsRef.entrySet())
        {
            VlmDfnKey vlmDfnKey = entry.getKey();
            ret.put(vlmDfnKey, entry.getValue().getSpec().vlmFlags);
        }
        return ret;
    }

    private Map<String, Map<String, String>> getProps(K8sCrdTransaction txFrom)
    {
        Collection<GenCrdV1_31_1.PropsContainers> propsCrdCollection;
        propsCrdCollection = txFrom.<GenCrdV1_31_1.PropsContainers, GenCrdV1_31_1.PropsContainersSpec>getCrd(
            GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS
        ).values();
        Map<String, Map<String, String>> ret = new HashMap<>();
        for (GenCrdV1_31_1.PropsContainers propCrd : propsCrdCollection)
        {
            GenCrdV1_31_1.PropsContainersSpec propSpec = propCrd.getSpec();
            ret.computeIfAbsent(propSpec.propsInstance, ignored -> new HashMap<>())
                .put(propSpec.propKey, propSpec.propValue);
        }
        return ret;
    }

    private void apply(
        K8sCrdTransaction txToRef,
        Map<VlmDfnKey, Result> migrationResultRef,
        Map<VlmDfnKey, GenCrdV1_31_1.VolumeDefinitions> vlmDfnsCrdsRef
    )
        throws DatabaseException
    {
        for (Map.Entry<VlmDfnKey, Result> entry : migrationResultRef.entrySet())
        {
            VlmDfnKey vlmDfnKey = entry.getKey();
            Result result = entry.getValue();
            @Nullable GenCrdV1_31_1.VolumeDefinitions vlmDfnCrd = vlmDfnsCrdsRef.get(vlmDfnKey);
            if (vlmDfnCrd != null)
            {
                VolumeDefinitionsSpec vlmDfnSpec = vlmDfnCrd.getSpec();
                txToRef.upsert(
                    GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
                    GenCrdV1_31_1.createPropsContainers(
                        result.vlmDfnPropsInstance,
                        Migration_2025_12_31_NewUpTodateMechanic.PROP_KEY_NEW_LINSTOR_DRBD_INITIAL_UPTODATE_ON,
                        result.winningNodeNamePropValue
                    )
                );
                txToRef.upsert(
                    GenCrdV1_31_1.GeneratedDatabaseTables.VOLUME_DEFINITIONS,
                    GenCrdV1_31_1.createVolumeDefinitions(
                        vlmDfnSpec.uuid,
                        vlmDfnSpec.resourceName,
                        vlmDfnSpec.snapshotName,
                        vlmDfnSpec.vlmNr,
                        vlmDfnSpec.vlmSize,
                        result.updatedFlags
                    )
                );
            }
            else
            {
                throw new DatabaseException("Migration created invalid entry. " + vlmDfnKey);
            }
        }
    }

    private void deleteObsoleteRscDfnPrimaryOnProp(K8sCrdTransaction txFromRef, K8sCrdTransaction txToRef)
    {
        Collection<GenCrdV1_31_1.PropsContainers> propsCrdCollection;
        propsCrdCollection = txFromRef.<GenCrdV1_31_1.PropsContainers, GenCrdV1_31_1.PropsContainersSpec>getCrd(
            GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS
        ).values();

        for (GenCrdV1_31_1.PropsContainers props : propsCrdCollection)
        {
            GenCrdV1_31_1.PropsContainersSpec propsSpec = props.getSpec();
            if (propsSpec.propKey.equals(Migration_2025_12_31_NewUpTodateMechanic.PROP_KEY_OLD_DRBD_PRIMARY_SET_ON))
            {
                txToRef.delete(GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS, props);
            }
        }
    }
}
