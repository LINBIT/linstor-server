package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes;
import com.linbit.linstor.dbcp.migration.Migration_2025_01_29_SplitSnapPropsMoreFixes.Changes;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainersSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@K8sCrdMigration(
    description = "Split snapshotted resource properties - Fix RscGrp instance names",
    version = 25
)
public class Migration_25_v1_27_1_SplitSnapPropsFromRscProps_FixRscGrpInstanceName extends BaseK8sCrdMigration
{
    public Migration_25_v1_27_1_SplitSnapPropsFromRscProps_FixRscGrpInstanceName()
    {
        super(GenCrdV1_27_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(MigrationContext ctx) throws Exception
    {
        K8sCrdTransaction txFrom = ctx.txFrom;
        K8sCrdTransaction txTo = ctx.txTo;
        Collection<PropsContainers> crdList = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            PROPS_CONTAINERS
        ).values();

        Map<String/* instanceName */, Map<String/* propKey */, String/* propValue */>> allProps = new HashMap<>();
        Map<String, Map<String, PropsContainersSpec>> propsToSpec = new HashMap<>();

        for (PropsContainers propsCrd : crdList)
        {
            PropsContainersSpec propsSpec = propsCrd.getSpec();
            allProps.computeIfAbsent(propsSpec.propsInstance, ignored -> new HashMap<>())
                .put(propsSpec.propKey, propsSpec.propValue);
            propsToSpec.computeIfAbsent(propsSpec.propsInstance, ignored -> new HashMap<>())
                .put(propsSpec.propKey, propsSpec);
        }

        Changes changes = Migration_2025_01_29_SplitSnapPropsMoreFixes.calculateChanges(allProps);

        for (Entry<String, Set<String>> entry : changes.entriesToOverrideInstanceName.entrySet())
        {
            String oldInstanceName = entry.getKey();
            String newInstanceName = Migration_2025_01_29_SplitSnapPropsMoreFixes.getNewInstanceName(oldInstanceName);

            Map<String, PropsContainersSpec> specMap = propsToSpec.get(oldInstanceName);
            for (String propKey : entry.getValue())
            {
                PropsContainersSpec spec = specMap.get(propKey);
                txTo.delete(PROPS_CONTAINERS, spec.getCrd());
                txTo.create(
                    PROPS_CONTAINERS,
                    GenCrdV1_27_1.createPropsContainers(
                        newInstanceName,
                        spec.propKey,
                        spec.propValue
                    )
                );
            }
        }

        for (Entry<String, Map<String, String>> entry : changes.entriesToUpdateValues.entrySet())
        {
            String oldInstanceName = entry.getKey();
            String newInstanceName = Migration_2025_01_29_SplitSnapPropsMoreFixes.getNewInstanceName(oldInstanceName);

            Map<String, PropsContainersSpec> specMapToDelete = propsToSpec.get(oldInstanceName);
            Map<String, PropsContainersSpec> specMapToUpdate = propsToSpec.get(newInstanceName);
            for (Entry<String, String> propsEntry : entry.getValue().entrySet())
            {
                PropsContainersSpec specToUpdate = specMapToUpdate.get(propsEntry.getKey());
                txTo.replace(
                    PROPS_CONTAINERS,
                    GenCrdV1_27_1.createPropsContainers(
                        newInstanceName,
                        specToUpdate.propKey,
                        propsEntry.getValue()
                    )
                );

                PropsContainersSpec specToDelete = specMapToDelete.get(propsEntry.getKey());
                txTo.delete(PROPS_CONTAINERS, specToDelete.getCrd());
            }
        }
        return null;
    }
}
