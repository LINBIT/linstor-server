package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbcp.migration.Migration_2025_10_28_DisableAutoBlockSizesForExistingResources;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@K8sCrdMigration(
    description = "Disable auto-block-size for existing resources",
    version = 29
)
public class Migration_29_v1_31_1_DisableAutoBlockSizesForExistingResources extends BaseK8sCrdMigration
{
    public Migration_29_v1_31_1_DisableAutoBlockSizesForExistingResources()
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

        Collection<String> rscNames = getRscNames(txFrom);

        HashMap<String, HashMap<String, String>> blockSizeProps = Migration_2025_10_28_DisableAutoBlockSizesForExistingResources.getPropsToInsert(
            rscNames
        );

        createNewPropEntries(txTo, blockSizeProps);

        return null;
    }

    private Collection<String> getRscNames(K8sCrdTransaction txFrom)
    {
        HashSet<String> ret = new HashSet<>();
        Collection<GenCrdV1_31_1.ResourceDefinitionsSpec> allRscDfnSpecs = txFrom.<GenCrdV1_31_1.ResourceDefinitions, GenCrdV1_31_1.ResourceDefinitionsSpec>getSpec(
            GenCrdV1_31_1.GeneratedDatabaseTables.RESOURCE_DEFINITIONS
        ).values();
        for (GenCrdV1_31_1.ResourceDefinitionsSpec rscDfnSpec : allRscDfnSpecs)
        {
            ret.add(rscDfnSpec.resourceDspName);
        }
        return ret;
    }


    private void createNewPropEntries(K8sCrdTransaction txTo, HashMap<String, HashMap<String, String>> blockSizeProps)
    {
        for (Map.Entry<String, HashMap<String, String>> outerEntry : blockSizeProps.entrySet())
        {
            String instanceName = outerEntry.getKey();
            for (Map.Entry<String, String> innerEntry : outerEntry.getValue().entrySet())
            {
                String propKey = innerEntry.getKey();
                String propValue = innerEntry.getValue();

                txTo.create(
                    GenCrdV1_31_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
                    GenCrdV1_31_1.createPropsContainers(instanceName, propKey, propValue)
                );
            }
        }
    }
}
