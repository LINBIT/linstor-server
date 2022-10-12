package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;

import java.util.List;
import java.util.UUID;

@K8sCrdMigration(
    description = "Randomize local cluster ID",
    version = 9
)
public class Migration_9_v1_20_0_RandomizeLocalClusterId extends BaseK8sCrdMigration
{
    public Migration_9_v1_20_0_RandomizeLocalClusterId()
    {
        super(
            GenCrdV1_19_1.createTxMgrContext(),
            GenCrdV1_19_1.createTxMgrContext(),
            GenCrdV1_19_1.createSchemaUpdateContext()
        );
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        List<PropsContainers> list = txFrom.<PropsContainers, PropsContainersSpec>getClient(
            GeneratedDatabaseTables.PROPS_CONTAINERS
        ).list();
        String newRandomUuid = UUID.randomUUID().toString().toLowerCase();
        for (PropsContainers propCrd : list)
        {
            PropsContainersSpec propSpec = propCrd.getSpec();
            if (propSpec.propKey.equals("Cluster/LocalID"))
            {
                txTo.replace(
                    GeneratedDatabaseTables.PROPS_CONTAINERS,
                    GenCrdV1_19_1.createPropsContainers(
                        propSpec.propsInstance,
                        propSpec.propKey,
                        newRandomUuid
                    )
                );
            }
        }

        return null;
    }
}
