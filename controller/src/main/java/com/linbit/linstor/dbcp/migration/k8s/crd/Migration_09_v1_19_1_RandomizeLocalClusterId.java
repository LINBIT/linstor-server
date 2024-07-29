package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;

import java.util.List;
import java.util.UUID;

@K8sCrdMigration(
    description = "Randomize local cluster ID",
    version = 9
)
public class Migration_09_v1_19_1_RandomizeLocalClusterId extends BaseK8sCrdMigration
{
    public Migration_09_v1_19_1_RandomizeLocalClusterId()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        List<PropsContainers> list = txFrom.<PropsContainers, PropsContainersSpec>getClient(
            GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS
        ).list();
        String newRandomUuid = UUID.randomUUID().toString().toLowerCase();
        for (PropsContainers propCrd : list)
        {
            PropsContainersSpec propSpec = propCrd.getSpec();
            if (propSpec.propKey.equals("Cluster/LocalID"))
            {
                txTo.replace(
                    GenCrdV1_19_1.GeneratedDatabaseTables.PROPS_CONTAINERS,
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
