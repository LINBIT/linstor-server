package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_19_1.PropsContainersSpec;

import java.util.Collection;

@K8sCrdMigration(
    description = "Upper case props instance",
    version = 13
)
public class Migration_13_v1_19_1_UpperCasePropsInstance extends BaseK8sCrdMigration
{
    public Migration_13_v1_19_1_UpperCasePropsInstance()
    {
        super(GenCrdV1_19_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        Collection<PropsContainers> propsContainers = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            GeneratedDatabaseTables.PROPS_CONTAINERS
        ).values();

        for (PropsContainers propsCon : propsContainers)
        {
            PropsContainersSpec spec = propsCon.getSpec();
            txTo.delete(GeneratedDatabaseTables.PROPS_CONTAINERS, propsCon);
            txTo.create(
                GeneratedDatabaseTables.PROPS_CONTAINERS,
                GenCrdV1_19_1.createPropsContainers(
                    // will also recalculate (changed) new k8s name
                    spec.propsInstance.toUpperCase(),
                    spec.propKey,
                    spec.propValue
                )
            );
        }
        return null;
    }
}
