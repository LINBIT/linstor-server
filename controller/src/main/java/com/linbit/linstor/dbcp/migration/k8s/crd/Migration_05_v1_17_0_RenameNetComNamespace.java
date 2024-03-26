package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbcp.migration.Migration_2022_03_23_RenameNetComProps;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_17_0.PropsContainersSpec;

import java.util.HashMap;

@K8sCrdMigration(
    description = "Rename netcom to NetCom namespace",
    version = 5
)
public class Migration_05_v1_17_0_RenameNetComNamespace extends BaseK8sCrdMigration
{
    public Migration_05_v1_17_0_RenameNetComNamespace()
    {
        super(GenCrdV1_17_0.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        HashMap<String, GenCrdV1_17_0.PropsContainers> propsCrd = txFrom
            .getCrd(GenCrdV1_17_0.GeneratedDatabaseTables.PROPS_CONTAINERS);

        for (PropsContainers propCrd : propsCrd.values())
        {
            PropsContainersSpec spec = propCrd.getSpec();
            if (spec.propKey.startsWith(Migration_2022_03_23_RenameNetComProps.NETCOM_NAMESPACE_OLD))
            {
                txTo.delete(GenCrdV1_17_0.GeneratedDatabaseTables.PROPS_CONTAINERS, propCrd);

                PropsContainers newPropCrd = GenCrdV1_17_0.createPropsContainers(
                    spec.propsInstance,
                    Migration_2022_03_23_RenameNetComProps.getNewKey(spec.propKey),
                    Migration_2022_03_23_RenameNetComProps.getNewValue(spec.propKey, spec.propValue)
                );
                txTo.create(GenCrdV1_17_0.GeneratedDatabaseTables.PROPS_CONTAINERS, newPropCrd);
            }
        }

        return null;
    }
}
