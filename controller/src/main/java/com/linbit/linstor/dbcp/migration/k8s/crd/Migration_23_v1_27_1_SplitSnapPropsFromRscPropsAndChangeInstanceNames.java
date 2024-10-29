package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainersSpec;

import static com.linbit.linstor.dbcp.migration.Migration_2024_10_24_SplitSnapPropsFromRscPropsAndChangeInstanceNames.getNewInstanceName;
import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.util.Collection;

@K8sCrdMigration(
    description = "Split snapshotted resource properties from snapshot properties and change instance names",
    version = 23
)
public class Migration_23_v1_27_1_SplitSnapPropsFromRscPropsAndChangeInstanceNames extends BaseK8sCrdMigration
{
    public Migration_23_v1_27_1_SplitSnapPropsFromRscPropsAndChangeInstanceNames()
    {
        super(GenCrdV1_27_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        Collection<PropsContainers> crdList = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            PROPS_CONTAINERS
        ).values();

        for (PropsContainers propsCrd : crdList)
        {
            PropsContainersSpec propsSpec = propsCrd.getSpec();

            String newInstanceName = getNewInstanceName(propsSpec.propsInstance, propsSpec.propKey);
            if (!propsSpec.propsInstance.equals(newInstanceName))
            {
                txTo.delete(PROPS_CONTAINERS, propsCrd);
                txTo.create(
                    PROPS_CONTAINERS,
                    GenCrdV1_27_1.createPropsContainers(
                        newInstanceName,
                        propsSpec.propKey,
                        propsSpec.propValue
                    )
                );
            }
        }
        return null;
    }
}
