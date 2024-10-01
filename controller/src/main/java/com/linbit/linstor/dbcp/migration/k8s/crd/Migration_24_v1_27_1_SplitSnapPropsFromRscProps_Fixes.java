package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainersSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import static com.linbit.linstor.dbcp.migration.Migration_2024_12_18_SplitSnapPropsFixes.getNewInstanceName;
import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.util.Collection;

@K8sCrdMigration(
    description = "Split snapshotted resource properties - Fix more properties",
    version = 24
)
public class Migration_24_v1_27_1_SplitSnapPropsFromRscProps_Fixes extends BaseK8sCrdMigration
{
    public Migration_24_v1_27_1_SplitSnapPropsFromRscProps_Fixes()
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
