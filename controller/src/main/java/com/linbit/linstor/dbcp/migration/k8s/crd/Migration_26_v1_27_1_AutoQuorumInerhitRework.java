package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.PropsContainersSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import static com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_27_1.GeneratedDatabaseTables.PROPS_CONTAINERS;

import java.util.Collection;

@K8sCrdMigration(
    description = "Auto-quorum rework",
    version = 26
)
public class Migration_26_v1_27_1_AutoQuorumInerhitRework extends BaseK8sCrdMigration
{
    public Migration_26_v1_27_1_AutoQuorumInerhitRework()
    {
        super(GenCrdV1_27_1.createMigrationContext());
    }

    @Override
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;
        Collection<PropsContainers> crdList = txFrom.<PropsContainers, PropsContainersSpec>getCrd(
            PROPS_CONTAINERS
        ).values();

        for (PropsContainers propsCrd : crdList)
        {
            PropsContainersSpec propsSpec = propsCrd.getSpec();
            if (propsSpec.propKey.equalsIgnoreCase("DrbdOptions/auto-quorum"))
            {
                String autoQuorumValue = propsSpec.propValue;
                String propInstance = propsSpec.propsInstance;

                propInstance = propInstance.equalsIgnoreCase("/CTRL") ? "/STLT" : propInstance;

                if (autoQuorumValue.equalsIgnoreCase("disabled"))
                {
                    // if disabled set quorum -> off for the props_instance if not already set
                    txTo.create(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            "Internal/Drbd/QuorumSetBy",
                            "user"
                        )
                    );

                    txTo.upsert(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            "DrbdOptions/Resource/quorum",
                            "off"
                        )
                    );
                }
                else
                {
                    // if not disabled, set the current auto-quorum value to on-no-quorum
                    txTo.upsert(PROPS_CONTAINERS,
                        GenCrdV1_27_1.createPropsContainers(
                            propInstance,
                            "DrbdOptions/Resource/on-no-quorum",
                            autoQuorumValue
                        )
                    );
                }

                txTo.delete(PROPS_CONTAINERS, propsCrd);
            }
        }

        return null;
    }
}
