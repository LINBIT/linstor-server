package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_31_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_33_1;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.util.Collection;

@K8sCrdMigration(
    description = "Change SpaceTracking tables from using DATE to TIMESTAMP",
    version = 32
)
public class Migration_32_v1_33_1_ChangeSpaceTrackingFromDateToTimestamp extends BaseK8sCrdMigration
{
    public Migration_32_v1_33_1_ChangeSpaceTrackingFromDateToTimestamp()
    {
        super(
            GenCrdV1_31_1.createMigrationContext(),
            GenCrdV1_33_1.createMigrationContext()
        );
    }

    @Override
    @SuppressWarnings("JavaUtilDate") // we are trying to migrate away from that, but ErrorProne is still complaining
                                      // about us reading data from a Date.
    public MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        K8sCrdTransaction txFrom = migrationCtxRef.txFrom;
        K8sCrdTransaction txTo = migrationCtxRef.txTo;

        Collection<GenCrdV1_31_1.SpaceHistory> oldSpaceHistoryEntries = txFrom.<GenCrdV1_31_1.SpaceHistory, GenCrdV1_31_1.SpaceHistorySpec>getCrd(
            GenCrdV1_31_1.GeneratedDatabaseTables.SPACE_HISTORY
        ).values();
        Collection<GenCrdV1_31_1.TrackingDate> oldTrackingDates = txFrom.<GenCrdV1_31_1.TrackingDate, GenCrdV1_31_1.TrackingDateSpec>getCrd(
            GenCrdV1_31_1.GeneratedDatabaseTables.TRACKING_DATE
        ).values();

        updateCrdSchemaForAllTables();

        for (GenCrdV1_31_1.SpaceHistory oldSpcHistEntry : oldSpaceHistoryEntries)
        {
            GenCrdV1_31_1.SpaceHistorySpec oldSpec = oldSpcHistEntry.getSpec();
            txTo.create(
                GenCrdV1_33_1.GeneratedDatabaseTables.SPACE_HISTORY,
                GenCrdV1_33_1.createSpaceHistory(oldSpec.entryDate.getTime(), oldSpec.capacity)
            );
            txFrom.delete(GenCrdV1_31_1.GeneratedDatabaseTables.SPACE_HISTORY, oldSpcHistEntry);
        }

        for (GenCrdV1_31_1.TrackingDate oldTrackingDate : oldTrackingDates)
        {
            GenCrdV1_31_1.TrackingDateSpec oldSpec = oldTrackingDate.getSpec();
            txTo.create(
                GenCrdV1_33_1.GeneratedDatabaseTables.TRACKING_DATE,
                GenCrdV1_33_1.createTrackingDate(oldSpec.entryDate.getTime())
            );
            txFrom.delete(GenCrdV1_31_1.GeneratedDatabaseTables.TRACKING_DATE, oldTrackingDate);
        }

        return null;
    }
}
