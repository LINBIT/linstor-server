package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.ControllerK8sCrdDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_25_1.SpaceHistory;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * No need for SQL or ETCD migrations:
 * * SQL only saved DATE instead of DATETIME (i.e. SQL cut the TIME part for us)
 * * LINSTOR's ETCD driver used a custom StringFormat that only included year, month and day, no time.
 */
@K8sCrdMigration(
    description = "Fix SpaceHistory dates",
    version = 20
)
public class Migration_20_v1_25_1_FixSpaceHistoryDates extends BaseK8sCrdMigration
{
    public Migration_20_v1_25_1_FixSpaceHistoryDates()
    {
        super(GenCrdV1_25_1.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(ControllerK8sCrdDatabase k8sDbRef) throws Exception
    {
        // Pre 1.24.0 the entryDates also stored the time, although they should not.
        // this did not cause any issue, until recently where the first entries with time are getting too old and are
        // marked for deletion.
        // An example for such an entry would be 2022-11-22T01:15:00Z. That is the date that is stored in the database.
        // However, LINSTOR sanitizes this date into 2022-11-22T00:00:00Z. But the calculated "derived K8s keys" from
        // these two objects are simply different. Since the "original" entry cannot be found, LINSTOR's
        // rollback-manager does not find the old entry and complains about it

        // that leads to an ImplementaionError, killing the SpaceTrackingService, which is also visible in a health
        // check causing k8s to continuously restart the controller.

        // now we fix that by simply cutting off the time of each date, apply the new entry and delete the old one (in
        // this order, since there should be no naming conflicts since we actively change the primary key - at least if
        // the date did not already had a correct midnight time)

        HashMap<String, GenCrdV1_25_1.SpaceHistorySpec> specMap = txFrom.getSpec(
            GenCrdV1_25_1.GeneratedDatabaseTables.SPACE_HISTORY
        );

        Calendar fromCal = Calendar.getInstance();
        Calendar toCal = Calendar.getInstance();

        toCal.set(Calendar.HOUR_OF_DAY, 0);
        toCal.set(Calendar.MINUTE, 0);
        toCal.set(Calendar.SECOND, 0);
        toCal.set(Calendar.MILLISECOND, 0);

        /*
         * We also need to track what entries we either already made or are currently creating.
         * The data we are migrating right now allows that the same day has multiple entries (with different times).
         * Cutting off the time will create the same entry, which causes an issue (i.e.
         * "cannot create the same entry multiple times" or a similar sounding error message)
         */
        HashSet<String> knownK8sKeys = new HashSet<>(specMap.keySet());

        for (GenCrdV1_25_1.SpaceHistorySpec spec : specMap.values())
        {
            Date entryDate = spec.entryDate;
            fromCal.setTime(entryDate);
            toCal.set(fromCal.get(Calendar.YEAR), fromCal.get(Calendar.MONTH), fromCal.get(Calendar.DAY_OF_MONTH));

            SpaceHistory updatedCrd = GenCrdV1_25_1.createSpaceHistory(toCal.getTime(), spec.capacity);
            String newK8sKey = updatedCrd.getK8sKey();
            if (fromCal.getTimeInMillis() != toCal.getTimeInMillis())
            {
                if (!knownK8sKeys.contains(newK8sKey))
                {
                    // to avoid duplicate entries - just to be sure
                    txTo.create(
                        GenCrdV1_25_1.GeneratedDatabaseTables.SPACE_HISTORY,
                        updatedCrd
                    );
                }
                txFrom.delete(
                    GenCrdV1_25_1.GeneratedDatabaseTables.SPACE_HISTORY,
                    GenCrdV1_25_1.specToCrd(spec)
                );
            }
            knownK8sKeys.add(newK8sKey);
        }

        return null;
    }
}
