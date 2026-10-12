package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2026_02_16_RenameExtCmdWaitTo;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainersSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.SpaceHistory;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.SpaceHistorySpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.TrackingDate;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.TrackingDateSpec;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;

/**
 * Migration 32 re-keyed the SpaceHistory and TrackingDate CRs (sha256 of the RFC3339 date to sha256 of the epoch
 * milliseconds) but the deletion of the old CRs was never committed, see {@link BaseK8sCrdMigration#migrate}. The
 * SpaceTracking service only ever addresses a CR by the key derived from its (start-of-day truncated) entry date, so
 * the old CRs could never be deleted again. Once the purge had removed the correctly keyed CR of an expired day, the
 * next purge tried to delete the leftover CR via the derived key, failed with "Resource SpaceHistory/... not found" and
 * killed the SpaceTracking service, which crash-looped the controller (GitHub issue 534).
 * <p>
 * This migration removes every SpaceHistory / TrackingDate CR whose name does not match the key derived from its entry
 * date. If no CR with the expected key exists, one is created from the stale CR first, so that no history is lost. It
 * also removes the stale "ExtCmdWaitTimeout" property entries that migration 31 failed to delete for the same reason.
 */
@K8sCrdMigration(
    description = "Remove stale SpaceHistory, TrackingDate and ExtCmdWaitTimeout entries left behind by " +
        "migrations 31 and 32",
    version = 36
)
public class Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntries extends BaseK8sCrdMigration
{
    public Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntries()
    {
        super(GenCrdV1_34_0.createMigrationContext());
    }

    @Override
    public @Nullable MigrationResult migrateImpl(MigrationContext migrationCtxRef) throws Exception
    {
        // same CRD version for from and to, so all changes can go through one transaction. The loaded CRs carry their
        // actual names, which makes the deletes independent of how the keys were derived when they were created.
        removeStaleEntries(migrationCtxRef.txTo);

        return null;
    }

    static void removeStaleEntries(K8sCrdTransaction tx)
    {
        removeStaleSpaceHistoryEntries(tx);
        removeStaleTrackingDateEntries(tx);
        removeStaleExtCmdWaitTimeoutProps(tx);
    }

    private static void removeStaleSpaceHistoryEntries(K8sCrdTransaction tx)
    {
        DatabaseTable table = GenCrdV1_34_0.GeneratedDatabaseTables.SPACE_HISTORY;
        HashMap<String, SpaceHistory> crdsByName = tx.<SpaceHistory, SpaceHistorySpec>getCrd(table);

        // expected key -> CR to create under that key, since none of the existing CRs has it
        HashMap<String, SpaceHistory> replacements = new HashMap<>();
        for (SpaceHistory crd : crdsByName.values())
        {
            SpaceHistorySpec spec = crd.getSpec();
            long expectedEntryDate = startOfDayMillis(spec.entryDate);
            SpaceHistory expectedCrd = GenCrdV1_34_0.createSpaceHistory(expectedEntryDate, spec.capacity);
            String expectedKey = expectedCrd.getK8sKey();

            if (!expectedKey.equals(crd.getK8sKey()))
            {
                tx.delete(table, crd);
                if (!crdsByName.containsKey(expectedKey))
                {
                    // prefer the entry whose date already is the start of the day, that is the one the SpaceTracking
                    // service used to work with
                    if (!replacements.containsKey(expectedKey) || spec.entryDate == expectedEntryDate)
                    {
                        replacements.put(expectedKey, expectedCrd);
                    }
                }
            }
        }
        for (SpaceHistory replacement : replacements.values())
        {
            tx.create(table, replacement);
        }
    }

    private static void removeStaleTrackingDateEntries(K8sCrdTransaction tx)
    {
        DatabaseTable table = GenCrdV1_34_0.GeneratedDatabaseTables.TRACKING_DATE;
        HashMap<String, TrackingDate> crdsByName = tx.<TrackingDate, TrackingDateSpec>getCrd(table);

        HashMap<String, TrackingDate> replacements = new HashMap<>();
        for (TrackingDate crd : crdsByName.values())
        {
            TrackingDate expectedCrd = GenCrdV1_34_0.createTrackingDate(crd.getSpec().entryDate);
            String expectedKey = expectedCrd.getK8sKey();

            if (!expectedKey.equals(crd.getK8sKey()))
            {
                tx.delete(table, crd);
                if (!crdsByName.containsKey(expectedKey))
                {
                    replacements.put(expectedKey, expectedCrd);
                }
            }
        }
        for (TrackingDate replacement : replacements.values())
        {
            tx.create(table, replacement);
        }
    }

    private static void removeStaleExtCmdWaitTimeoutProps(K8sCrdTransaction tx)
    {
        DatabaseTable table = GenCrdV1_34_0.GeneratedDatabaseTables.PROPS_CONTAINERS;
        HashMap<String, PropsContainers> staleProps = tx.<PropsContainers, PropsContainersSpec>getCrd(
            table,
            crd -> Migration_2026_02_16_RenameExtCmdWaitTo.PROP_KEY_OLD_EXT_CMD_WAIT_TIMEOUT.equals(
                crd.getSpec().propKey
            )
        );
        for (PropsContainers crd : staleProps.values())
        {
            tx.delete(table, crd);
        }
    }

    /**
     * Truncates the given timestamp to the start of its day in the controller's time zone, the same way the
     * SpaceTracking service does before deriving the key of a SpaceHistory entry.
     */
    static long startOfDayMillis(long epochMillis)
    {
        ZoneId zone = ZoneId.systemDefault();
        return Instant.ofEpochMilli(epochMillis).atZone(zone).toLocalDate().atStartOfDay(zone)
            .toInstant().toEpochMilli();
    }
}
