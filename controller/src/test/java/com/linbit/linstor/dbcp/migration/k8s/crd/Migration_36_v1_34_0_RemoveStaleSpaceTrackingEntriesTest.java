package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2026_02_16_RenameExtCmdWaitTo;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainers;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.PropsContainersSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.SpaceHistory;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.SpaceHistorySpec;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.TrackingDate;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdV1_34_0.TrackingDateSpec;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.transaction.K8sCrdTransaction;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.StatusDetails;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntriesTest
{
    private static final DatabaseTable SPACE_HISTORY = GenCrdV1_34_0.GeneratedDatabaseTables.SPACE_HISTORY;
    private static final DatabaseTable TRACKING_DATE = GenCrdV1_34_0.GeneratedDatabaseTables.TRACKING_DATE;
    private static final DatabaseTable PROPS_CONTAINERS = GenCrdV1_34_0.GeneratedDatabaseTables.PROPS_CONTAINERS;
    private static final String OLD_KEY = Migration_2026_02_16_RenameExtCmdWaitTo.PROP_KEY_OLD_EXT_CMD_WAIT_TIMEOUT;
    private static final String NEW_KEY = Migration_2026_02_16_RenameExtCmdWaitTo.PROP_KEY_NEW_EXT_CMD_WAIT_TIMEOUT;
    private static final long HOUR_MS = 60 * 60 * 1000L;

    private FakeClient<SpaceHistory> spaceHistoryClient;
    private FakeClient<TrackingDate> trackingDateClient;
    private FakeClient<PropsContainers> propsClient;
    private K8sCrdTransaction tx;

    @Before
    public void setUp()
    {
        spaceHistoryClient = new FakeClient<>();
        trackingDateClient = new FakeClient<>();
        propsClient = new FakeClient<>();
        Map<DatabaseTable, Supplier<K8sResourceClient<?>>> clients = new HashMap<>();
        clients.put(SPACE_HISTORY, () -> spaceHistoryClient);
        clients.put(TRACKING_DATE, () -> trackingDateClient);
        clients.put(PROPS_CONTAINERS, () -> propsClient);
        tx = new K8sCrdTransaction(clients, null, null, GenCrdV1_34_0.VERSION);
    }

    @Test
    public void staleSpaceHistoryEntryNextToCorrectlyKeyedEntryIsDeleted()
    {
        long day = startOfDay(2026, 6, 23);
        SpaceHistory correct = GenCrdV1_34_0.createSpaceHistory(day, capacity(1));
        spaceHistoryClient.add(correct);
        spaceHistoryClient.add(renamed(GenCrdV1_34_0.createSpaceHistory(day, capacity(1)), "old-rfc3339-key"));

        removeStaleEntries();

        HashMap<String, SpaceHistory> result = spaceHistory();
        assertThat(result).containsOnlyKeys(correct.getK8sKey());
        assertThat(result.get(correct.getK8sKey()).getSpec().capacity).isEqualTo(capacity(1));
    }

    @Test
    public void staleSpaceHistoryEntryWithoutCorrectlyKeyedEntryIsRecreated()
    {
        long day = startOfDay(2026, 6, 24);
        spaceHistoryClient.add(renamed(GenCrdV1_34_0.createSpaceHistory(day, capacity(2)), "old-rfc3339-key"));
        String expectedKey = GenCrdV1_34_0.createSpaceHistory(day, capacity(2)).getK8sKey();

        removeStaleEntries();

        HashMap<String, SpaceHistory> result = spaceHistory();
        assertThat(result).containsOnlyKeys(expectedKey);
        assertThat(result.get(expectedKey).getSpec().entryDate).isEqualTo(day);
        assertThat(result.get(expectedKey).getSpec().capacity).isEqualTo(capacity(2));
    }

    @Test
    public void entryWithTimeOfDayIsReplacedByStartOfDayEntry()
    {
        long day = startOfDay(2026, 6, 25);
        // the key matches the stored (non-truncated) date, but the SpaceTracking service derives the key from the
        // start of the day
        spaceHistoryClient.add(GenCrdV1_34_0.createSpaceHistory(day + HOUR_MS + HOUR_MS / 4, capacity(3)));
        String expectedKey = GenCrdV1_34_0.createSpaceHistory(day, capacity(3)).getK8sKey();

        removeStaleEntries();

        HashMap<String, SpaceHistory> result = spaceHistory();
        assertThat(result).containsOnlyKeys(expectedKey);
        assertThat(result.get(expectedKey).getSpec().entryDate).isEqualTo(day);
        assertThat(result.get(expectedKey).getSpec().capacity).isEqualTo(capacity(3));
    }

    @Test
    public void startOfDayEntryIsPreferredWhenRecreating()
    {
        long day = startOfDay(2026, 6, 26);
        spaceHistoryClient.add(renamed(GenCrdV1_34_0.createSpaceHistory(day + HOUR_MS, capacity(4)), "stale-a"));
        spaceHistoryClient.add(renamed(GenCrdV1_34_0.createSpaceHistory(day, capacity(5)), "stale-b"));
        String expectedKey = GenCrdV1_34_0.createSpaceHistory(day, capacity(5)).getK8sKey();

        removeStaleEntries();

        HashMap<String, SpaceHistory> result = spaceHistory();
        assertThat(result).containsOnlyKeys(expectedKey);
        assertThat(result.get(expectedKey).getSpec().capacity).isEqualTo(capacity(5));
    }

    @Test
    public void correctlyKeyedEntriesAreLeftAlone()
    {
        long day = startOfDay(2026, 6, 27);
        SpaceHistory history = GenCrdV1_34_0.createSpaceHistory(day, capacity(6));
        TrackingDate trackingDate = GenCrdV1_34_0.createTrackingDate(day);
        PropsContainers prop = GenCrdV1_34_0.createPropsContainers("/CTRLCFG", NEW_KEY, "45000");
        spaceHistoryClient.add(history);
        trackingDateClient.add(trackingDate);
        propsClient.add(prop);

        removeStaleEntries();

        assertThat(spaceHistory()).containsOnlyKeys(history.getK8sKey());
        assertThat(trackingDates()).containsOnlyKeys(trackingDate.getK8sKey());
        assertThat(props()).containsOnlyKeys(prop.getK8sKey());
        assertThat(tx.hasPendingChanges()).isFalse();
    }

    @Test
    public void staleTrackingDateNextToCorrectlyKeyedEntryIsDeleted()
    {
        long day = startOfDay(2026, 6, 28);
        TrackingDate correct = GenCrdV1_34_0.createTrackingDate(day);
        trackingDateClient.add(correct);
        trackingDateClient.add(renamed(GenCrdV1_34_0.createTrackingDate(day), "old-rfc3339-key"));

        removeStaleEntries();

        assertThat(trackingDates()).containsOnlyKeys(correct.getK8sKey());
    }

    @Test
    public void staleTrackingDateWithoutCorrectlyKeyedEntryIsRecreated()
    {
        long day = startOfDay(2026, 6, 29);
        trackingDateClient.add(renamed(GenCrdV1_34_0.createTrackingDate(day), "old-rfc3339-key"));
        String expectedKey = GenCrdV1_34_0.createTrackingDate(day).getK8sKey();

        removeStaleEntries();

        HashMap<String, TrackingDate> result = trackingDates();
        assertThat(result).containsOnlyKeys(expectedKey);
        assertThat(result.get(expectedKey).getSpec().entryDate).isEqualTo(day);
    }

    @Test
    public void oldExtCmdWaitTimeoutPropsAreDeleted()
    {
        PropsContainers oldProp = GenCrdV1_34_0.createPropsContainers("/CTRLCFG", OLD_KEY, "45000");
        PropsContainers newProp = GenCrdV1_34_0.createPropsContainers("/CTRLCFG", NEW_KEY, "45000");
        PropsContainers other = GenCrdV1_34_0.createPropsContainers("/CTRLCFG", "SomeOtherKey", "value");
        propsClient.add(oldProp);
        propsClient.add(newProp);
        propsClient.add(other);

        removeStaleEntries();

        assertThat(props()).containsOnlyKeys(newProp.getK8sKey(), other.getK8sKey());
    }

    @Test
    public void startOfDayMillisTruncatesToLocalMidnight()
    {
        long day = startOfDay(2026, 10, 12);
        assertThat(Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntries.startOfDayMillis(day)).isEqualTo(day);
        assertThat(Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntries.startOfDayMillis(day + 1)).isEqualTo(day);
        assertThat(Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntries.startOfDayMillis(day + 12 * HOUR_MS))
            .isEqualTo(day);
    }

    private void removeStaleEntries()
    {
        Migration_36_v1_34_0_RemoveStaleSpaceTrackingEntries.removeStaleEntries(tx);
    }

    private HashMap<String, SpaceHistory> spaceHistory()
    {
        return tx.<SpaceHistory, SpaceHistorySpec>getCrd(SPACE_HISTORY);
    }

    private HashMap<String, TrackingDate> trackingDates()
    {
        return tx.<TrackingDate, TrackingDateSpec>getCrd(TRACKING_DATE);
    }

    private HashMap<String, PropsContainers> props()
    {
        return tx.<PropsContainers, PropsContainersSpec>getCrd(PROPS_CONTAINERS);
    }

    private static long startOfDay(int year, int month, int dayOfMonth)
    {
        return LocalDate.of(year, month, dayOfMonth).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private static byte[] capacity(int seed)
    {
        byte[] ret = new byte[32];
        ret[0] = (byte) seed;
        return ret;
    }

    /**
     * Gives the CR a different name than the one derived from its spec, like the CRs created before migration 32.
     */
    private static <CRD extends LinstorCrd<?>> CRD renamed(CRD crd, String name)
    {
        crd.setMetadata(new ObjectMetaBuilder().withName(name).build());
        return crd;
    }

    /**
     * In-memory replacement for the K8s API, keyed by the resource name like the real caching client.
     */
    private static class FakeClient<T extends HasMetadata> implements K8sResourceClient<T>
    {
        private final LinkedHashMap<String, T> items = new LinkedHashMap<>();

        void add(T item)
        {
            items.put(item.getMetadata().getName(), item);
        }

        @Override
        public List<T> list()
        {
            return new ArrayList<>(items.values());
        }

        @Override
        public T create(T item)
        {
            items.put(item.getMetadata().getName(), item);
            return item;
        }

        @Override
        public T replace(T item)
        {
            return create(item);
        }

        @Override
        public List<StatusDetails> delete()
        {
            items.clear();
            return Collections.emptyList();
        }

        @Override
        public List<StatusDetails> delete(T item)
        {
            return delete(item.getMetadata().getName());
        }

        @Override
        public List<StatusDetails> delete(String name)
        {
            items.remove(name);
            return Collections.emptyList();
        }

        @Override
        public T createOrReplace(T item)
        {
            return create(item);
        }

        @Override
        public @Nullable T get(String name)
        {
            return items.get(name);
        }
    }
}
