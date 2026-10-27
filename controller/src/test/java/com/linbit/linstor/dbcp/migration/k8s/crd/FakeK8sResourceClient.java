package com.linbit.linstor.dbcp.migration.k8s.crd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.StatusDetails;

/**
 * In-memory replacement for the K8s API, keyed by the resource name like the real caching client.
 */
class FakeK8sResourceClient<T extends HasMetadata> implements K8sResourceClient<T>
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
