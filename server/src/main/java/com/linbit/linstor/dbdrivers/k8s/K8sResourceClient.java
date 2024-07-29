package com.linbit.linstor.dbdrivers.k8s;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;

import io.fabric8.kubernetes.api.model.StatusDetails;

/**
 * A slimmed down variant of {@link io.fabric8.kubernetes.client.dsl.MixedOperation}.
 *
 * @param <T> The Kubernetes resource type.
 */
public interface K8sResourceClient<T>
{
    /**
     * List all items in the k8s API.
     *
     * @return a list of all items.
     */
    List<T> list();

    /**
     * Create a new item in the k8s API.
     *
     * @param item The item to add.
     * @return The item with modifications made during the creation in k8s.
     */
    T create(T item);

    /**
     * Replace the item in k8s with the new version, based on the same name.
     *
     * @param item The item to replace.
     * @return The item with modifications made during replacement in k8s.
     */
    T replace(T item);

    /**
     * Delete all resource of the given type.
     *
     * @return True, if something was deleted.
     */
    List<StatusDetails> delete();

    /**
     * Delete the given item.
     *
     * @param item The item to delete.
     * @return True, if something was removed.
     */
    List<StatusDetails> delete(T item);

    /**
     * Delete an item based on its name.
     *
     * @param name The name of the item to remove.
     * @return True, if something was removed.
     */
    List<StatusDetails> delete(String name);

    /**
     * Upsert for k8s. Either replace the existing item, or create a new one.
     *
     * @param item The item to upsert.
     * @return The item with modifications made during replacement in k8s.
     */
    T createOrReplace(T item);

    /**
     * Get the item of the given name.
     *
     * @param name The name of the item to get.
     * @return The item of the given name or null, if non could be found.
     */
    @Nullable
    T get(String name);
}
