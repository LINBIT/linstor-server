package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.k8s.K8sResourceClient;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;

import io.fabric8.kubernetes.client.KubernetesClient;

public interface ControllerK8sCrdDatabase extends ControllerDatabase
{
    KubernetesClient getClient();

    K8sResourceClient<?> getCachingClient(Class<? extends LinstorCrd<? extends LinstorSpec<?, ?>>> clazz);

    int getMaxRollbackEntries();

    void clearCache();
}
