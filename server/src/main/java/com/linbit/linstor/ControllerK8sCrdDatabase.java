package com.linbit.linstor;

import io.fabric8.kubernetes.client.KubernetesClient;

public interface ControllerK8sCrdDatabase extends ControllerDatabase
{
    KubernetesClient getClient();
}
