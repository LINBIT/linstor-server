package com.linbit.linstor.dbdrivers.k8s.crd;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.HasMetadata;

@JsonDeserialize
public interface LinstorCrd<SPEC extends LinstorSpec> extends HasMetadata
{
    String VERSION = "v1_15_0_rc_2";
    String GROUP = "internal.linstor.linbit.com";

    String getK8sKey();
    String getLinstorKey();

    SPEC getSpec();
}
