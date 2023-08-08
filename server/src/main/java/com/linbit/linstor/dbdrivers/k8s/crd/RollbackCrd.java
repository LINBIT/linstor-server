package com.linbit.linstor.dbdrivers.k8s.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(RollbackCrd.VERSION)
@Group(RollbackCrd.GROUP)
@Plural(RollbackCrd.ROLLBACK_CRD_NAME)
@Singular(RollbackCrd.ROLLBACK_CRD_NAME)
@Kind(RollbackCrd.ROLLBACK_CRD_KIND)
public class RollbackCrd extends CustomResource<RollbackSpec, Void>
{
    private static final long serialVersionUID = -7957137156435324367L;

    public static final String VERSION = "v1"; // will (hopefully) never change
    public static final String GROUP = "internal.linstor.linbit.com";
    public static final String ROLLBACK_CRD_NAME = "rollback";
    public static final String ROLLBACK_CRD_KIND = "Rollback";

    public RollbackCrd()
    {
        super();
    }

    public RollbackCrd(String name, RollbackSpec spec)
    {
        super();
        setMetadata(new ObjectMetaBuilder().withName(name).build());
        setSpec(spec);
    }

    @JsonIgnore
    public static String getYamlLocation()
    {
        return RollbackSpec.getYamlLocation();
    }
}
