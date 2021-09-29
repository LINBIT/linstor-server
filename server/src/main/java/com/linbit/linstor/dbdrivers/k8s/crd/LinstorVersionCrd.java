package com.linbit.linstor.dbdrivers.k8s.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(LinstorVersionCrd.VERSION)
@Group(LinstorVersionCrd.GROUP)
@Plural(LinstorVersionCrd.LINSTOR_CRD_NAME)
@Singular(LinstorVersionCrd.LINSTOR_CRD_NAME)
@Kind(LinstorVersionCrd.LINSTOR_CRD_KIND)
public class LinstorVersionCrd extends CustomResource<LinstorVersionSpec, Void> implements LinstorCrd<LinstorVersionSpec>
{
    private static final long serialVersionUID = -8682837877370152832L;

    public static final String VERSION = "v1"; // will (hopefully) never change
    public static final String GROUP = "internal.linstor.linbit.com";

    public static final String LINSTOR_CRD_NAME = "linstorversion";
    public static final String LINSTOR_CRD_KIND = "LinstorVersion";

    public LinstorVersionCrd()
    {
        super();
    }

    public LinstorVersionCrd(LinstorVersionSpec spec)
    {
        setMetadata(new ObjectMetaBuilder().withName(spec.getKey()).build());
        setSpec(spec);
    }

    @JsonIgnore
    public static String getYamlLocation()
    {
        return LinstorVersionSpec.getYamlLocation();
    }

    @Override
    @JsonIgnore
    public String getKey()
    {
        return spec.getKey();
    }
}
