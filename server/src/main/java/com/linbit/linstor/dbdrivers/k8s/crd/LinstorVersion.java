package com.linbit.linstor.dbdrivers.k8s.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(LinstorVersion.VERSION)
@Group(LinstorVersion.GROUP)
@Plural(LinstorVersion.LINSTOR_CRD_NAME)
@Singular(LinstorVersion.LINSTOR_CRD_NAME)
public class LinstorVersion extends CustomResource<LinstorVersionSpec, Void> implements LinstorCrd<LinstorVersionSpec>
{
    private static final long serialVersionUID = -8682837877370152832L;

    public static final String VERSION = "v1"; // will (hopefully) never change
    public static final String GROUP = "internal.linstor.linbit.com";

    public static final String LINSTOR_CRD_NAME = "linstorversion";

    public LinstorVersion()
    {
        super();
    }

    public LinstorVersion(LinstorVersionSpec spec)
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
