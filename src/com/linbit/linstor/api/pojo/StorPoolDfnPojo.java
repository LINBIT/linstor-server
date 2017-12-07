package com.linbit.linstor.api.pojo;

import com.linbit.linstor.StorPoolDefinition;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class StorPoolDfnPojo implements StorPoolDefinition.StorPoolDfnApi {

    private final UUID uuid;
    private final String name;
    private final Map<String, String> props;

    public StorPoolDfnPojo(
        final UUID uuid,
        final String name,
        final Map<String, String> props)
    {
        this.uuid = uuid;
        this.name = name;
        this.props = props;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, String> getProps() {
        return props;
    }

}
