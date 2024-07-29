package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers.ExosRestPort;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestPorts extends ExosRestBaseResponse
{
    @JsonProperty("port")
    public @Nullable ExosRestPort[] port;
}
