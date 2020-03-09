package com.linbit.linstor.storage.layer.provider.openflex.rest.responses;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenflexVolumesCollection
{
    public String Self;
    public List<OpenflexVolume> Members;
}
