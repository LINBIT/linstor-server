package com.linbit.linstor.layer.storage.openflex.rest.responses;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenflexVolumesCollection
{
    public String Self;
    public List<OpenflexVolume> Members;
}
