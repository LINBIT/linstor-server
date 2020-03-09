package com.linbit.linstor.storage.layer.provider.openflex.rest.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenflexJob
{
    public static final int PERCENT_COMPLETE_FINISHED = 100;

    public String Self;
    public String ID;
    public String Affected;
    public int PercentComplete;
    public OpenflexStatus Status;

    public boolean isfinished()
    {
        return PercentComplete == PERCENT_COMPLETE_FINISHED &&
            Status.State.ID == OpenflexStatus.COMPLETED;
    }

    /*
     * "Self": "http://10.43.7.185:80/Storage/Devices/000af795789d/Jobs/VolumeOperation1579884873054546680/",
     * "ID": "VolumeOperation1579884873054546680",
     * "Affected": "http://10.43.7.185:80/Storage/Devices/000af795789d/Volumes/365902d1-11d3-4c43-b58e-045bd4ae79e3/",
     * "PercentComplete": 100,
     * "Status": {
     * "State": {
     * "ID": 8,
     * "Name": "Completed"
     * },
     * "Health": [
     * {
     * "ID": 5,
     * "Name": "OK"
     * }
     * ],
     * "Details": [
     * "Method: POST, "
     * ]
     * }
     */
}
