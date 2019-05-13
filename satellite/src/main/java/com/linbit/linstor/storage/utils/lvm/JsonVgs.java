package com.linbit.linstor.storage.utils.lvm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;

public class JsonVgs
{
    @JsonAlias("report")
    public List<Entry> reportList;

    static class Entry
    {
        @JsonAlias("vg")
        public List<VolumeGroup> vgList;
    }

    static class VolumeGroup
    {
        @JsonAlias("vg_name") public String name;
        @JsonAlias("vg_extent_size") public String extentSizeStr;
        @JsonAlias("vg_size") public String capacityStr;
        @JsonAlias("vg_free") public String freeStr;
        @JsonAlias("data_percent") public String dataPercentStr;

        @JsonAlias("lv_name") public String lvName;
        @JsonAlias("lv_size") public String lvSizeStr;
    }
}
