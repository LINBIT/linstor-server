package com.linbit.linstor.storage.utils.lvm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAlias;

public class JsonLvs
{
    @JsonAlias("report")
    public List<Entry> reportList;

    static class Entry
    {
        @JsonAlias("lv")
        public List<LogicalVolume> lvList;
    }

    static class LogicalVolume
    {
        @JsonAlias("lv_name") public String name;
        @JsonAlias("lv_path") public String path;
        @JsonAlias("lv_size") public String sizeStr;
        @JsonAlias("vg_name") public String volumeGroup;
        @JsonAlias("pool_lv") public String thinPool;
        @JsonAlias("data_percent") public String dataPercentStr;
        @JsonAlias("lv_attr") public String attributes;
    }
}

