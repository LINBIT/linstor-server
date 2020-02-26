package com.linbit.linstor.testutils;

import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class DummySupportedKinds
{
    public static final int VERSION_MAJOR_HIGH_ENOUGH = 20;

    private DummySupportedKinds()
    {
    }

    public static ArrayList<ExtToolsInfo> convertSupportedExtTools(
        Collection<ExtTools> supportedExtTools
    )
    {
        ArrayList<ExtTools> remainingExtTools = new ArrayList<>(Arrays.asList(ExtTools.values()));
        ArrayList<ExtToolsInfo> ret = new ArrayList<>();

        for (ExtTools extTool : supportedExtTools)
        {
            ret.add(
                new ExtToolsInfo(extTool, true, VERSION_MAJOR_HIGH_ENOUGH, 0, 0, Collections.emptyList())
            );
            remainingExtTools.remove(extTool);
        }
        for (ExtTools extTool : remainingExtTools)
        {
            ret.add(
                new ExtToolsInfo(
                    extTool, false, 0, 0, 0,
                    Collections.singletonList("Not supported by test configuration")
                )
            );
        }
        return ret;
    }
}
