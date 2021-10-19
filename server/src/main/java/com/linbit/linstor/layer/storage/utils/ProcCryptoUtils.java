package com.linbit.linstor.layer.storage.utils;

import com.linbit.linstor.storage.ProcCryptoEntry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ProcCryptoUtils
{

    private static final Pattern PROC_CRYPTO_PATTERN = Pattern.compile("(\\w+)\\s+:\\s+([a-zA-Z_0-9\\-(),]+)");

    static List<ProcCryptoEntry> parseProcCryptoString(String cryptoOutput)
    {
        ArrayList<ProcCryptoEntry> entries = new ArrayList<>();

        String name = null;
        String driver = null;
        int priority = 0;
        String type = "";

        for (String line : cryptoOutput.split("\n"))
        {
            if (line.trim().isEmpty())
                continue;

            Matcher m = PROC_CRYPTO_PATTERN.matcher(line);
            if (m.matches()) {
                final String fieldName = m.group(1).toLowerCase();
                final String fieldValue = m.group(2);
                switch (fieldName)
                {
                    case "name":
                        if (name != null)
                        { // start new crypto entry
                            entries.add(new ProcCryptoEntry(
                                name,
                                driver,
                                ProcCryptoEntry.CryptoType.fromString(type),
                                priority
                            ));
                        }
                        name = fieldValue;
                        driver = null;
                        type = "";
                        priority = 0;
                        break;
                    case "driver":
                        driver = fieldValue;
                        break;
                    case "type":
                        type = fieldValue;
                        break;
                    case "priority":
                        priority = Integer.parseInt(fieldValue);
                        break;
                }
            }
        }

        // add the last entry
        if (name != null) {
            entries.add(new ProcCryptoEntry(
                name,
                driver,
                ProcCryptoEntry.CryptoType.fromString(type),
                priority
            ));
        }

        return entries;
    }

    public static List<ProcCryptoEntry> parseProcCrypto() throws IOException
    {
        final String procCryptoContent = new String(Files.readAllBytes(Paths.get("/proc/crypto")));
        return parseProcCryptoString(procCryptoContent);
    }

    public static List<ProcCryptoEntry> getHashByPriority(List<ProcCryptoEntry> entries) {
        return entries.stream()
            .filter(e -> e.getType() == ProcCryptoEntry.CryptoType.SHASH)
            .sorted((e1, e2) -> Integer.compare(e2.getPriority(), e1.getPriority()))
            .collect(Collectors.toList());
    }

    private static List<ProcCryptoEntry> commonCryptos(
            Map<String, List<ProcCryptoEntry>> nodeCryptoMap,
            ProcCryptoEntry.CryptoType type,
            List<String> allowedDrivers) {
        List<ProcCryptoEntry> commons = new ArrayList<>();
        if (nodeCryptoMap.values().stream().findFirst().isPresent()) {
            List<ProcCryptoEntry> firstNode = nodeCryptoMap.values().stream().findFirst().get();
            commons = firstNode.stream()
                .filter(pce -> pce.getType() == type && (allowedDrivers.isEmpty() || allowedDrivers.contains(pce.getDriver().toLowerCase())))
                .collect(Collectors.toList());

            for (List<ProcCryptoEntry> nodeCryptos : nodeCryptoMap.values().stream()
                .skip(1)
                .collect(Collectors.toList()))
            {
                commons.retainAll(nodeCryptos);
            }
        }
        return commons;
    }

    public static boolean cryptoDriverSupported(
        Map<String, List<ProcCryptoEntry>> nodeCryptoMap,
        ProcCryptoEntry.CryptoType type,
        String algoName
    )
    {
        return commonCryptos(nodeCryptoMap, type, Collections.emptyList())
            .stream()
            .anyMatch(pce -> pce.getName().equalsIgnoreCase(algoName) || pce.getDriver().equalsIgnoreCase(algoName));
    }

    public static @Nullable ProcCryptoEntry commonCryptoType(
            Map<String, List<ProcCryptoEntry>> nodeCryptoMap,
            ProcCryptoEntry.CryptoType type,
            List<String> allowedDrivers) {
        List<ProcCryptoEntry> commons = commonCryptos(nodeCryptoMap, type, allowedDrivers);

        return commons.stream()
            .min((e1, e2) -> Integer.compare(e2.getPriority(), e1.getPriority()))
            .orElse(null);
    }
}
