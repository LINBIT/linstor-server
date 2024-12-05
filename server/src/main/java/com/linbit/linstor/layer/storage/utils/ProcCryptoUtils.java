package com.linbit.linstor.layer.storage.utils;

import com.linbit.Platform;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.ProcCryptoEntry;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ProcCryptoUtils
{

    private static final Pattern PROC_CRYPTO_PATTERN = Pattern.compile("(\\w+)\\s+:\\s+([a-zA-Z_0-9\\-(),]+)");

    static List<ProcCryptoEntry> parseProcCryptoString(ErrorReporter errorReporterRef, String cryptoOutput)
    {
        ArrayList<ProcCryptoEntry> entries = new ArrayList<>();

        String name = null;
        String driver = null;
        int priority = 0;
        String type = "";

        for (String line : cryptoOutput.split("\n"))
        {
            if (!line.trim().isEmpty())
            {
                Matcher mtc = PROC_CRYPTO_PATTERN.matcher(line);
                if (mtc.matches())
                {
                    final String fieldName = mtc.group(1).toLowerCase();
                    final String fieldValue = mtc.group(2);
                    switch (fieldName)
                    {
                        case "name":
                            if (name != null)
                            {
                                if (driver == null)
                                {
                                    errorReporterRef.logWarning(
                                        "Ignoring /proc/crypto entry since it is missing a driver: %s",
                                        name
                                    );
                                }
                                else if (type.isEmpty())
                                {
                                    errorReporterRef.logWarning(
                                        "Ignoring /proc/crypto entry since it is missing a type: %s",
                                        name
                                    );
                                }
                                else
                                {
                                    // start new crypto entry
                                    entries.add(
                                        new ProcCryptoEntry(
                                            name,
                                            driver,
                                            ProcCryptoEntry.CryptoType.fromString(type),
                                            priority
                                        )
                                    );
                                }
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
                        default:
                            // Unknown field, no-op
                            break;
                    }
                }
            }
        }

        // add the last entry
        if (name != null)
        {
            entries.add(
                new ProcCryptoEntry(
                    name,
                    driver,
                    ProcCryptoEntry.CryptoType.fromString(type),
                    priority
                )
            );
        }

        return entries;
    }

    public static List<ProcCryptoEntry> parseProcCrypto(ErrorReporter errorReporterRef) throws IOException
    {
        List<ProcCryptoEntry> cryptoEntries = new ArrayList<>();

        if (Platform.isLinux())
        {
            final String procCryptoContent = new String(Files.readAllBytes(Paths.get("/proc/crypto")));
            cryptoEntries = parseProcCryptoString(errorReporterRef, procCryptoContent);
        }
        if (Platform.isWindows())
        {
            /* driver name must be lower case. */
            cryptoEntries.add(new ProcCryptoEntry("crc32c", "windrbd", ProcCryptoEntry.CryptoType.SHASH, 200));
        }
        return cryptoEntries;
    }

    public static List<ProcCryptoEntry> getHashByPriority(List<ProcCryptoEntry> entries) {
        return entries.stream()
            .filter(e -> e.getType() == ProcCryptoEntry.CryptoType.SHASH)
            .sorted((e1, e2) -> Integer.compare(e2.getPriority(), e1.getPriority()))
            .collect(Collectors.toList());
    }

    @SuppressWarnings("checkstyle:DescendantToken")
    private static boolean hasCryptoName(List<ProcCryptoEntry> cryptos, String name)
    {
        for (var pce : cryptos)
        {
            if (pce.getName().equalsIgnoreCase(name))
            {
                return true;
            }
        }
        return false;
    }

    private static List<ProcCryptoEntry> commonCryptos(
        Map<String, List<ProcCryptoEntry>> nodeCryptoMap,
        ProcCryptoEntry.CryptoType type,
        List<String> allowedDrivers
    )
    {
        List<ProcCryptoEntry> commons = new ArrayList<>();
        Optional<List<ProcCryptoEntry>> optFirstEntry = nodeCryptoMap.values().stream().findFirst();
        if (optFirstEntry.isPresent())
        {
            List<ProcCryptoEntry> firstNode = optFirstEntry.get();
            commons = firstNode.stream()
                .filter(pce -> pce.getType() == type && (allowedDrivers.isEmpty()
                    || allowedDrivers.contains(pce.getDriver().toLowerCase())
                    || allowedDrivers.contains(pce.getName().toLowerCase())))
                .collect(Collectors.toList());

            for (List<ProcCryptoEntry> nodeCryptos : nodeCryptoMap.values().stream()
                .skip(1)
                .collect(Collectors.toList()))
            {
                commons.removeIf(pce -> !hasCryptoName(nodeCryptos, pce.getName()));
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
        List<String> allowedDrivers
    )
    {
        List<ProcCryptoEntry> commons = commonCryptos(nodeCryptoMap, type, allowedDrivers);

        return commons.stream()
            .min((e1, e2) -> Integer.compare(e2.getPriority(), e1.getPriority()))
            .orElse(null);
    }
}
