# Java import ordering

This document defines the **canonical order of `import` statements** in the LINSTOR
sources. Its purpose is to stop IDEs from "fighting" each other: without a shared
rule every developer's IDE re-organizes imports on save, producing noisy, meaningless
diffs. The rule below is the de-facto convention already used by the vast majority of
the code base; it is now written down, made reproducible in the common IDEs, and
enforced by Checkstyle.

## The rule

Imports are split into the following **groups, in this exact order**, with a **single
blank line between groups** and **alphabetical sorting within each group**:

| # | Group                                   | Examples                                             |
|---|-----------------------------------------|------------------------------------------------------|
| 1 | `com.linbit.**` (own code)              | `com.linbit.linstor.core.objects.Resource`           |
| 2 | `jakarta.**` and `javax.**` (one group) | `jakarta.inject.Inject`, `javax.net.ssl.SSLContext`  |
| 3 | `java.**`                               | `java.util.List`                                     |
| 4 | all other (3rd-party) imports           | `com.fasterxml...`, `org.slf4j...`, `reactor...`, `edu...` |
| 5 | **all** `static` imports (single block) | `static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP`, `static org.junit.Assert.assertEquals` |

Notes:

* **Own code first, JDK in the middle, third-party last.** This is the reverse of the
  Google/Android style but is what LINSTOR has always done.
* **`jakarta`/`javax` come before `java`.** This is deliberate and non-alphabetical; treat
  them as separate groups. `jakarta.**` and `javax.**` form a *single* group (sorted
  alphabetically, so `jakarta.*` lines end up before `javax.*` lines); the Java EE APIs
  moved from `javax` to `jakarta` and only a few JDK-shipped `javax.*` packages remain.
* **Static imports form one block at the very bottom**, sorted alphabetically
  (`com.linbit` statics naturally sort first, then the rest). Do *not* interleave a
  group's static imports with its regular imports.
* **No wildcard (`.*`) imports** - neither regular nor static. This is enforced
  separately by the Checkstyle `AvoidStarImport` rule.

### Example

```java
import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.locks.LockGuardFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.net.ssl.SSLContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

import static com.linbit.linstor.api.ApiConsts.MODIFIED;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
```

## Enforcement (Checkstyle)

The rule is enforced by a `CustomImportOrder` module in
[`config/checkstyle/checkstyle.xml`](../config/checkstyle/checkstyle.xml):

```xml
<module name="CustomImportOrder">
    <property name="customImportOrderRules"
              value="SAME_PACKAGE(2)###SPECIAL_IMPORTS###STANDARD_JAVA_PACKAGE###THIRD_PARTY_PACKAGE###STATIC"/>
    <property name="specialImportsRegExp" value="^(javax|jakarta)\."/>
    <property name="standardPackageRegExp" value="^java\."/>
    <property name="separateLineBetweenGroups" value="true"/>
    <property name="sortImportsInGroupAlphabetically" value="true"/>
</module>
```

`SAME_PACKAGE(2)` maps to `com.linbit` because **every** source package lives under
`com.linbit.*`. Checkstyle runs on the *main* source sets only
(`checkstyleTest.enabled = false` in `build.gradle`), so test sources are not blocked
by this rule - but please follow it there too.

Run it locally with:

```sh
./gradlew checkstyleMain
```

## IDE configuration

Configure your IDE once so it produces exactly this order on "Organize/Optimize
Imports". This is what actually ends the churn - Checkstyle only reports, the IDE
formats.

### IntelliJ IDEA

IDEA should read .editorconfig out of the box and apply the specified rules. If that
for some reason does not work you can try to import `config/ide/idea/linstor.importorder.xml`.
Alternatively please follow the rest of this section:

`Settings/Preferences > Editor > Code Style > Java > Imports`

1. **Class count to use import with '\*'** and **Names count to use static import
   with '\*'**: set both to a high value, e.g. `999` (never collapse to `.*`).
2. In **Import Layout**, tick **"Layout static imports separately"** and set the table
   to exactly (use the *blank line* rows between groups):

   ```
   import com.linbit.**
   <blank line>
   import jakarta.**
   import javax.**
   <blank line>
   import java.**
   <blank line>
   import all other imports
   <blank line>
   import static all other imports
   ```
3. Untick **"Import java packages as a group with '\*'"** if present.

### Eclipse

`Preferences > Java > Code Style > Organize Imports`

1. Click **Import...** and choose
   [`config/ide/eclipse/linstor.importorder`](../config/ide/eclipse/linstor.importorder).
   This sets the group order to `com.linbit`, `jakarta`, `javax`, `java`, then the
   third-party bucket, with a trailing static group.
   Alternatively add the groups by hand with **New...** (for `com.linbit`, `jakarta`,
   `javax`, `java`) and **New Static...** (leave the package empty for the "all other static"
   bucket, move it to the bottom).
2. Set **"Number of imports needed for .\*"** and **"Number of static imports needed
   for .\*"** both to a high value, e.g. `999`.
3. Make sure **"Do not create imports for types starting with a lowercase letter"** is
   left at its default.

> Note: in the `.importorder` file the empty entry (`4=`) is the "all other imports"
> catch-all, so third-party imports (`org.*`, `reactor.*`, `com.fasterxml.*`, ...) land
> in position 4; the `5=\#` entry is the "all other static imports" catch-all placed
> last. Eclipse cannot merge `jakarta` and `javax` into one group and inserts a blank
> line between them; Checkstyle reports that as an import-order warning in the few files
> that import both - remove the blank line by hand there. Eclipse's static-import placement is the least flexible of the three IDEs -
> after configuring, organize imports on a sample file and confirm the static block
> ends up at the bottom.

### NetBeans

`Tools > Options > Editor > Formatting`, select **Language: Java**, **Category:
Imports**:

1. Set **"Class Count to Use FQN / Star Import"** and the static equivalent to a high
   value, e.g. `999`, so no `.*` imports are generated.
2. Under **Group Imports**, define the package groups in this order:
   `com.linbit`, `jakarta`, `javax`, `java`, `*` (everything else).
3. Enable **"Separate Static Imports"** and place the static group **last**, and enable
   sorting within groups.

> Note: older NetBeans releases have limited grouping controls. If your version cannot
> reproduce the exact order, rely on Checkstyle (`./gradlew checkstyleMain`) to catch
> deviations before committing.

## Rationale

* A written, tool-enforced rule removes the single biggest source of accidental diff
  noise: import re-ordering on save.
* The chosen order matches the overwhelming majority of the existing tree, so adopting
  it requires minimal churn.
* Consolidating static imports into one bottom block is the only static layout that all
  three IDEs *and* Checkstyle can reproduce identically - which is what lets the rule
  be actually enforced rather than merely suggested.
