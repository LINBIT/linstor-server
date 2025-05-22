<details><summary>Commit messages</summary>

```
%{all_commits}
```
</details>

----------

* Do all methods have proper **Javadoc**?
   * Are **(flux-) exceptions** properly documented?
   * Are **special return values** properly documented? (i.e. -1)
   * Are **parameters** properly explained?
   * Does the Javadoc (still) properly **describe the functionality**?
* Are all parameters annotated with **`@com.linbit.linstor.annotation.Nullable`**?
* Do **tests (unit/E2E)** exist (if needed)?
* Does this MR require updates in **UG**?
* Does this MR require updates in **linstor-client**?
* Does this MR require updates in **linstor-api-py**?
* **CHANGELOG.md**?
* If `rest_v1_openapi.yaml` was modified:
  * Was the **changelog section** of the file also updated?
  * **Version-bump** (if first change of new release)?
  * Are new **JSON structures** (response AND requests) **extendable**? (i.e. allows for new fields for future features)
