# Build and Release Workflow

[release.yml](release.yml) builds and validates the installation packages of FastWrappers-TSQL, and publishes them for each version.

## Triggers

| Event | Packages validated | Workflow artifact | GitHub release |
|---|---|---|---|
| Pull request to `master` | Yes | Yes | No |
| Push to `master` | Yes | Yes | No |
| Tag `vX.Y.Z` or `X.Y.Z` | Yes | Yes | Yes |

## Packages

| File | Description | SQL Server |
|---|---|---|
| **FastWrappers-TSQL.sql** | Self-contained script with the assembly in hexadecimal. Installs or upgrades, and can be replayed | 2017+ |
| **FastWrappers-TSQL.bak** | Backup of the database, created with LocalDB 2019 | 2019+ |
| **FastWrappers-TSQL.dacpac** | Data-tier package, to publish with SqlPackage or Visual Studio | 2017+ |

## What is checked

The logic is in [Build-Packages.ps1](../scripts/Build-Packages.ps1). Any failed check stops the workflow, so no release is published with a missing or broken package:

- the versions are aligned between the assembly, the project, the scripts, the README and the tag
- the `.dacpac` deploys, and the deployed assembly is the built one
- the `.bak` has a database version compatible with SQL Server 2019 (904)
- the `.sql` upgrades a `.bak` / `.dacpac` installation, can be replayed (members of the roles and grants are kept), and installs on a server without the database
- the `.bak` can be restored, with the logical file names given in the README
- the CLR function `dbo.EncryptString` runs after each of these installations

FastTransfer and FastBCP are not available on the runner: the procedures are created and the CLR assembly is loaded, but no transfer or export is run.

## How to Create a New Release

### 1. Update the Version

Edit `WrapperVersions` in [Properties/AssemblyInfo.cs](../../Properties/AssemblyInfo.cs). It sets the assembly version and the versions printed by the wrappers with `@debug = 1`:
```csharp
public const string Wrapper = "x.y.z.0";
public const string FastTransfer = "<validated FastTransfer version>";
public const string FastBCP = "<validated FastBCP version>";
```

Then align the same version in:
- [FastWrappers_TSQL.sqlproj](../../FastWrappers_TSQL.sqlproj): `<DacVersion>x.y.z.0</DacVersion>`
- [FastWrappers-TSQL.sql](../../FastWrappers-TSQL.sql): script header and description of the trusted assembly
- [xp_RunFastTransfer_secure.sql](../../xp_RunFastTransfer_secure.sql) and [xp_RunFastBCP_secure.sql](../../xp_RunFastBCP_secure.sql): header comments
- [samples/samples.sql](../../samples/samples.sql): header comment
- [README.md](../../README.md): compatibility table and description of the trusted assembly

### 2. Merge, then Tag

```bash
git tag vx.y.z
git push origin vx.y.z
```

### 3. Verify the Release

1. Go to the **Actions** tab and check the "Build and Release Packages" workflow
2. Once completed, the release with the 3 packages is in the **Releases** page

## Running it Locally

On a dedicated SQL Server instance (the script stops if the database exists, and it enables CLR):

```powershell
sqllocaldb create FWPackages -s
msbuild FastWrappers_TSQL.sqlproj /p:Configuration=Release "/p:Platform=Any CPU" /t:Rebuild
powershell -File .github\scripts\Build-Packages.ps1 -ServerInstance "(localdb)\FWPackages" -SqlPackage <path to SqlPackage.exe> -OutputDir <output directory> -MaxDatabaseVersion 0
sqllocaldb stop FWPackages
sqllocaldb delete FWPackages
```

`-MaxDatabaseVersion 0` disables the SQL Server 2019 check, for a local instance of another version.

## Troubleshooting

- **Versions are not aligned**: the message names the file to update (see step 1).
- **The tag does not match the version**: delete the tag, fix the version or the tag, and push it again.
- **The `.bak` is not compatible with SQL Server 2019**: the LocalDB instance is not a 2019 one, check the "Setup SQL Server 2019 (LocalDB)" step.
