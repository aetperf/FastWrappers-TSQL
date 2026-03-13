# Release Workflow

This GitHub Actions workflow automates the creation of releases for the FastWrappers-TSQL project.

## Trigger

The workflow is automatically triggered when creating a new tag starting with `v` (e.g., `v0.3.3`).

## Generated Artifacts

The workflow generates 4 types of artifacts for different installation methods:

1. **FastWrappers-TSQL.dacpac** - Data-tier Application Package
   - Recommended for Visual Studio / SQL Server Data Tools
   - Enables controlled deployment with drift detection

2. **FastWrappers-TSQL.bacpac** - Binary Application Package
   - For import/export between servers
   - Contains the schema + compiled assembly

3. **FastWrappers-TSQL.bak** - SQL Server Backup
   - Compatible with SQL Server 2016+ (Compatibility Level 130)
   - Direct restore via SSMS or T-SQL

4. **FastWrappers-TSQL.sql** - Pure SQL Script
   - Executable via sqlcmd or SSMS
   - **Automatically generated from DACPAC with up-to-date binary**
   - Contains the compiled assembly in inline hexadecimal format

## Build Process

1. **Checkout** source code
2. **Setup** MSBuild and NuGet
3. **Build** SQL project in Release mode
4. **Deploy** temporarily to SQL LocalDB
5. **Generate** artifacts:
   - BACPAC via SqlPackage export
   - BAK via BACKUP DATABASE (with compression)
   - DACPAC copied from bin/Release
   - **SQL script generated from DACPAC (contains up-to-date compiled binary)**
6. **Create** GitHub release with all artifacts

## How to Create a New Release

### 1. Update the Version

Edit [Properties/AssemblyInfo.cs](../Properties/AssemblyInfo.cs):
```csharp
[assembly: AssemblyVersion("0.3.3.0")]
[assembly: AssemblyFileVersion("0.3.3.0")]
```

Optional: Update [FastWrappers_TSQL.sqlproj](../FastWrappers_TSQL.sqlproj):
```xml
<DacVersion>0.3.3.0</DacVersion>
```

### 2. Commit and Tag

```bash
git add Properties/AssemblyInfo.cs
git commit -m "Bump version to 0.3.3"
git tag v0.3.3
git push origin main
git push origin v0.3.3
```

### 3. Verify the Release

1. Go to https://github.com/aetperf/FastWrappers-TSQL/actions
2. Check that the "Create Release Artifacts" workflow is running
3. Once completed, verify the release at https://github.com/aetperf/FastWrappers-TSQL/releases

## Troubleshooting

### Workflow fails during build

- Verify that the project compiles locally in Release mode
- Check NuGet dependencies

### SqlPackage cannot find the assembly

- Verify that the DACPAC is correctly generated in `bin/Release/`
- Verify that the file is signed (AetPCLRSign.pfx.snk)

### Backup fails

- SQL LocalDB may need more time to start
- Increase the `Start-Sleep` after LocalDB startup
