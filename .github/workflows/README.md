# Release Workflow

Ce workflow GitHub Actions automatise la création de releases pour le projet FastWrappers-TSQL.

## Déclenchement

Le workflow se déclenche automatiquement lors de la création d'un nouveau tag commençant par `v` (ex: `v0.3.3`).

## Artefacts Générés

Le workflow génère 4 types d'artefacts pour différentes méthodes d'installation :

1. **FastWrappers-TSQL.dacpac** - Data-tier Application Package
   - Recommandé pour Visual Studio / SQL Server Data Tools
   - Permet un déploiement contrôlé avec détection de drift

2. **FastWrappers-TSQL.bacpac** - Binary Application Package
   - Pour l'import/export entre serveurs
   - Contient le schéma + l'assembly compilé

3. **FastWrappers-TSQL.bak** - Backup SQL Server
   - Compatible SQL Server 2016+ (Compatibility Level 130)
   - Restauration directe via SSMS ou T-SQL

4. **FastWrappers-TSQL.sql** - Script SQL pur
   - Exécutable via sqlcmd ou SSMS
   - **Généré automatiquement depuis le DACPAC avec le binaire à jour**
   - Contient l'assembly compilé en format hexadécimal inline

## Processus de Build

1. **Checkout** du code source
2. **Configuration** de MSBuild et NuGet
3. **Build** du projet SQL en mode Release
4. **Déploiement** temporaire sur SQL LocalDB
5. **Génération** des artefacts :
   - BACPAC via SqlPackage export
   - BAK via BACKUP DATABASE (avec compression)
   - DACPAC copié depuis bin/Release
   - **SQL script généré depuis le DACPAC (contient le binaire compilé à jour)**
6. **Création** de la release GitHub avec tous les artefacts

## Comment Créer une Nouvelle Release

### 1. Mettre à jour la version

Modifier [Properties/AssemblyInfo.cs](../Properties/AssemblyInfo.cs) :
```csharp
[assembly: AssemblyVersion("0.3.3.0")]
[assembly: AssemblyFileVersion("0.3.3.0")]
```

Optionnel : Mettre à jour [FastWrappers_TSQL.sqlproj](../FastWrappers_TSQL.sqlproj) :
```xml
<DacVersion>0.3.3.0</DacVersion>
```

### 2. Commit et Tag

```bash
git add Properties/AssemblyInfo.cs
git commit -m "Bump version to 0.3.3"
git tag v0.3.3
git push origin main
git push origin v0.3.3
```

### 3. Vérifier la Release

1. Aller sur https://github.com/aetperf/FastWrappers-TSQL/actions
2. Vérifier que le workflow "Create Release Artifacts" s'exécute
3. Une fois terminé, vérifier la release sur https://github.com/aetperf/FastWrappers-TSQL/releases

## Dépannage

### Le workflow échoue lors du build

- Vérifier que le projet compile localement en mode Release
- Vérifier les dépendances NuGet

### SqlPackage ne trouve pas l'assembly

- Vérifier que le DACPAC est correctement généré dans `bin/Release/`
- Vérifier que le fichier est signé (AetPCLRSign.pfx.snk)

### Le backup échoue

- SQL LocalDB peut nécessiter plus de temps pour démarrer
- Augmenter le `Start-Sleep` après le démarrage de LocalDB
