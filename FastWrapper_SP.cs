
namespace FastWrapper
{

	using System;
	using System.Data.SqlTypes;
	using System.Diagnostics;
	using System.IO;
	using System.Security.Permissions;
	using System.Security.Cryptography;
	using Microsoft.SqlServer.Server;

	public static class FastTransferCLR
	{

		// --------------------------------------------------------------------
		// 1) Clé et IV statiques pour la démonstration
		// --------------------------------------------------------------------
		// Idéalement : stocker la clé ailleurs (DPAPI, config sécurisée, etc.)
		private static readonly byte[] AesKey = {
            // 32 octets pour AES-256
            0x01, 0x33, 0x58, 0xA7, 0x3B, 0x99, 0x2D, 0xFA,
			0x62, 0x11, 0xD5, 0xE7, 0x8F, 0x2C, 0x99, 0x0A,
			0xF2, 0x68, 0x44, 0xFA, 0x48, 0x92, 0xBE, 0x65,
			0x10, 0x7A, 0xCA, 0xAC, 0x9E, 0xDE, 0x7F, 0x7F
		};

		private static readonly byte[] AesIV = {
            // 16 octets pour un block AES
            0x11, 0x22, 0xAA, 0x77, 0x55, 0x99, 0x10, 0x01,
			0x66, 0x33, 0x45, 0x0F, 0x3A, 0x2B, 0xCC, 0xEE
		};

		// --------------------------------------------------------------------
		// 2) Méthode de chiffrement
		// --------------------------------------------------------------------
		private static string AesEncrypt(string plainText)
		{
			if (plainText == null) return null;
			using (Aes aes = Aes.Create())
			{
				aes.Key = AesKey;
				aes.IV = AesIV;
				aes.Mode = CipherMode.CBC;
				aes.Padding = PaddingMode.PKCS7;

				using (MemoryStream ms = new MemoryStream())
				using (ICryptoTransform encryptor = aes.CreateEncryptor())
				using (CryptoStream cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
				{
					using (StreamWriter sw = new StreamWriter(cs))
					{
						sw.Write(plainText);
					}
					byte[] cipherBytes = ms.ToArray();
					// on retourne un Base64
					return Convert.ToBase64String(cipherBytes);
				}
			}
		}

		// --------------------------------------------------------------------
		// 3) Méthode de déchiffrement
		// --------------------------------------------------------------------
		private static string AesDecrypt(string base64Cipher)
		{
			if (string.IsNullOrEmpty(base64Cipher)) return null;
			byte[] cipherBytes = Convert.FromBase64String(base64Cipher);
			using (Aes aes = Aes.Create())
			{
				aes.Key = AesKey;
				aes.IV = AesIV;
				aes.Mode = CipherMode.CBC;
				aes.Padding = PaddingMode.PKCS7;

				using (MemoryStream ms = new MemoryStream(cipherBytes))
				using (ICryptoTransform decryptor = aes.CreateDecryptor())
				using (CryptoStream cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
				using (StreamReader sr = new StreamReader(cs))
				{
					return sr.ReadToEnd();
				}
			}
		}

		// --------------------------------------------------------------------
		// 4) Fonction CLR : EncryptString
		// --------------------------------------------------------------------
		// Permet de renvoyer la version chiffrée (Base64) d’une chaîne en clair
		// à partir de T-SQL.
		// --------------------------------------------------------------------
		[SqlFunction]
		public static SqlString EncryptString(SqlString plainText)
		{
			if (plainText.IsNull) return SqlString.Null;
			string encrypted = AesEncrypt(plainText.Value);
			return new SqlString(encrypted);
		}


		[SqlProcedure]	
		public static void RunFastTransfer(
		SqlString fastTransferDir,
		SqlString sourceConnectionType,
		SqlString sourceConnectString,
		SqlString sourceServer,
		SqlString sourceDSN,
		SqlString sourceProvider,
		SqlBoolean isSourceTrusted,
		SqlString sourceUser,
		SqlString sourcePassword,
		SqlString sourceDatabase,
		SqlString fileInput,
		SqlString query,
		SqlString sourceSchema,
		SqlString sourceTable,
		SqlString targetConnectionType,
		SqlString targetConnectString,
		SqlString targetServer,
		SqlBoolean isTargetTrusted,
		SqlString targetUser,
		SqlString targetPassword,
		SqlString targetDatabase,
		SqlString targetSchema,
		SqlString targetTable,
		SqlString loadMode,
		SqlInt32 batchSize,
		SqlString method,
		SqlString distributeKeyColumn,
		SqlInt32 degree,
		SqlString mapMethod,
		SqlString runId,
		SqlString settingsFile
		)
		{

			RunFastTransferInternal(
			fastTransferDir,
			sourceConnectionType,
			sourceConnectString,
			sourceServer,
			sourceDSN,
			sourceProvider,
			isSourceTrusted,
			sourceUser,
			sourcePassword,
			sourceDatabase,
			fileInput,
			query,
			sourceSchema,
			sourceTable,
			targetConnectionType,
			targetConnectString,
			targetServer,
			isTargetTrusted,
			targetUser,
			targetPassword,
			targetDatabase,
			targetSchema,
			targetTable,
			loadMode,
			batchSize,
			method,
			distributeKeyColumn,
			degree,
			mapMethod,
			runId,
			settingsFile
			);
		}


		// --------------------------------------------------------------------
		// RunFastTransfer_Secure :
		// Toutes les valeurs sensibles (password, connectString) sont
		// toujours passées en version chiffrée (Base64).
		// On déchiffre avant la logique de construction + exécution du binaire.
		// --------------------------------------------------------------------
		[SqlProcedure]
		public static void RunFastTransfer_Secure(
			SqlString fastTransferDir,

			// 2) Source Connection 
			SqlString sourceConnectionType,
			SqlString sourceConnectStringSecure,   // chiffré
			SqlString sourceServer,
			SqlString sourceDSN,
			SqlString sourceProvider,
			SqlBoolean isSourceTrusted,
			SqlString sourceUser,
			SqlString sourcePasswordSecure,        // chiffré
			SqlString sourceDatabase,

			// 3) Source Infos
			SqlString fileInput,
			SqlString query,
			SqlString sourceSchema,
			SqlString sourceTable,

			// 4) Target Connection 
			SqlString targetConnectionType,
			SqlString targetConnectStringSecure,   // chiffré
			SqlString targetServer,
			SqlBoolean isTargetTrusted,
			SqlString targetUser,
			SqlString targetPasswordSecure,        // chiffré
			SqlString targetDatabase,

			// 5) Target Infos
			SqlString targetSchema,
			SqlString targetTable,
			SqlString loadMode,
			SqlInt32 batchSize,

			// 6) Advanced
			SqlString method,
			SqlString distributeKeyColumn,
			SqlInt32 degree,
			SqlString mapMethod,

			// 7) Logging
			SqlString runId,
			SqlString settingsFile
		)
		{
			

			// Déchiffrer systématiquement les valeurs sensibles s'il y en a 
			string sourceConnectString = null;
			if (!string.IsNullOrEmpty((string)sourceConnectStringSecure))
			{
				sourceConnectString = AesDecrypt((string)sourceConnectStringSecure);
			}

			string sourcePassword = null;
			if (!string.IsNullOrEmpty((string)sourcePasswordSecure))
			{
				sourcePassword = AesDecrypt((string)sourcePasswordSecure);
			}

			string targetConnectString = null;
			if (!string.IsNullOrEmpty((string)targetConnectStringSecure))
			{
				targetConnectString = AesDecrypt((string)targetConnectStringSecure);
			}

			string targetPassword = null;
			if (!string.IsNullOrEmpty((string)targetPasswordSecure))
			{
				targetPassword = AesDecrypt((string)targetPasswordSecure);
			}


			RunFastTransferInternal(
			fastTransferDir,
			sourceConnectionType,
			sourceConnectString,
			sourceServer,
			sourceDSN,
			sourceProvider,
			isSourceTrusted,
			sourceUser,
			sourcePassword,
			sourceDatabase,
			fileInput,
			query,
			sourceSchema,
			sourceTable,
			targetConnectionType,
			targetConnectString,
			targetServer,
			isTargetTrusted,
			targetUser,
			targetPassword,
			targetDatabase,
			targetSchema,
			targetTable,
			loadMode,
			batchSize,
			method,
			distributeKeyColumn,
			degree,
			mapMethod,
			runId,
			settingsFile
			);
		}

		private static void RunFastTransferInternal(
		SqlString fastTransferDir,		// 1) FastTransfer Binary Directory/Path e.g. ".\" or "C:\\tools\\MyFastTransfer" or "./fasttransfer"
		SqlString sourceConnectionType, // must be one of [clickhouse, duckdb, hana, mssql, mysql, nzsql, odbc, oledb, oraodp, pgcopy, pgsql, teradata]
		SqlString sourceConnectString,	// if provided, we skip server/user/password/etc.
		SqlString sourceServer,			// e.g. "Host", "Host:Port", "Host\\Instance", "Host:Port/TNSService"
		SqlString sourceDSN,            // optional DSN name, if using ODBC
		SqlString sourceProvider,       // optional, for OLEDB only
		SqlBoolean isSourceTrusted,
		SqlString sourceUser,
		SqlString sourcePassword,
		SqlString sourceDatabase,
		SqlString fileInput,            // e.g. "C:\\path\\to\\file.sql"
		SqlString query,                // e.g. "SELECT * FROM table"
		SqlString sourceSchema,         // e.g. "dbo" . Required for some parallel methods (RowId, Ctid, Ntile, RangeId)
		SqlString sourceTable,          // e.g. "MyTable" . Required for some parallel methods (RowId, Ctid, Ntile, RangeId)
		SqlString targetConnectionType, // must be one of [clickhousebulk, duckdb, hanabulk, msbulk, mysqlbulk, nzbulk, orabulk, oradirect, pgcopy, teradata]
		SqlString targetConnectString,
		SqlString targetServer,
		SqlBoolean isTargetTrusted,
		SqlString targetUser,
		SqlString targetPassword,
		SqlString targetDatabase,
		SqlString targetSchema,         // Mandatory. eg "public"
		SqlString targetTable,          // Mandatory. eg "CopyTable"
		SqlString loadMode,             // "Append" or "Truncate"
		SqlInt32 batchSize,             // e.g. 130000
		SqlString method,               // distribution method for parallelism :"None", "Random", "DataDriven", "RangeId", "Ntile", "Ctid", "Rowid"
		SqlString distributeKeyColumn,  // required if method in ["Random","DataDriven","RangeId","Ntile"]
		SqlInt32 degree,                // concurrency degree if method != "None" useless if method = "None" should be != 1, can be less than 0 for dynamic degree (based on cpucount on the platform where FastTransfer is running. -2 = CpuCount/2)
		SqlString mapMethod,            // "Position"(default) or "Name" (Automatic mapping of columns based on names (case insensitive) with tolerance on the order of columns. Non present columns in source or target are ignored. Name may mot be available for all target types (see doc))
		SqlString runId,                // a run identifier for logging (can be a string for grouping or a unique identifier). Guid is used if not provide
		SqlString settingsFile			// path for a custom FastTransfer_Settings.json file, for custom logging
		)
		
		{
			// --------------------------------------------------------------------
			// Convert SqlTypes to .NET types
			// --------------------------------------------------------------------
			string exeDir = fastTransferDir.IsNull ? null : fastTransferDir.Value.Trim();
			string srcConnType = sourceConnectionType.IsNull ? null : sourceConnectionType.Value.Trim().ToLowerInvariant();
			string srcConnStr = sourceConnectString.IsNull ? null : sourceConnectString.Value.Trim();

			string srcServerVal = sourceServer.IsNull ? null : sourceServer.Value.Trim();
			string srcDsn = sourceDSN.IsNull ? null : sourceDSN.Value.Trim();
			string srcProvider = sourceProvider.IsNull ? null : sourceProvider.Value.Trim();
			bool srcTrusted = !isSourceTrusted.IsNull && isSourceTrusted.Value;
			string srcUserVal = sourceUser.IsNull ? null : sourceUser.Value.Trim();
			string srcPasswordVal = sourcePassword.IsNull ? null : sourcePassword.Value.Trim();
			string srcDatabaseVal = sourceDatabase.IsNull ? null : sourceDatabase.Value.Trim();

			string fileInputVal = fileInput.IsNull ? null : fileInput.Value.Trim();
			string queryVal = query.IsNull ? null : query.Value.Trim();
			string srcSchemaVal = sourceSchema.IsNull ? null : sourceSchema.Value.Trim();
			string srcTableVal = sourceTable.IsNull ? null : sourceTable.Value.Trim();

			string tgtConnType = targetConnectionType.IsNull ? null : targetConnectionType.Value.Trim().ToLowerInvariant();
			string tgtConnStr = targetConnectString.IsNull ? null : targetConnectString.Value.Trim();

			string tgtServerVal = targetServer.IsNull ? null : targetServer.Value.Trim();
			bool tgtTrusted = !isTargetTrusted.IsNull && isTargetTrusted.Value;
			string tgtUserVal = targetUser.IsNull ? null : targetUser.Value.Trim();
			string tgtPasswordVal = targetPassword.IsNull ? null : targetPassword.Value.Trim();
			string tgtDatabaseVal = targetDatabase.IsNull ? null : targetDatabase.Value.Trim();

			string tgtSchemaVal = targetSchema.IsNull ? null : targetSchema.Value.Trim();
			string tgtTableVal = targetTable.IsNull ? null : targetTable.Value.Trim();
			string loadModeVal = loadMode.IsNull ? null : loadMode.Value.Trim();
			int? batchSizeVal = batchSize.IsNull ? (int?)null : batchSize.Value;

			string methodVal = method.IsNull ? null : method.Value.Trim();
			string distKeyColVal = distributeKeyColumn.IsNull ? null : distributeKeyColumn.Value.Trim();
			int? degreeVal = degree.IsNull ? (int?)null : degree.Value;
			string mapMethodVal = mapMethod.IsNull ? null : mapMethod.Value.Trim();

			string runIdVal = runId.IsNull ? null : runId.Value.Trim();
			string settingsFileVal = settingsFile.IsNull ? null : settingsFile.Value.Trim();

			// --------------------------------------------------------------------
			// 1. Parameter Validation
			// --------------------------------------------------------------------

			if (string.IsNullOrWhiteSpace(exeDir))
			{
				throw new ArgumentException("fastTransferDir must be provided (directory containing the FastTransfer executable).");
			}

			// Validate SourceConnectionType
			string[] validSourceTypes = {
			"clickhouse", "duckdb", "hana", "mssql", "mysql",
			"nzsql", "odbc", "oledb", "oraodp", "pgcopy", "pgsql", "teradata"
		};
			if (Array.IndexOf(validSourceTypes, srcConnType) < 0)
			{
				throw new ArgumentException($"Invalid SourceConnectionType: '{srcConnType}'. Possible value are {string.Join(", ", validSourceTypes)}.");
			}

			// Validate TargetConnectionType
			string[] validTargetTypes = {
			"clickhousebulk", "duckdb", "hanabulk", "msbulk", "mysqlbulk",
			"nzbulk", "orabulk", "oradirect", "pgcopy", "teradata"
		};
			if (Array.IndexOf(validTargetTypes, tgtConnType) < 0)
			{
				throw new ArgumentException($"Invalid TargetConnectionType: '{tgtConnType}'. Possible value are {string.Join(", ", validTargetTypes)}.");
			}

			// Source: either use connect string or explicit params
			bool hasSrcConnString = !string.IsNullOrEmpty(srcConnStr);
			if (hasSrcConnString)
			{
				// If using connect string, no server/DSN/provider/trusted user/pwd/database
				if (!string.IsNullOrEmpty(srcServerVal) ||
					!string.IsNullOrEmpty(srcDatabaseVal) ||
					!string.IsNullOrEmpty(srcUserVal) ||
					!string.IsNullOrEmpty(srcPasswordVal) ||
					!string.IsNullOrEmpty(srcDsn))
				{
					throw new ArgumentException("When sourceConnectString is provided, do not supply server/DSN/credentials/database.");
				}
			}
			else
			{
				// Not using connect string: must provide server/DSN and database
				if (string.IsNullOrEmpty(srcServerVal) && string.IsNullOrEmpty(srcDsn))
				{
					throw new ArgumentException("Must provide either sourceServer or sourceDSN if sourceConnectString is NULL.");
				}
				if (string.IsNullOrEmpty(srcDatabaseVal))
				{
					throw new ArgumentException("Must provide sourceDatabase when not using sourceConnectString.");
				}
				if (!srcTrusted)
				{
					if (string.IsNullOrEmpty(srcUserVal) || string.IsNullOrEmpty(srcPasswordVal))
					{
						throw new ArgumentException("Must provide sourceUser and sourcePassword when sourceTrusted = 0.");
					}
				}
			}

			// SourceInfos: at least one of fileInput, query, or (schema+table)
			int sourceChoiceCount = 0;
			if (!string.IsNullOrEmpty(fileInputVal)) sourceChoiceCount++;
			if (!string.IsNullOrEmpty(queryVal)) sourceChoiceCount++;
			if (!string.IsNullOrEmpty(srcSchemaVal) && !string.IsNullOrEmpty(srcTableVal)) sourceChoiceCount++;
			if (sourceChoiceCount == 0)
			{
				throw new ArgumentException("You must supply at least one of fileInput, query, or (sourceSchema AND sourceTable).");
			}
			else
			{
				// SourceInfos: exactly one of fileInput or query (if provided) (table +schema is optional)
				int sourceChoiceCountfileandquery = 0;
				if (!string.IsNullOrEmpty(fileInputVal)) sourceChoiceCount++;
				if (!string.IsNullOrEmpty(queryVal)) sourceChoiceCount++;
				if (sourceChoiceCountfileandquery > 1)
				{
					throw new ArgumentException("You must supply only one of fileInput or query");
				}
			}






				// Target: either use connect string or explicit params
				bool hasTgtConnString = !string.IsNullOrEmpty(tgtConnStr);
			if (hasTgtConnString)
			{
				if (!string.IsNullOrEmpty(tgtServerVal) ||
					!string.IsNullOrEmpty(tgtDatabaseVal) ||
					!string.IsNullOrEmpty(tgtUserVal) ||
					!string.IsNullOrEmpty(tgtPasswordVal))
				{
					throw new ArgumentException("When targetConnectString is provided, do not supply server/credentials/database.");
				}
			}
			else
			{
				if (string.IsNullOrEmpty(tgtServerVal))
				{
					throw new ArgumentException("Must provide targetServer if targetConnectString is NULL.");
				}
				if (string.IsNullOrEmpty(tgtDatabaseVal))
				{
					throw new ArgumentException("Must provide targetDatabase if targetConnectString is NULL.");
				}
				if (!tgtTrusted)
				{
					if (string.IsNullOrEmpty(tgtUserVal) || string.IsNullOrEmpty(tgtPasswordVal))
					{
						throw new ArgumentException("Must provide targetUser and targetPassword when targetTrusted = 0.");
					}
				}
			}

			if (string.IsNullOrEmpty(tgtSchemaVal) || string.IsNullOrEmpty(tgtTableVal))
			{
				throw new ArgumentException("Must provide targetSchema and targetTable.");
			}

			// Advanced Parameters
			bool hasMethod = !string.IsNullOrEmpty(methodVal);
			if (hasMethod)
			{
				string[] validMethods = { "None", "Random", "DataDriven", "RangeId", "Ntile", "Ctid", "Rowid" };
				if (Array.IndexOf(validMethods, methodVal) < 0)
				{
					throw new ArgumentException($"Invalid method: '{methodVal}'.");
				}

				if (!methodVal.Equals("None"))
				{
					// If method in [Random, DataDriven, RangeId, Ntile], must have distributeKeyColumn
					if (methodVal.Equals("Random") ||
						methodVal.Equals("DataDriven") ||
						methodVal.Equals("RangeId") ||
						methodVal.Equals("Ntile"))
					{
						if (string.IsNullOrEmpty(distKeyColVal))
						{
							throw new ArgumentException($"When method is '{methodVal}', you must provide distributeKeyColumn.");
						}
					}

					if (!degreeVal.HasValue)
					{
						throw new ArgumentException("When specifying a method other than None, you must provide degree.");
					}
				}
			}

			bool hasMapMethod = !string.IsNullOrEmpty(mapMethodVal);
			if (hasMapMethod)
			{
				string[] validMapMethods = { "Position", "Name" };
				if (Array.IndexOf(validMapMethods, mapMethodVal) < 0)
				{
					throw new ArgumentException($"Invalid mapMethod: '{mapMethodVal}'.");
				}
			}

			// --------------------------------------------------------------------
			// 2. Build the path to the FastTransfer tool
			// --------------------------------------------------------------------
			// Ensure we have the actual EXE name or './fasttransfer' at the end
			string exePath = exeDir;
			// If not specifically ending with "FastTransfer.exe" or "fasttransfer",
			// try appending "FastTransfer.exe" automatically:
			if (!exePath.EndsWith("FastTransfer.exe", StringComparison.OrdinalIgnoreCase) &&
				!exePath.EndsWith("fasttransfer", StringComparison.OrdinalIgnoreCase))
			{
				if (!exePath.EndsWith("\\") && !exePath.EndsWith("/"))
				{
					exePath += Path.DirectorySeparatorChar;
				}
				exePath += "FastTransfer.exe";
			}


			SqlContext.Pipe.Send("FastTransfer Path: " + exePath + Environment.NewLine);

			// --------------------------------------------------------------------
			// 3. Construct Command-Line Arguments
			// --------------------------------------------------------------------
			// We'll build this up in a string. We'll quote all values that might have spaces.
			// The final invocation is: "exePath" <arguments>

			// Helper local function for quoting arguments
			string Q(string val) => $"\"{val}\"";

			// Start with empty arguments
			string args = string.Empty;

			// --sourceconnectiontype
			args += $" --sourceconnectiontype {Q(srcConnType)}";

			// Source connection parameters
			if (hasSrcConnString)
			{
				// --sourceconnectstring
				args += $" --sourceconnectstring {Q(srcConnStr)}";
			}
			else
			{
				// Either server or DSN
				if (!string.IsNullOrEmpty(srcServerVal))
				{
					args += $" --sourceserver {Q(srcServerVal)}";

					// If provider is not null, add it
					if (!string.IsNullOrEmpty(srcProvider))
					{
						args += $" --sourceprovider {Q(srcProvider)}";
					}
				}
				else
				{
					// Then must be DSN
					args += $" --sourcedsn {Q(srcDsn)}";
				}

				if (srcTrusted)
				{
					args += " --sourcetrusted";
				}
				else
				{
					args += $" --sourceuser {Q(srcUserVal)} --sourcepassword {Q(srcPasswordVal)}";
				}

				args += $" --sourcedatabase {Q(srcDatabaseVal)}";
			}

			// Source Infos
			if (!string.IsNullOrEmpty(fileInputVal))
			{
				args += $" --fileinput {Q(fileInputVal)}";
			}
			else if (!string.IsNullOrEmpty(queryVal))
			{
				args += $" --query {Q(queryVal)}";
			}
			
			if (!string.IsNullOrEmpty(srcTableVal) && !string.IsNullOrEmpty(srcSchemaVal))
			{
				args += $" --sourceschema {Q(srcSchemaVal)}";
				args += $" --sourcetable {Q(srcTableVal)}";
			}

			// --targetconnectiontype
			args += $" --targetconnectiontype {Q(tgtConnType)}";

			// Target connection parameters
			if (hasTgtConnString)
			{
				// --targetconnectstring
				args += $" --targetconnectstring {Q(tgtConnStr)}";
			}
			else
			{
				args += $" --targetserver {Q(tgtServerVal)}";

				if (tgtTrusted)
				{
					args += " --targettrusted";
				}
				else
				{
					args += $" --targetuser {Q(tgtUserVal)} --targetpassword {Q(tgtPasswordVal)}";
				}

				args += $" --targetdatabase {Q(tgtDatabaseVal)}";
			}

			// Target Infos
			args += $" --targetschema {Q(tgtSchemaVal)}";
			args += $" --targettable {Q(tgtTableVal)}";
			if (!string.IsNullOrEmpty(loadModeVal))
			{
				args += $" --loadmode {Q(loadModeVal)}";
			}
			if (batchSizeVal.HasValue)
			{
				args += $" --batchsize {batchSizeVal.Value}";
			}

			// Advanced Parameters
			if (hasMethod)
			{
				// e.g. --method "RangeId" ...
				// If methodVal = "None" =>  --method None
				args += $" --method {Q(methodVal)}";
				if (!methodVal.Equals("None", StringComparison.OrdinalIgnoreCase))
				{
					// If it's one of the partition-based
					if (methodVal.Equals("random", StringComparison.OrdinalIgnoreCase) ||
						methodVal.Equals("datadriven", StringComparison.OrdinalIgnoreCase) ||
						methodVal.Equals("rangeid", StringComparison.OrdinalIgnoreCase) ||
						methodVal.Equals("ntile", StringComparison.OrdinalIgnoreCase))
					{
						args += $" --distributeKeyColumn {Q(distKeyColVal)}";
					}
					args += $" --degree {degreeVal.Value}";
				}
			}
			if (hasMapMethod)
			{
				args += $" --mapmethod {Q(mapMethodVal)}";
			}

			// Log Parameters
			if (!string.IsNullOrEmpty(runIdVal))
			{
				args += $" --runid {Q(runIdVal)}";
			}
			if (!string.IsNullOrEmpty(settingsFileVal))
			{
				args += $" --settingsfile {Q(settingsFileVal)}";
			}

			// Trim leading space
			args = args.Trim();

			//print the command line exe and args
			SqlContext.Pipe.Send("FastTransfer Command " + exePath + " " + args + Environment.NewLine);

			// --------------------------------------------------------------------
			// 4. Execute the CLI
			// --------------------------------------------------------------------
			ProcessStartInfo psi = new ProcessStartInfo
			{
				FileName = exePath,
				Arguments = args,
				UseShellExecute = false, // maybe true ?
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				CreateNoWindow = true
			};

			try
			{
				using (Process proc = Process.Start(psi))
				{
					// Read output by 
					string stdout = proc.StandardOutput.ReadToEnd();
					string stderr = proc.StandardError.ReadToEnd();

					proc.WaitForExit();
					int exitCode = proc.ExitCode;

					// Send the output back to the SQL client
					if (!string.IsNullOrEmpty(stdout))
					{
						// Diviser la sortie en morceaux de 4000 caractères
						int chunkSize = 4000;
						for (int i = 0; i < stdout.Length; i += chunkSize)
						{
							string chunk = stdout.Substring(i, Math.Min(chunkSize, stdout.Length - i));
							SqlContext.Pipe.Send(chunk);
						}
					}
					if (!string.IsNullOrEmpty(stderr))
					{
						SqlContext.Pipe.Send("FastTransfer stderr:\r\n" + stderr);
					}

					if (exitCode != 0)
					{
						throw new Exception($"FastTransfer process returned exit code {exitCode}.");
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error invoking FastTransfer: " + ex.Message, ex);
			}
		}
	}

}