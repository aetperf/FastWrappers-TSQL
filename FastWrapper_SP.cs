
namespace FastWrapper
{

	using System;
	using System.Data.SqlTypes;
	using System.Diagnostics;
	using System.IO;
	using System.Security.Permissions;
	using Microsoft.SqlServer.Server;

	public static class FastTransferCLR
	{
		[SqlProcedure]
		public static void RunFastTransfer(
			// 1) FastTransfer Binary Directory/Path
			SqlString fastTransferDir,   // e.g. ".\" or "C:\\tools\\MyFastTransfer" or "./fasttransfer"

			// 2) Source Connection
			SqlString sourceConnectionType, // must be one of [clickhouse, duckdb, hana, mssql, mysql, nzsql, odbc, oledb, oraodp, pgcopy, pgsql, teradata]
			SqlString sourceConnectString,  // if provided, we skip server/user/password/etc.

			// If not using connect string, then:
			SqlString sourceServer,         // e.g. "Host", "Host:Port", "Host\\Instance", "Host:Port/TNSService"
			SqlString sourceDSN,           // optional DSN name if using ODBC
			SqlString sourceProvider,       // optional for OLEDB
			SqlBoolean isSourceTrusted,
			SqlString sourceUser,
			SqlString sourcePassword,
			SqlString sourceDatabase,

			// 3) Source Infos
			SqlString fileInput,    // e.g. "C:\\path\\to\\file.sql"
			SqlString query,        // e.g. "SELECT * FROM table"
			SqlString sourceSchema,
			SqlString sourceTable,

			// 4) Target Connection
			SqlString targetConnectionType, // must be one of [clickhousebulk, duckdb, hanabulk, msbulk, mysqlbulk, nzbulk, orabulk, oradirect, pgcopy, teradata]
			SqlString targetConnectString,

			// If not using connect string, then:
			SqlString targetServer,
			SqlBoolean isTargetTrusted,
			SqlString targetUser,
			SqlString targetPassword,
			SqlString targetDatabase,

			// 5) Target Infos
			SqlString targetSchema,
			SqlString targetTable,
			SqlString loadMode,     // "Append" or "Truncate"
			SqlInt32 batchSize,     // e.g. 10000

			// 6) Advanced Parameters
			SqlString method,       // "None", "Random", "DataDriven", "RangeId", "Ntile", "Ctid", "Rowid"
			SqlString distributeKeyColumn, // required if method in ["Random","DataDriven","RangeId","Ntile"]
			SqlInt32 degree,        // concurrency degree if method != "None" useless if method = "None" should be != 1
			SqlString mapMethod,    // "Position"(default) or "Name"

			// 7) Log Parameters
			SqlString runId,
			SqlString settingsFile
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