using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

namespace FastWrapper
{
	public static class FastBCPCLR
	{
		[SqlProcedure]
		public static void RunFastBCP_Secure(
			// Binary path
			SqlString fastBcpDir,

			// Connection infosa
			SqlString connectionType,          // clickhouse;hana;msoledbsql;mssql;mysql;nzcopy;nzoledb;nzsql;odbc;oledb;oraodp;pgcopy;pgsql;teradata
			SqlString sourceConnectStringEnc,  // chiffré (Base64) ; exclusif avec DSN/Provider/Server/User/Password/Trusted/Database
			SqlString dsn,                     // DSN (exclusif avec Provider/Server)
			SqlString provider,                // OleDB provider (MSOLEDBSQL, NZOLEDB, …)
			SqlString server,                  // Server | Server\Instance | Server:Port
			SqlString user,                    // facultatif si Trusted
			SqlString passwordEnc,             // chiffré (Base64) ; exclusif avec Trusted
			SqlBoolean trusted,                // exclusif avec User/Password
			SqlString database,                // obligatoire si pas de connectstring

			// Sources infos
			SqlString inputFile,               // -F fileinput (optionnel) – exclusif avec query
			SqlString query,                   // -q query (optionnel) – exclusif avec fileinput
			SqlString sourceschema,            // optionnel
			SqlString sourcetable,             // optionnel

			// Output infos
			SqlString outputFile,              // -o fileoutput (optionnel)
			SqlString outputDirectory,         // -D directory (optionnel)

			// Format options
			SqlString delimiter,               // -d (default "|")
			SqlBoolean usequotes,              // -t (bool)
			SqlString dateformat,              // -f (default "yyyy-MM-dd")
			SqlString encoding,                // -e (default "UTF-8")
			SqlString decimalseparator,        // -n (default ",")

			// Parallel options
			SqlInt32 degree,                   // -p (Default -2)
			SqlString method,                  // -m (Random;DataDriven;Ctid;Physloc;Rowid;RangeId;Ntile;None) (Default None)
			SqlString distributeKeyColumn,     // -c
			SqlString datadrivenquery,         // -Q (optionnel)
			SqlBoolean mergeDistributedFile,   // -M (bool) default True

			// Others
			SqlBoolean timestamped,            // -x
			SqlBoolean noheader,               // -h
			SqlString boolformat,              // -b automatic;true/false;1/0;t/f;
			SqlString runid,                   // -R
			SqlString settingsfile,            // -l (default FastBCP_Settings.json)
			SqlString cloudprofile,            // --cloudprofile
			SqlString license,                 // --license | --licence
			SqlString loglevel,                 // --loglevel Information|Debug|Verbose
			SqlBoolean debug                   // --debug (output stdout to SQL client)
		)
		{
			// -----------------------------
			// 1) Extract and decrypt parameters
			// -----------------------------
			string binDir = fastBcpDir.IsNull ? null : fastBcpDir.Value.Trim();

			string connType = connectionType.IsNull ? null : connectionType.Value.Trim().ToLowerInvariant();

			string connectStringEnc = sourceConnectStringEnc.IsNull ? null : sourceConnectStringEnc.Value.Trim();
			string connectString = null;
			if (!string.IsNullOrEmpty(connectStringEnc))
				connectString = KeyProvider.AesDecrypt(connectStringEnc);

			string dsnVal = dsn.IsNull ? null : dsn.Value.Trim();
			string providerVal = provider.IsNull ? null : provider.Value.Trim();
			string serverVal = server.IsNull ? null : server.Value.Trim();

			string userVal = user.IsNull ? null : user.Value.Trim();
			string passwordEncVal = passwordEnc.IsNull ? null : passwordEnc.Value.Trim();
			string passwordVal = null;
			if (!string.IsNullOrEmpty(passwordEncVal))
				passwordVal = KeyProvider.AesDecrypt(passwordEncVal);

			bool trustedVal = !trusted.IsNull && trusted.Value;

			string databaseVal = database.IsNull ? null : database.Value.Trim();

			string inputFileVal = inputFile.IsNull ? null : inputFile.Value.Trim();
			string queryVal = query.IsNull ? null : query.Value.Trim();
			string srcSchemaVal = sourceschema.IsNull ? null : sourceschema.Value.Trim();
			string srcTableVal = sourcetable.IsNull ? null : sourcetable.Value.Trim();

			string outFileVal = outputFile.IsNull ? null : outputFile.Value.Trim();
			string outDirVal = outputDirectory.IsNull ? null : outputDirectory.Value.Trim();

			string delimiterVal = delimiter.IsNull ? null : delimiter.Value.Trim();
			bool quotesVal = !usequotes.IsNull && usequotes.Value;
			string datefmtVal = dateformat.IsNull ? null : dateformat.Value.Trim();
			string encodingVal = encoding.IsNull ? null : encoding.Value.Trim();
			string decSepVal = decimalseparator.IsNull ? null : decimalseparator.Value.Trim();

			int? degreeVal = degree.IsNull ? (int?)null : degree.Value;
			string methodVal = method.IsNull ? null : method.Value.Trim();
			string distKeyCol = distributeKeyColumn.IsNull ? null : distributeKeyColumn.Value.Trim();
			string ddQuery = datadrivenquery.IsNull ? null : datadrivenquery.Value.Trim();
			bool mergeVal = !mergeDistributedFile.IsNull && mergeDistributedFile.Value;

			bool tsVal = !timestamped.IsNull && timestamped.Value;
			bool noheaderVal = !noheader.IsNull && noheader.Value;
			string boolfmtVal = boolformat.IsNull ? null : boolformat.Value.Trim();
			string runidVal = runid.IsNull ? null : runid.Value.Trim();
			string settingsVal = settingsfile.IsNull ? null : settingsfile.Value.Trim();
			string cloudProfVal = cloudprofile.IsNull ? null : cloudprofile.Value.Trim();
			string licenseVal = license.IsNull ? null : license.Value.Trim();
			string loglevelVal = loglevel.IsNull ? null : loglevel.Value.Trim();
			bool debugVal = !debug.IsNull && debug.Value ? true : false;

			// -----------------------------
			// Validations
			// -----------------------------

			// Binaire
			if (string.IsNullOrWhiteSpace(binDir))
				throw new ArgumentException("fastBcpDir must be provided.");

			// Connection type
			string[] allowedConn = {
				"clickhouse","hana","msoledbsql","mssql","mysql",
				"nzcopy","nzoledb","nzsql","odbc","oledb","oraodp","pgcopy","pgsql","teradata"
			};
			if (Array.IndexOf(allowedConn, connType) < 0)
				throw new ArgumentException($"Invalid connectiontype '{connType}'.");

			// DistinctGroupsCertification("N","P,S") → DSN XOR (Provider or Server)
			// => si DSN fourni, alors Provider & Server doivent être vides
			if (!string.IsNullOrEmpty(dsnVal))
			{
				if (!string.IsNullOrEmpty(providerVal) || !string.IsNullOrEmpty(serverVal))
					throw new ArgumentException("DSN cannot be used together with Provider or Server.");
			}

			// DistinctGroupsCertification("A","U,X") → Trusted XOR (User or Password)
			if (trustedVal && (!string.IsNullOrEmpty(userVal) || !string.IsNullOrEmpty(passwordVal)))
				throw new ArgumentException("trusted cannot be used together with user/password.");
			if (!trustedVal && (string.IsNullOrEmpty(userVal) || string.IsNullOrEmpty(passwordVal)) && string.IsNullOrEmpty(connectString))
				throw new ArgumentException("When not trusted and no connectstring, user and password are required.");

			// Exclusivity connectstring vs (dsn/provider/server/user/password/trusted/database)
			if (!string.IsNullOrEmpty(connectString))
			{
				if (!string.IsNullOrEmpty(dsnVal) || !string.IsNullOrEmpty(providerVal) || !string.IsNullOrEmpty(serverVal) ||
					!string.IsNullOrEmpty(userVal) || !string.IsNullOrEmpty(passwordVal) || trustedVal || !string.IsNullOrEmpty(databaseVal))
				{
					throw new ArgumentException("sourceconnectstring is exclusive with dsn/provider/server/user/password/trusted/database.");
				}
			}
			else
			{
				// mandatory database parameter
				if (string.IsNullOrEmpty(databaseVal))
					throw new ArgumentException("database is required when sourceconnectstring is not provided.");
				// DSN ou Server/Provider doivent couvrir la connectivité
				if (string.IsNullOrEmpty(dsnVal) && string.IsNullOrEmpty(serverVal))
					throw new ArgumentException("Either dsn or server must be provided (when not using sourceconnectstring).");
			}

			// ArgumentGroupCertification("F,q", OneOrNoneUsed) → fileinput XOR query (ou aucun des deux)
			if (!string.IsNullOrEmpty(inputFileVal) && !string.IsNullOrEmpty(queryVal))
				throw new ArgumentException("fileinput and query are mutually exclusive.");

			// Sortie : au moins un des deux conseillé (fichier ou répertoire)
			if (string.IsNullOrEmpty(outFileVal) && string.IsNullOrEmpty(outDirVal))
				SqlContext.Pipe.Send("Warning: neither fileoutput nor directory provided; ensure your scenario is valid.\r\n");

			// -----------------------------
			// FastBCP path building
			// -----------------------------
			string exePath = binDir;
			if (!exePath.EndsWith("FastBCP.exe", StringComparison.OrdinalIgnoreCase) &&
				!exePath.EndsWith("fastbcp", StringComparison.OrdinalIgnoreCase))
			{
				if (!exePath.EndsWith("\\") && !exePath.EndsWith("/"))
					exePath += Path.DirectorySeparatorChar;
				exePath += "FastBCP.exe";
			}

			// -----------------------------
			// Argument line building
			// -----------------------------
			string Q(string v) => $"\"{v}\"";
			var args = new System.Text.StringBuilder();

			// connection type
			if (!string.IsNullOrEmpty(connType))
				args.Append(" --connectiontype ").Append(Q(connType));

			// connectstring VS dsn/provider/server/user/password/trusted/database
			if (!string.IsNullOrEmpty(connectString))
			{
				args.Append(" --sourceconnectstring ").Append(Q(connectString));
			}
			else
			{
				if (!string.IsNullOrEmpty(dsnVal))
					args.Append(" --sourcedsn ").Append(Q(dsnVal));
				if (!string.IsNullOrEmpty(providerVal))
					args.Append(" --sourceprovider ").Append(Q(providerVal));
				if (!string.IsNullOrEmpty(serverVal))
					args.Append(" --sourceserver ").Append(Q(serverVal));

				if (trustedVal)
					args.Append(" --sourcetrusted");
				else
				{
					if (!string.IsNullOrEmpty(userVal))
						args.Append(" --sourceuser ").Append(Q(userVal));
					if (!string.IsNullOrEmpty(passwordVal))
						args.Append(" --sourcepassword ").Append(Q(passwordVal));
				}

				if (!string.IsNullOrEmpty(databaseVal))
					args.Append(" --sourcedatabase ").Append(Q(databaseVal));
			}

			// decimal separator
			if (!string.IsNullOrEmpty(decSepVal))
				args.Append(" --decimalseparator ").Append(Q(decSepVal));

			// fileinput / query / schema-table
			if (!string.IsNullOrEmpty(inputFileVal))
				args.Append(" --fileinput ").Append(Q(inputFileVal));
			if (!string.IsNullOrEmpty(queryVal))
				args.Append(" --query ").Append(Q(queryVal));
			if (!string.IsNullOrEmpty(srcSchemaVal))
				args.Append(" --sourceschema ").Append(Q(srcSchemaVal));
			if (!string.IsNullOrEmpty(srcTableVal))
				args.Append(" --sourcetable ").Append(Q(srcTableVal));

			// Output
			if (!string.IsNullOrEmpty(outFileVal))
				args.Append(" --fileoutput ").Append(Q(outFileVal));
			if (!string.IsNullOrEmpty(outDirVal))
				args.Append(" --directory ").Append(Q(outDirVal));
			if (!string.IsNullOrEmpty(delimiterVal))
				args.Append(" --delimiter ").Append(Q(delimiterVal));
			if (quotesVal) args.Append(" --quotes true"); // le parser accepte ValueArgument<bool>
			if (!string.IsNullOrEmpty(datefmtVal))
				args.Append(" --dateformat ").Append(Q(datefmtVal));
			if (!string.IsNullOrEmpty(encodingVal))
				args.Append(" --encoding ").Append(Q(encodingVal));

			// Parallel options
			if (degreeVal.HasValue)
				args.Append(" --paralleldegree ").Append(degreeVal.Value);
			if (!string.IsNullOrEmpty(methodVal))
				args.Append(" --parallelmethod ").Append(Q(methodVal));
			if (!string.IsNullOrEmpty(distKeyCol))
				args.Append(" --distributekeycolumn ").Append(Q(distKeyCol));
			if (!string.IsNullOrEmpty(ddQuery))
				args.Append(" --datadrivenquery ").Append(Q(ddQuery));
			// Merge
			args.Append(" --merge ").Append(mergeVal ? "true" : "false");

			// Divers
			if (tsVal) args.Append(" --timestamped");
			if (noheaderVal) args.Append(" --noheader");
			if (!string.IsNullOrEmpty(boolfmtVal))
				args.Append(" --boolformat ").Append(Q(boolfmtVal));
			if (!string.IsNullOrEmpty(runidVal))
				args.Append(" --runid ").Append(Q(runidVal));
			if (!string.IsNullOrEmpty(settingsVal))
				args.Append(" --settingsfile ").Append(Q(settingsVal));
			if (!string.IsNullOrEmpty(cloudProfVal))
				args.Append(" --cloudprofile ").Append(Q(cloudProfVal));
			if (!string.IsNullOrEmpty(licenseVal))
				args.Append(" --license ").Append(Q(licenseVal));
			if (!string.IsNullOrEmpty(loglevelVal))
				args.Append(" --loglevel ").Append(Q(loglevelVal));

			string argLine = args.ToString().Trim();

			// Debug Display
			if (debugVal)
				SqlContext.Pipe.Send("FastBCP Command:\r\n" + exePath + " " + argLine + "\r\n");

			// -----------------------------
			// Execution
			// -----------------------------
			var psi = new ProcessStartInfo
			{
				FileName = exePath,
				Arguments = argLine,
				UseShellExecute = false,
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

					if (debugVal)
					{
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
					}
					// Extract some metrics from the end of the stdout (last 3000 characters) and search for 
					// Total rows : xxxx to get the number of rows processed
					// Total columns : xxxx to get the number of columns processed
					// Total cells : xxxx to get the number of cells processed
					// Total time : Elapsed=xxxx ms to get the total time taken in milliseconds
					// Target file(s) : need to get the list of target files created (if several files, comma separated)

					if (stdout.Length > 3000)
					{
						stdout = stdout.Substring(stdout.Length - 3000);
					}

					// Extract metrics from stdout using regex
					string totalRows = System.Text.RegularExpressions.Regex.Match(stdout, @"Total data rows\s*:\s*(\d+)").Groups[1].Value;
					string totalColumns = System.Text.RegularExpressions.Regex.Match(stdout, @"Total data columns\s*:\s*(\d+)").Groups[1].Value;
					string totalCells = System.Text.RegularExpressions.Regex.Match(stdout, @"Total cells\s*:\s*(\d+)").Groups[1].Value;
					string totalTime = System.Text.RegularExpressions.Regex.Match(stdout, @"Total time : Elapsed=\s*(\d+)\s*ms").Groups[1].Value;



					// Send metrics to the SQL client as a table output added to the parameters (exepted the passwords and connect strings)
					SqlDataRecord record = new SqlDataRecord(
						new SqlMetaData("sourcedatabase", SqlDbType.NVarChar, 256), // Col0
						new SqlMetaData("source", SqlDbType.NVarChar, -1),          // Col1
						new SqlMetaData("TotalRows", SqlDbType.BigInt),             // Col2
						new SqlMetaData("TotalColumns", SqlDbType.Int),				// Col3
						new SqlMetaData("TotalCells", SqlDbType.BigInt),			// Col4
						new SqlMetaData("TotalTimeMs", SqlDbType.BigInt),			// Col5
						new SqlMetaData("Status", SqlDbType.Int),					// Col6
						new SqlMetaData("Directory", SqlDbType.NVarChar, -1),		// Col7
						new SqlMetaData("Files", SqlDbType.NVarChar, -1),			// Col8
						new SqlMetaData("StdOut", SqlDbType.NVarChar, -1),			// Col9
						new SqlMetaData("StdErr", SqlDbType.NVarChar, -1)			// Col10
					);

					var errorMsg = string.Empty;
					if (exitCode != 0)
					{
						errorMsg = $"FastTransfer process failed with exit code {exitCode}. See stderr for details.";
						errorMsg += Environment.NewLine + stdout;
					}

					string source = "";
					// check if query is used then try if inputfile else database.schema.table
					if (!string.IsNullOrEmpty(queryVal))
					{
						source = queryVal;
					}
					else if (!string.IsNullOrEmpty(inputFileVal))
					{
						source = inputFileVal;
					}
					else
					{
						source = $"{databaseVal}.{srcSchemaVal}.{srcTableVal}";
					}

					string extension = Regex.Escape(Path.GetExtension(outFileVal)); // e.g. ".csv" → "\.csv"
					string pattern = $@"into\s+(.+?{extension})\s";

					var matches = Regex.Matches(stdout, pattern, RegexOptions.IgnoreCase);
					//For files extract multiple files if any from stdout and comma separate them

					List<string> files = new List<string>();

					foreach (Match match in matches)
					{
						if (match.Success)
						{
							files.Add(match.Groups[1].Value);
						}
					}
					string filestringlog = string.Join(", ", files);

					string logout = "";

					if (debugVal)
					{
						logout = "\r\nFastBCP stdout:\r\n" + stdout;
					}

					


					record.SetString(0, databaseVal ?? string.Empty);
					record.SetString(1, source ?? string.Empty);
					record.SetInt64(2, Int64.TryParse(totalRows, out Int64 rows) ? rows : 0);
					record.SetInt32(3, int.TryParse(totalColumns, out int cols) ? cols : 0);
					record.SetInt64(4, Int64.TryParse(totalCells, out Int64 cells) ? cells : 0);
					record.SetInt64(5, Int64.TryParse(totalTime, out Int64 time) ? time : 0);
					record.SetInt32(6, exitCode); // Status: 0 for success, non-zero for error
					record.SetString(7, outDirVal ?? string.Empty);
					record.SetString(8, filestringlog ?? string.Empty);
					record.SetString(9, logout ?? string.Empty);
					record.SetString(10, errorMsg ?? string.Empty);



					SqlContext.Pipe.SendResultsStart(record);
					SqlContext.Pipe.SendResultsRow(record);
					SqlContext.Pipe.SendResultsEnd();


					if (!string.IsNullOrEmpty(stderr))
					{
						SqlContext.Pipe.Send("FastBCP stderr:\r\n" + stderr);
					}

					if (exitCode != 0)
					{
						throw new Exception($"FastBCP process returned exit code {exitCode}.");
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error invoking FastBCP: " + ex.Message, ex);
			}
		}
	}
}
