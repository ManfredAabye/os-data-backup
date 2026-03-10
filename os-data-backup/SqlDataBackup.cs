using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using log4net;
using MySql.Data.MySqlClient;
using Nini.Config;
using OpenSim.Framework;
using OpenSim.Framework.Servers.HttpServer;
using OpenSim.Framework.Serialization;
using OpenSim.Server.Handlers.Base;

namespace OpenSim.Addons.SqlDataBackup
{
	public class SqlDataBackup : ServiceConnector
	{
		private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
		private static readonly Regex s_safeTableNameRegex = new Regex("^[A-Za-z0-9_]+$", RegexOptions.Compiled);
		private const string OtbExtension = ".otb";
		private const long DefaultMaxSingleTableExportBytes = 0;
		private const long DefaultMaxOtbPartBytes = 512L * 1024L * 1024L;

		private readonly bool m_enabled;
		private readonly string m_connectionString;
		private readonly string m_commandPrefix;
		private readonly string m_backupFolder;
		private readonly long m_maxSingleTableExportBytes;
		private readonly long m_maxOtbPartBytes;
		private readonly object m_tableProcessingLock = new object();
		private int m_bulkOperationRunning;

		public SqlDataBackup(IConfigSource config, IHttpServer server, string configName)
		{
			string effectiveConfigName = string.IsNullOrWhiteSpace(configName) ? "SqlDataBackup" : configName;
			IConfig moduleConfig = config.Configs[effectiveConfigName];
			if (moduleConfig == null)
				return;

			m_enabled = moduleConfig.GetBoolean("Enabled", false);
			if (!m_enabled)
				return;

			IConfig dbConfig = config.Configs["DatabaseService"];
			m_connectionString = moduleConfig.GetString("ConnectionString", dbConfig != null ? dbConfig.GetString("ConnectionString", string.Empty) : string.Empty);
			m_commandPrefix = moduleConfig.GetString("CommandPrefix", "sqlbackup").Trim();
			m_backupFolder = moduleConfig.GetString("BackupFolder", "backupOTB");
			m_maxSingleTableExportBytes = ReadMaxSingleTableExportBytes(moduleConfig);
			m_maxOtbPartBytes = ReadMaxOtbPartBytes(moduleConfig);

			if (string.IsNullOrWhiteSpace(m_connectionString))
			{
				m_log.Error("[SQL DATA BACKUP]: ConnectionString fehlt. Addon wird nicht aktiviert.");
				return;
			}

			if (MainConsole.Instance == null)
			{
				m_log.Warn("[SQL DATA BACKUP]: MainConsole nicht verfuegbar. Befehle wurden nicht registriert.");
				return;
			}

			MainConsole.Instance.Commands.AddCommand(
				"Backup", false,
				m_commandPrefix + " list",
				m_commandPrefix + " list",
				"Listet alle Tabellen in der Datenbank auf.",
				HandleCommand);

			MainConsole.Instance.Commands.AddCommand(
				"Backup", false,
				m_commandPrefix + " export",
				m_commandPrefix + " export <table|all> <datei.otb|ordner>",
				"Exportiert eine oder alle Tabellen als .otb Archiv.",
				HandleCommand);

			MainConsole.Instance.Commands.AddCommand(
				"Backup", false,
				m_commandPrefix + " import",
				m_commandPrefix + " import <table|all> <datei.otb|ordner>",
				"Importiert eine oder alle Tabellen aus .otb Archiven.",
				HandleCommand);

			m_log.InfoFormat("[SQL DATA BACKUP]: Aktiviert. Kommando-Praefix '{0}', MaxSingleTableExportBytes={1}, MaxOtbPartBytes={2}.", m_commandPrefix, m_maxSingleTableExportBytes, m_maxOtbPartBytes);
		}

		private static long ReadMaxSingleTableExportBytes(IConfig moduleConfig)
		{
			string configured = moduleConfig.GetString("MaxSingleTableExportBytes", DefaultMaxSingleTableExportBytes.ToString(CultureInfo.InvariantCulture));
			if (long.TryParse(configured, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) && parsed >= 0)
				return parsed;

			return DefaultMaxSingleTableExportBytes;
		}

		private static long ReadMaxOtbPartBytes(IConfig moduleConfig)
		{
			string configured = moduleConfig.GetString("MaxOtbPartBytes", DefaultMaxOtbPartBytes.ToString(CultureInfo.InvariantCulture));
			if (long.TryParse(configured, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) && parsed > 0)
			{
				if (parsed > int.MaxValue)
					return int.MaxValue;

				return parsed;
			}

			return DefaultMaxOtbPartBytes;
		}

		private void HandleCommand(string module, string[] cmd)
		{
			if (cmd.Length < 2)
			{
				ShowUsage();
				return;
			}

			try
			{
				string action = cmd[1].ToLowerInvariant();
				switch (action)
				{
					case "list":
						ListTables();
						break;
					case "export":
						HandleExport(cmd);
						break;
					case "import":
						HandleImport(cmd);
						break;
					default:
						ShowUsage();
						break;
				}
			}
			catch (Exception ex)
			{
				m_log.Error("[SQL DATA BACKUP]: Fehler im Kommando.", ex);
				MainConsole.Instance.Output("Fehler: {0}", ex.Message);
			}
		}

		private void HandleExport(string[] cmd)
		{
			if (cmd.Length < 3)
			{
				ShowUsage();
				return;
			}

			string scope = cmd[2];
			string targetPath = cmd.Length >= 4 ? cmd[3] : string.Empty;

			if (scope.Equals("all", StringComparison.OrdinalIgnoreCase))
			{
				RunBulkInBackground("export all", () => ExportAllTables(targetPath));
				return;
			}

			EnsureSafeTableName(scope);
			if (string.IsNullOrWhiteSpace(targetPath))
				targetPath = BuildDefaultBackupFilePath(scope);
			else if (Directory.Exists(targetPath))
				targetPath = Path.Combine(targetPath, BuildTimestampedBackupFileName(scope));

			ExportTable(scope, targetPath, true);
		}

		private void HandleImport(string[] cmd)
		{
			if (cmd.Length < 4)
			{
				ShowUsage();
				return;
			}

			string scope = cmd[2];
			string sourcePath = cmd[3];

			if (scope.Equals("all", StringComparison.OrdinalIgnoreCase))
			{
				RunBulkInBackground("import all", () => ImportAllTables(sourcePath));
				return;
			}

			EnsureSafeTableName(scope);
			ImportTable(scope, sourcePath, true);
		}

		private void ListTables()
		{
			List<string> tables = GetAllTables();
			MainConsole.Instance.Output("Tabellen:");
			foreach (string table in tables)
				MainConsole.Instance.Output(" - " + table);
			MainConsole.Instance.Output("Gesamt: {0}", tables.Count);
		}

		private void ExportAllTables(string folderPath)
		{
			if (string.IsNullOrWhiteSpace(folderPath))
				folderPath = m_backupFolder;

			Directory.CreateDirectory(folderPath);
			string batchTimestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);

			List<string> tables = GetAllTables();
			int done = 0;
			int failed = 0;
			foreach (string table in tables)
			{
				try
				{
					string filePath = Path.Combine(folderPath, table + "_" + batchTimestamp + OtbExtension);
					MainConsole.Instance.Output("Sichere Tabelle: {0} -> {1}", table, filePath);
					ExportTable(table, filePath, false);
					done++;
				}
				catch (Exception ex)
				{
					failed++;
					m_log.Error("[SQL DATA BACKUP]: Export fuer Tabelle fehlgeschlagen: " + table, ex);
					MainConsole.Instance.Output("Fehler bei Export von {0}: {1}", table, ex.Message);
				}
			}

			MainConsole.Instance.Output("Export abgeschlossen: {0} ok, {1} Fehler, Ziel {2}", done, failed, folderPath);
		}

		private void ImportAllTables(string folderPath)
		{
			if (string.IsNullOrWhiteSpace(folderPath))
				folderPath = m_backupFolder;

			if (!Directory.Exists(folderPath))
				throw new DirectoryNotFoundException("Backup-Ordner nicht gefunden: " + folderPath);

			string[] files = Directory.GetFiles(folderPath, "*" + OtbExtension);
			Array.Sort(files, StringComparer.OrdinalIgnoreCase);

			int done = 0;
			int failed = 0;
			foreach (string file in files)
			{
				string table = GetTableNameFromBackupFile(file);
				if (!IsSafeTableName(table))
				{
					MainConsole.Instance.Output("Ueberspringe unsicheren Dateinamen: {0}", file);
					continue;
				}

				try
				{
					MainConsole.Instance.Output("Importiere Tabelle: {0} <- {1}", table, file);
					ImportTable(table, file, false);
					done++;
				}
				catch (Exception ex)
				{
					failed++;
					m_log.Error("[SQL DATA BACKUP]: Import aus Datei fehlgeschlagen: " + file, ex);
					MainConsole.Instance.Output("Fehler bei Import aus {0}: {1}", file, ex.Message);
				}
			}

			MainConsole.Instance.Output("Import abgeschlossen: {0} ok, {1} Fehler, Quelle {2}", done, failed, folderPath);
		}

		private void RunBulkInBackground(string operationName, Action operation)
		{
			if (Interlocked.CompareExchange(ref m_bulkOperationRunning, 1, 0) != 0)
			{
				MainConsole.Instance.Output("Es laeuft bereits ein SQL-Backup Bulk-Job. Bitte warten.");
				return;
			}

			MainConsole.Instance.Output("Starte {0} im Hintergrund...", operationName);
			ThreadPool.QueueUserWorkItem(_ =>
			{
				try
				{
					operation();
				}
				catch (Exception ex)
				{
					m_log.Error("[SQL DATA BACKUP]: Bulk-Operation fehlgeschlagen.", ex);
					MainConsole.Instance.Output("Bulk-Operation fehlgeschlagen: {0}", ex.Message);
				}
				finally
				{
					Interlocked.Exchange(ref m_bulkOperationRunning, 0);
				}
			});
		}

		private static string GetTableNameFromBackupFile(string filePath)
		{
			string name = Path.GetFileNameWithoutExtension(filePath) ?? string.Empty;
			name = Regex.Replace(name, "\\.part\\d{4}$", string.Empty, RegexOptions.IgnoreCase);
			Match m = Regex.Match(name, "^(?<table>[A-Za-z0-9_]+)_\\d{8}_\\d{6}$");
			if (m.Success)
				return m.Groups["table"].Value;

			return name;
		}

		private void ExportTable(string tableName, string filePath, bool verbose)
		{
			lock (m_tableProcessingLock)
			{
				EnsureSafeTableName(tableName);
				filePath = EnsureOtbPath(filePath);

				long approxBytes = GetApproxTableSizeBytes(tableName);
				if (m_maxSingleTableExportBytes > 0 && approxBytes > m_maxSingleTableExportBytes)
				{
					throw new InvalidOperationException(
						"Tabelle zu gross fuer OTB-Export in diesem Modus: " + tableName
						+ " (ca. " + approxBytes.ToString(CultureInfo.InvariantCulture)
						+ " Bytes, Limit " + m_maxSingleTableExportBytes.ToString(CultureInfo.InvariantCulture)
						+ "). Bitte Tabelle aufteilen oder extern dumpen.");
				}

				string directory = Path.GetDirectoryName(Path.GetFullPath(filePath));
				if (!string.IsNullOrWhiteSpace(directory))
					Directory.CreateDirectory(directory);

				ExportTableToOtbParts(tableName, filePath, out int rowCount, out int skippedRows, out int partCount);
				long totalBackupBytes = GetWrittenBackupSizeBytes(filePath, partCount);

				MainConsole.Instance.Output(
					"Tabelle gespeichert: {0} ({1} Eintraege, {2} uebersprungen, {3} Teil(e), {4}) -> {5}",
					tableName,
					rowCount,
					skippedRows,
					partCount,
					FormatSize(totalBackupBytes),
					filePath);
			}
		}

		private void ImportTable(string tableName, string filePath, bool verbose)
		{
			lock (m_tableProcessingLock)
			{
				EnsureSafeTableName(tableName);
				filePath = EnsureOtbPath(filePath);

				if (!File.Exists(filePath))
					throw new FileNotFoundException("Backup-Datei nicht gefunden", filePath);

				string scriptText = ReadOtbArchive(filePath, tableName);
				if (string.IsNullOrWhiteSpace(scriptText))
					throw new InvalidOperationException("Backup-Datei ist leer: " + filePath);

				using (MySqlConnection conn = new MySqlConnection(m_connectionString))
				{
					conn.Open();
					ExecuteScriptLenient(conn, tableName, filePath, scriptText, out int executedStatements, out int failedStatements);

					if (failedStatements > 0)
					{
						MainConsole.Instance.Output(
							"Import mit Fehlern fortgesetzt: {0} ({1} Statements ok, {2} uebersprungen) <- {3}",
							tableName,
							executedStatements,
							failedStatements,
							filePath);
					}
					else if (verbose)
					{
						MainConsole.Instance.Output("Importiert: {0} ({1} Statements) <- {2}", tableName, executedStatements, filePath);
					}
				}
			}
		}

		private void ExportTableToOtbParts(string tableName, string filePath, out int rowCount, out int skippedRows, out int partCount)
		{
			string createStatement = string.Empty;
			List<string> columns = new List<string>();
			Encoding enc = new UTF8Encoding(false);
			string createdUtc = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture);
			string firstHeader = BuildPartHeader(tableName, createStatement, createdUtc, true);
			string nextHeader = BuildPartHeader(tableName, createStatement, createdUtc, false);
			const string footer = "SET FOREIGN_KEY_CHECKS=1;\n";
			long footerBytes = enc.GetByteCount(footer);
			long maxPartBytes = m_maxOtbPartBytes > 0 ? m_maxOtbPartBytes : DefaultMaxOtbPartBytes;
			string basePath = Path.Combine(Path.GetDirectoryName(filePath) ?? string.Empty, Path.GetFileNameWithoutExtension(filePath));
			StringBuilder currentPart = null;
			long currentBytes = 0;
			bool currentPartHasRows = false;
			bool splitMode = false;
			rowCount = 0;
			skippedRows = 0;
			partCount = 0;

			DeleteExistingSplitArtifacts(filePath, basePath);

			using (MySqlConnection conn = new MySqlConnection(m_connectionString))
			{
				conn.Open();

				using (MySqlCommand createCmd = conn.CreateCommand())
				{
					createCmd.CommandText = "SHOW CREATE TABLE `" + tableName + "`";
					using (MySqlDataReader reader = createCmd.ExecuteReader())
					{
						if (!reader.Read())
							throw new InvalidOperationException("Tabelle nicht gefunden: " + tableName);

						createStatement = reader.GetString("Create Table");
					}
				}

				firstHeader = BuildPartHeader(tableName, createStatement, createdUtc, true);
				nextHeader = BuildPartHeader(tableName, createStatement, createdUtc, false);
				currentPart = new StringBuilder(firstHeader.Length + footer.Length + 1024);
				currentPart.Append(firstHeader);
				currentBytes = enc.GetByteCount(firstHeader);

				using (MySqlCommand selectCmd = conn.CreateCommand())
				{
					selectCmd.CommandText = "SELECT * FROM `" + tableName + "`";
					using (MySqlDataReader reader = selectCmd.ExecuteReader())
					{
						for (int i = 0; i < reader.FieldCount; i++)
							columns.Add("`" + reader.GetName(i) + "`");

						while (reader.Read())
						{
							string insertLine;
							try
							{
								insertLine = BuildInsertLine(tableName, reader, columns) + "\n";
							}
							catch (Exception ex)
							{
								skippedRows++;
								m_log.Error("[SQL DATA BACKUP]: Export-Eintrag uebersprungen fuer Tabelle " + tableName, ex);
								continue;
							}

							long lineBytes = enc.GetByteCount(insertLine);

							if (currentPartHasRows && currentBytes + lineBytes + footerBytes > maxPartBytes)
							{
								splitMode = true;
								FinalizePartWrite(filePath, basePath, tableName, currentPart, footer, ++partCount, splitMode);
								currentPart = new StringBuilder(nextHeader.Length + footer.Length + 1024);
								currentPart.Append(nextHeader);
								currentBytes = enc.GetByteCount(nextHeader);
								currentPartHasRows = false;
							}

							currentPart.Append(insertLine);
							currentBytes += lineBytes;
							currentPartHasRows = true;
							rowCount++;
						}
					}
				}
			}

			if (currentPart == null)
				throw new InvalidOperationException("Export-Teil konnte nicht initialisiert werden.");

			if (rowCount == 0)
			{
				FinalizePartWrite(filePath, basePath, tableName, currentPart, footer, 1, false);
				partCount = 1;
			}
			else
			{
				FinalizePartWrite(filePath, basePath, tableName, currentPart, footer, ++partCount, splitMode);
			}
		}

		private void ExecuteScriptLenient(MySqlConnection conn, string tableName, string filePath, string scriptText, out int executedStatements, out int failedStatements)
		{
			executedStatements = 0;
			failedStatements = 0;

			foreach (string statement in SplitSqlStatements(scriptText))
			{
				if (string.IsNullOrWhiteSpace(statement))
					continue;

				using (MySqlCommand cmd = conn.CreateCommand())
				{
					cmd.CommandText = statement;
					try
					{
						cmd.ExecuteNonQuery();
						executedStatements++;
					}
					catch (Exception ex)
					{
						failedStatements++;
						string preview = statement.Length > 180 ? statement.Substring(0, 180) + "..." : statement;
						m_log.Error("[SQL DATA BACKUP]: Import-Statement uebersprungen fuer Tabelle " + tableName + " aus " + filePath + " -> " + preview, ex);
					}
				}
			}
		}

		private static IEnumerable<string> SplitSqlStatements(string scriptText)
		{
			StringBuilder sb = new StringBuilder(scriptText.Length > 4096 ? 4096 : scriptText.Length);
			bool inSingleQuote = false;
			bool inDoubleQuote = false;
			bool inBacktick = false;
			bool escaped = false;

			for (int i = 0; i < scriptText.Length; i++)
			{
				char c = scriptText[i];

				if (escaped)
				{
					sb.Append(c);
					escaped = false;
					continue;
				}

				if (c == '\\')
				{
					sb.Append(c);
					escaped = true;
					continue;
				}

				if (!inDoubleQuote && !inBacktick && c == '\'')
				{
					inSingleQuote = !inSingleQuote;
					sb.Append(c);
					continue;
				}

				if (!inSingleQuote && !inBacktick && c == '"')
				{
					inDoubleQuote = !inDoubleQuote;
					sb.Append(c);
					continue;
				}

				if (!inSingleQuote && !inDoubleQuote && c == '`')
				{
					inBacktick = !inBacktick;
					sb.Append(c);
					continue;
				}

				if (!inSingleQuote && !inDoubleQuote && !inBacktick && c == ';')
				{
					string statement = sb.ToString().Trim();
					sb.Clear();
					if (!string.IsNullOrWhiteSpace(statement))
						yield return statement;
					continue;
				}

				sb.Append(c);
			}

			string trailing = sb.ToString().Trim();
			if (!string.IsNullOrWhiteSpace(trailing))
				yield return trailing;
		}

		private static string BuildPartHeader(string tableName, string createStatement, string createdUtc, bool firstPart)
		{
			StringBuilder builder = new StringBuilder(512);
			builder.AppendLine("-- OpenSim SQL Data Backup");
			builder.AppendLine("-- Table: " + tableName);
			builder.AppendLine("-- Created UTC: " + createdUtc);
			if (!firstPart)
				builder.AppendLine("-- Continuation");
			builder.AppendLine();
			builder.AppendLine("SET FOREIGN_KEY_CHECKS=0;");
			if (firstPart)
			{
				builder.AppendLine("DROP TABLE IF EXISTS `" + tableName + "`;");
				builder.AppendLine(createStatement + ";");
			}

			return builder.ToString();
		}

		private static void FinalizePartWrite(string filePath, string basePath, string tableName, StringBuilder partBuilder, string footer, int partNumber, bool splitMode)
		{
			partBuilder.Append(footer);
			string scriptText = partBuilder.ToString();
			string targetPath = splitMode
				? basePath + ".part" + partNumber.ToString("D4", CultureInfo.InvariantCulture) + OtbExtension
				: filePath;
			WriteSingleOtbArchive(targetPath, tableName, scriptText);
		}

		private static void DeleteExistingSplitArtifacts(string filePath, string basePath)
		{
			if (File.Exists(filePath))
			{
				File.SetAttributes(filePath, FileAttributes.Normal);
				File.Delete(filePath);
			}

			string directory = Path.GetDirectoryName(basePath);
			if (string.IsNullOrWhiteSpace(directory) || !Directory.Exists(directory))
				return;

			string baseName = Path.GetFileName(basePath);
			string[] existing = Directory.GetFiles(directory, baseName + ".part*" + OtbExtension);
			for (int i = 0; i < existing.Length; i++)
			{
				File.SetAttributes(existing[i], FileAttributes.Normal);
				File.Delete(existing[i]);
			}
		}

		private static long GetWrittenBackupSizeBytes(string filePath, int partCount)
		{
			if (partCount <= 1)
			{
				FileInfo single = new FileInfo(filePath);
				return single.Exists ? single.Length : 0;
			}

			string basePath = Path.Combine(Path.GetDirectoryName(filePath) ?? string.Empty, Path.GetFileNameWithoutExtension(filePath));
			long total = 0;
			for (int i = 1; i <= partCount; i++)
			{
				string partPath = basePath + ".part" + i.ToString("D4", CultureInfo.InvariantCulture) + OtbExtension;
				FileInfo info = new FileInfo(partPath);
				if (info.Exists)
					total += info.Length;
			}

			return total;
		}

		private static string FormatSize(long bytes)
		{
			string[] units = new string[] { "B", "KB", "MB", "GB", "TB" };
			double value = bytes;
			int unitIndex = 0;
			while (value >= 1024 && unitIndex < units.Length - 1)
			{
				value /= 1024;
				unitIndex++;
			}

			return value.ToString("0.##", CultureInfo.InvariantCulture) + " " + units[unitIndex];
		}

		private static void WriteSingleOtbArchive(string filePath, string tableName, string scriptText)
		{
			byte[] sqlData = new UTF8Encoding(false).GetBytes(scriptText);

			if (File.Exists(filePath))
			{
				File.SetAttributes(filePath, FileAttributes.Normal);
				File.Delete(filePath);
			}

			using (FileStream fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None))
			using (GZipStream gzip = new GZipStream(fileStream, CompressionMode.Compress))
			{
				TarArchiveWriter tar = new TarArchiveWriter(gzip);
				tar.WriteFile(tableName + ".sql", sqlData);
				tar.Close();
			}
		}

		private static string ReadOtbArchive(string filePath, string expectedTableName)
		{
			using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
			using (GZipStream gzip = new GZipStream(fileStream, CompressionMode.Decompress))
			{
				TarArchiveReader tar = new TarArchiveReader(gzip);
				while (true)
				{
					byte[] data = tar.ReadEntry(out string entryPath, out TarArchiveReader.TarEntryType entryType);
					if (data == null)
						break;

					if (entryType != TarArchiveReader.TarEntryType.TYPE_NORMAL_FILE)
						continue;

					string fileName = Path.GetFileName(entryPath);
					if (string.IsNullOrWhiteSpace(fileName))
						continue;

					string tableFromEntry = Path.GetFileNameWithoutExtension(fileName);
					if (!tableFromEntry.Equals(expectedTableName, StringComparison.OrdinalIgnoreCase))
						continue;

					return Encoding.UTF8.GetString(data);
				}
			}

			throw new InvalidOperationException("Keine gueltige SQL-Nutzlast fuer Tabelle in Archiv gefunden: " + filePath);
		}

		private static string EnsureOtbPath(string path)
		{
			if (path.EndsWith(OtbExtension, StringComparison.OrdinalIgnoreCase))
				return path;

			return path + OtbExtension;
		}

		private string BuildDefaultBackupFilePath(string tableName)
		{
			Directory.CreateDirectory(m_backupFolder);
			return Path.Combine(m_backupFolder, BuildTimestampedBackupFileName(tableName));
		}

		private static string BuildTimestampedBackupFileName(string tableName)
		{
			string timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
			return tableName + "_" + timestamp + OtbExtension;
		}

		private List<string> GetAllTables()
		{
			List<string> tables = new List<string>();

			using (MySqlConnection conn = new MySqlConnection(m_connectionString))
			{
				conn.Open();

				using (MySqlCommand cmd = conn.CreateCommand())
				{
					cmd.CommandText = "SHOW TABLES";
					using (MySqlDataReader reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							string table = reader.GetString(0);
							if (IsSafeTableName(table))
								tables.Add(table);
						}
					}
				}
			}

			tables.Sort(StringComparer.OrdinalIgnoreCase);
			return tables;
		}

		private long GetApproxTableSizeBytes(string tableName)
		{
			using (MySqlConnection conn = new MySqlConnection(m_connectionString))
			{
				conn.Open();

				using (MySqlCommand cmd = conn.CreateCommand())
				{
					cmd.CommandText = "SELECT COALESCE(DATA_LENGTH,0) + COALESCE(INDEX_LENGTH,0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @table";
					cmd.Parameters.AddWithValue("@table", tableName);

					object result = cmd.ExecuteScalar();
					if (result == null || result == DBNull.Value)
						return 0;

					return Convert.ToInt64(result, CultureInfo.InvariantCulture);
				}
			}
		}

		private static string BuildInsertLine(string tableName, IDataRecord row, List<string> columnNames)
		{
			StringBuilder sb = new StringBuilder(256);
			sb.Append("INSERT INTO `");
			sb.Append(tableName);
			sb.Append("` (");
			sb.Append(string.Join(",", columnNames));
			sb.Append(") VALUES (");

			for (int i = 0; i < row.FieldCount; i++)
			{
				if (i > 0)
					sb.Append(',');

				sb.Append(ToSqlLiteral(row.GetValue(i)));
			}

			sb.Append(");");
			return sb.ToString();
		}

		private static string ToSqlLiteral(object value)
		{
			if (value == null || value == DBNull.Value)
				return "NULL";

			Type t = value.GetType();

			if (t == typeof(byte[]))
			{
				byte[] data = (byte[])value;
				StringBuilder hex = new StringBuilder(data.Length * 2 + 2);
				hex.Append("0x");
				for (int i = 0; i < data.Length; i++)
					hex.Append(data[i].ToString("X2", CultureInfo.InvariantCulture));
				return hex.ToString();
			}

			if (t == typeof(bool))
				return ((bool)value) ? "1" : "0";

			if (t == typeof(DateTime))
			{
				DateTime dt = (DateTime)value;
				return "'" + dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";
			}

			if (t == typeof(float) || t == typeof(double) || t == typeof(decimal))
				return Convert.ToString(value, CultureInfo.InvariantCulture);

			if (t == typeof(sbyte) || t == typeof(byte) || t == typeof(short) || t == typeof(ushort) ||
				t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong))
				return Convert.ToString(value, CultureInfo.InvariantCulture);

			string s = Convert.ToString(value, CultureInfo.InvariantCulture);
			s = MySqlHelper.EscapeString(s ?? string.Empty);
			return "'" + s + "'";
		}

		private static bool IsSafeTableName(string tableName)
		{
			if (string.IsNullOrWhiteSpace(tableName))
				return false;

			return s_safeTableNameRegex.IsMatch(tableName);
		}

		private static void EnsureSafeTableName(string tableName)
		{
			if (!IsSafeTableName(tableName))
				throw new InvalidOperationException("Unsicherer Tabellenname: " + tableName);
		}

		private void ShowUsage()
		{
			MainConsole.Instance.Output("sqlbackup list");
			MainConsole.Instance.Output("sqlbackup export <table> <datei.otb>");
			MainConsole.Instance.Output("sqlbackup export all <ordner>");
			MainConsole.Instance.Output("sqlbackup import <table> <datei.otb>");
			MainConsole.Instance.Output("sqlbackup import all <ordner>");
		}
	}
}
