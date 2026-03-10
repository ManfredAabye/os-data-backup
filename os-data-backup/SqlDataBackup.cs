using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
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

		private readonly bool m_enabled;
		private readonly string m_connectionString;
		private readonly string m_commandPrefix;
		private readonly string m_backupFolder;

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
			m_backupFolder = moduleConfig.GetString("BackupFolder", "sqlbackup");

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

			m_log.InfoFormat("[SQL DATA BACKUP]: Aktiviert. Kommando-Praefix '{0}'.", m_commandPrefix);
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
			if (cmd.Length < 4)
			{
				MainConsole.Instance.Output("Syntax: {0} export <table|all> <datei.otb|ordner>", m_commandPrefix);
				return;
			}

			string scope = cmd[2];
			string targetPath = cmd[3];

			if (scope.Equals("all", StringComparison.OrdinalIgnoreCase))
			{
				ExportAllTables(targetPath);
				return;
			}

			EnsureSafeTableName(scope);
			ExportTable(scope, targetPath);
		}

		private void HandleImport(string[] cmd)
		{
			if (cmd.Length < 4)
			{
				MainConsole.Instance.Output("Syntax: {0} import <table|all> <datei.otb|ordner>", m_commandPrefix);
				return;
			}

			string scope = cmd[2];
			string sourcePath = cmd[3];

			if (scope.Equals("all", StringComparison.OrdinalIgnoreCase))
			{
				ImportAllTables(sourcePath);
				return;
			}

			EnsureSafeTableName(scope);
			ImportTable(scope, sourcePath);
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
			foreach (string table in tables)
			{
				string filePath = Path.Combine(folderPath, table + "_" + batchTimestamp + OtbExtension);
				ExportTable(table, filePath);
				done++;
			}

			MainConsole.Instance.Output("Export abgeschlossen: {0}/{1} Tabellen nach {2}", done, tables.Count, folderPath);
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
			foreach (string file in files)
			{
				string table = Path.GetFileNameWithoutExtension(file);
				if (!IsSafeTableName(table))
				{
					MainConsole.Instance.Output("Ueberspringe unsicheren Dateinamen: {0}", file);
					continue;
				}

				ImportTable(table, file);
				done++;
			}

			MainConsole.Instance.Output("Import abgeschlossen: {0}/{1} Dateien aus {2}", done, files.Length, folderPath);
		}

		private void ExportTable(string tableName, string filePath)
		{
			EnsureSafeTableName(tableName);
			filePath = EnsureOtbPath(filePath);

			string scriptText = BuildTableDumpScript(tableName, out int rowCount);

			string directory = Path.GetDirectoryName(Path.GetFullPath(filePath));
			if (!string.IsNullOrWhiteSpace(directory))
				Directory.CreateDirectory(directory);

			WriteOtbArchive(filePath, tableName, scriptText);

			MainConsole.Instance.Output("Exportiert: {0} ({1} Zeilen) -> {2}", tableName, rowCount, filePath);
		}

		private void ImportTable(string tableName, string filePath)
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

				MySqlScript script = new MySqlScript(conn, scriptText);
				script.Execute();
			}

			MainConsole.Instance.Output("Importiert: {0} <- {1}", tableName, filePath);
		}

		private string BuildTableDumpScript(string tableName, out int rowCount)
		{
			string createStatement = string.Empty;
			List<string> columns = new List<string>();
			List<string> insertLines = new List<string>();

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

				using (MySqlCommand selectCmd = conn.CreateCommand())
				{
					selectCmd.CommandText = "SELECT * FROM `" + tableName + "`";
					using (MySqlDataReader reader = selectCmd.ExecuteReader())
					{
						for (int i = 0; i < reader.FieldCount; i++)
							columns.Add("`" + reader.GetName(i) + "`");

						while (reader.Read())
							insertLines.Add(BuildInsertLine(tableName, reader, columns));
					}
				}
			}

			rowCount = insertLines.Count;

			StringBuilder builder = new StringBuilder(1024 + (insertLines.Count * 64));
			builder.AppendLine("-- OpenSim SQL Data Backup");
			builder.AppendLine("-- Table: " + tableName);
			builder.AppendLine("-- Created UTC: " + DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture));
			builder.AppendLine();
			builder.AppendLine("SET FOREIGN_KEY_CHECKS=0;");
			builder.AppendLine("DROP TABLE IF EXISTS `" + tableName + "`;");
			builder.AppendLine(createStatement + ";");

			foreach (string line in insertLines)
				builder.AppendLine(line);

			builder.AppendLine("SET FOREIGN_KEY_CHECKS=1;");
			return builder.ToString();
		}

		private static void WriteOtbArchive(string filePath, string tableName, string scriptText)
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
			MainConsole.Instance.Output("{0} list", m_commandPrefix);
			MainConsole.Instance.Output("{0} export <table> <datei.otb>", m_commandPrefix);
			MainConsole.Instance.Output("{0} export all <ordner>", m_commandPrefix);
			MainConsole.Instance.Output("{0} import <table> <datei.otb>", m_commandPrefix);
			MainConsole.Instance.Output("{0} import all <ordner>", m_commandPrefix);
		}
	}
}
