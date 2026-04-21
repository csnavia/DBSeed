using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

// Parse command-line arguments
if (args.Length == 0)
{
    Console.WriteLine("ERROR: Environment parameter is required.");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  DBSeed.exe <environment>");
    Console.WriteLine();
    Console.WriteLine("Available Environments:");
    Console.WriteLine("  Development  - Use development database and settings");
    Console.WriteLine("  Test         - Use test database and settings");
    Console.WriteLine("  Staging      - Use staging database and settings");
    Console.WriteLine("  Production   - Use production database and settings");
    Console.WriteLine();
    Console.WriteLine("Example:");
    Console.WriteLine("  DBSeed.exe Development");
    Console.WriteLine();
    return;
}

var environment = args[0];
var validEnvironments = new[] { "Development", "Test", "Staging", "Production" };

if (!validEnvironments.Contains(environment, StringComparer.OrdinalIgnoreCase))
{
    Console.WriteLine($"ERROR: Invalid environment '{environment}'.");
    Console.WriteLine();
    Console.WriteLine("Valid environments are:");
    foreach (var env in validEnvironments)
    {
        Console.WriteLine($"  - {env}");
    }
    Console.WriteLine();
    Console.WriteLine("Example:");
    Console.WriteLine("  DBSeed.exe Development");
    Console.WriteLine();
    return;
}

// Normalize environment name to match file naming convention
environment = validEnvironments.First(e => e.Equals(environment, StringComparison.OrdinalIgnoreCase));

var baseDirectory = Directory.GetCurrentDirectory();
var baseSettingsFile = "appsettings.json";
var environmentSettingsFile = $"appsettings.{environment}.json";

Console.WriteLine("Configuration Files:");
Console.WriteLine($"  Base Directory: {baseDirectory}");
Console.WriteLine($"  Loading: {baseSettingsFile}");
Console.WriteLine($"  Env Settings: {environmentSettingsFile}");


var baseSettingsPath = Path.Combine(baseDirectory, baseSettingsFile);
if (File.Exists(baseSettingsPath))
{
    Console.WriteLine($"    ✓ Found: {baseSettingsFile}");
}
else
{
    Console.WriteLine($"    ✗ NOT FOUND: {baseSettingsFile}");
}

Console.WriteLine($"  Loading: {environmentSettingsFile}");
var environmentSettingsPath = Path.Combine(baseDirectory, environmentSettingsFile);
if (File.Exists(environmentSettingsPath))
{
    Console.WriteLine($"    ✓ Found: {environmentSettingsFile}");
}
else
{
    Console.WriteLine($"    - Not found (optional): {environmentSettingsFile}");
}
Console.WriteLine();

// Build configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(baseDirectory)
    .AddJsonFile(baseSettingsFile, optional: false, reloadOnChange: true)
    .AddJsonFile(environmentSettingsFile, optional: true, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

// Read connection strings
var sourceConnection = configuration.GetConnectionString("SourceConnection");
var environmentLabel = configuration["EnvironmentLabel"];
var createInputFile = configuration["CreateInputFile"];
var updateInputFile = configuration["UpdateInputFile"];
var createStartLine = int.Parse(configuration["CreateStartLine"] ?? "1");
var updateStartLine = int.Parse(configuration["UpdateStartLine"] ?? "1");

var createContactLog = configuration["CreateContactLog"];
var updateContactLog = configuration["UpdateContactLog"];
//var logFile = configuration["LogFile"];

Console.WriteLine("Application Settings:");
Console.WriteLine($"  Environment: {environment}");
Console.WriteLine($"  Environment Label: {environmentLabel}");        
Console.WriteLine($"  Source Connection: {sourceConnection}");
//Console.WriteLine($"  Log File: {logFile}");
Console.WriteLine();

// Main menu loop
bool continueRunning = true;
while (continueRunning)
{
    Console.WriteLine("=".PadRight(70, '='));
    Console.WriteLine("DATABASE SEED MENU");
    Console.WriteLine("=".PadRight(70, '='));
    Console.WriteLine();
    Console.WriteLine("  [C] Create Contacts (DRY RUN - No changes)");
    Console.WriteLine("  [U] Update Contacts (DRY RUN - No changes)");
    Console.WriteLine();
    Console.WriteLine("  [X] Create Contacts (LIVE - Will insert records)");
    Console.WriteLine("  [Y] Update Contacts (LIVE - Will update records)");
    Console.WriteLine();
    Console.WriteLine("  [Q] Quit");
    Console.WriteLine();
    Console.Write("Select an option: ");

    var input = Console.ReadLine()?.Trim().ToUpper();
    Console.WriteLine();

    switch (input)
    {
        case "C":
            Console.WriteLine("*** Running CREATE in DRY RUN mode ***");
            Console.WriteLine();
            CreateContactsVision(createInputFile, sourceConnection, createContactLog, createStartLine, performUpdate: false);
            break;

        case "U":
            Console.WriteLine("*** Running UPDATE in DRY RUN mode ***");
            Console.WriteLine();
            UpdateContactsVision(updateInputFile, sourceConnection, updateContactLog, updateStartLine, performUpdate: false);
            break;

        case "X":
            Console.WriteLine("*** WARNING: Running CREATE in LIVE mode - Records WILL be inserted ***");
            Console.Write("Are you sure? (Y/N): ");
            var confirmCreate = Console.ReadLine()?.Trim().ToUpper();
            if (confirmCreate == "Y")
            {
                Console.WriteLine();
                CreateContactsVision(createInputFile, sourceConnection, createContactLog, createStartLine, performUpdate: true);
            }
            else
            {
                Console.WriteLine("Operation cancelled.");
            }
            break;

        case "Y":
            Console.WriteLine("*** WARNING: Running UPDATE in LIVE mode - Records WILL be updated ***");
            Console.Write("Are you sure? (Y/N): ");
            var confirmUpdate = Console.ReadLine()?.Trim().ToUpper();
            if (confirmUpdate == "Y")
            {
                Console.WriteLine();
                UpdateContactsVision(updateInputFile, sourceConnection, updateContactLog, updateStartLine, performUpdate: true);
            }
            else
            {
                Console.WriteLine("Operation cancelled.");
            }
            break;

        case "Q":
            Console.WriteLine("Exiting...");
            continueRunning = false;
            break;

        default:
            Console.WriteLine("Invalid option. Please try again.");
            break;
    }

    if (continueRunning)
    {
        Console.WriteLine();
        Console.WriteLine("Press any key to return to menu...");
        Console.ReadKey();
        Console.Clear();
    }
}

Console.WriteLine();
Console.WriteLine("Thank you. Goodbye!");

// Helper method to convert empty strings to null
static string? GetNullableString(string value)
{
    var trimmed = value.Trim();
    return string.IsNullOrWhiteSpace(trimmed) ? null : trimmed;
}

// Helper method to parse nullable int
static int? GetNullableInt(string value)
{
    var trimmed = value.Trim();
    if (string.IsNullOrWhiteSpace(trimmed))
        return null;
    return int.TryParse(trimmed, out int result) ? result : null;
}


static void CreateContactsVision(string? inputFile, string? connectionString, string? logFile, int startLine = 1, bool performUpdate = false)
{
    if (string.IsNullOrEmpty(inputFile))
    {
        Console.WriteLine("Input file is not configured.");
        return;
    }
    if (!File.Exists(inputFile))
    {
        Console.WriteLine($"Input file not found: {inputFile}");
        return;
    }
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    if (string.IsNullOrEmpty(logFile))
    {
        Console.WriteLine("Log file is not configured.");
        return;
    }
    try
    {
        // Define expected number of fields in the input file
        const int expectedFieldCount = 18;

        using var logWriter = new StreamWriter(logFile, append: true);

        var modeMessage = performUpdate ? "INSERT MODE - Records will be created" : "DRY RUN MODE - No records will be created";
        var header1 = $"Reading users from {inputFile}: ({modeMessage})";
        var separator = "".PadRight(130, '-');
        var header2 = $"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5} {"Email",-30} {"Exists",-10}";

        // Write to console
        Console.WriteLine(header1);
        Console.WriteLine(separator);
        Console.WriteLine(header2);
        Console.WriteLine(separator);

        // Write to log file with timestamp
        logWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {header1}");
        logWriter.WriteLine(separator);
        logWriter.WriteLine(header2);
        logWriter.WriteLine(separator);

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        using var reader = new StreamReader(inputFile);
        string? line;
        int lineNumber = 0;
        int insertedCount = 0;
        int alreadyExistsCount = 0;
        int skippedCount = 0;

        while ((line = reader.ReadLine()) != null)
        {
            lineNumber++;

            // Skip lines before the start line (e.g., header rows)
            if (lineNumber < startLine)
                continue;

            // Skip empty lines
            if (string.IsNullOrWhiteSpace(line))
                continue;

            // Split the line by comma
            var values = line.Split(';');

            if (values.Length < expectedFieldCount)
            {
                var skipMessage = $"Line {lineNumber}: Skipped - Invalid format (expected {expectedFieldCount} values, got {values.Length})";
                Console.WriteLine(skipMessage);
                logWriter.WriteLine(skipMessage);
                skippedCount++;
                continue;
            }

            try
            {
                // Validate email address if specified
                var emailAddress = values[10].Trim();
                if (!string.IsNullOrEmpty(emailAddress))
                {
                    var emailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";
                    if (!Regex.IsMatch(emailAddress, emailPattern))
                    {
                        var invalidEmailMessage = $"Line {lineNumber}: Skipped - EmailAddress '{emailAddress}' is not a valid email address";
                        Console.WriteLine(invalidEmailMessage);
                        logWriter.WriteLine(invalidEmailMessage);
                        skippedCount ++;
                        continue;
                    }
                }

                // Validate CompanyNum is a valid integer > 0
                if (!int.TryParse(values[31].Trim(), out int companyNum) || companyNum <= 0)
                {
                    var invalidCompanyNumMessage = $"Line {lineNumber}: Skipped - CompanyNum '{values[31].Trim()}' must be a valid integer greater than 0";
                    Console.WriteLine(invalidCompanyNumMessage);
                    logWriter.WriteLine(invalidCompanyNumMessage);
                    skippedCount ++ ;
                    continue;
                }

                // Retrieve AddressGUID from dbo.tAddress (required for all records)
                var addressCommand = new SqlCommand(
                    """
                      SELECT TOP 1
                        AddressGUID 
                      FROM 
                        dbo.tAddress 
                      WHERE 
                        SubscriberID = @SubscriberID AND 
                        CompanyNum = @CompanyNum 
                      ORDER BY
                        CASE
                          WHEN AddressClass = 'MAIL' THEN 1
                          WHEN AddressClass = 'HOME' THEN 2
                          ELSE 3
                        END
                    """,
                    connection);
                addressCommand.Parameters.AddWithValue("@SubscriberID", 8000);
                addressCommand.Parameters.AddWithValue("@CompanyNum", companyNum);

                var result = addressCommand.ExecuteScalar();
                //if (result == null || result == DBNull.Value)
                //{
                //    var noAddressMessage = $"Line {lineNumber}: Skipped - AddressGUID not found for CompanyNum '{companyNum}'";
                //    Console.WriteLine(noAddressMessage);
                //    logWriter.WriteLine(noAddressMessage);
                //    skippedCount ++;
                //    continue;
                //}

                //Guid workAddressGUID = (Guid)(result ?? Guid.Empty);
                Guid? workAddressGUID = result is Guid guid ? guid :
                       (result != null && result != DBNull.Value && Guid.TryParse(result.ToString(), out var parsed))
                       ? parsed : null;

                // Get the next UserNum from the sequence only when actually inserting
                int userNum = 0;
                if (performUpdate)
                {
                    var sequenceCommand = new SqlCommand("SELECT NEXT VALUE FOR sub8000.seqUserNum", connection);
                    userNum = (int)sequenceCommand.ExecuteScalar();
                }

                // Parse values into VisionUserModel with UserNum from sequence
                var user = new VisionUserModel
                {
                    SubscriberID = 8000,
                    UserNum = userNum,  // Set from sequence if performUpdate is true, otherwise 0
                    UserRoleID = GetNullableInt(values[24]),
                    FirstName = GetNullableString(values[8]),
                    MiddleInitial = GetNullableString(values[25]),
                    LastName = GetNullableString(values[9]),
                    //Generation = GetNullableString(values[6]),
                    Salutation = GetNullableString(values[26]),
                    Birthdate = values[27].IsNullOrEmpty() ? null : DateTime.Parse(values[27].Trim()),
                    LanguageCode = GetNullableString(values[28]),
                    EmailAddress = emailAddress,
                    Phone = GetNullableString(values[11]),
                    Phone2 = GetNullableString(values[12]),
                    Phone3 = GetNullableString(values[29]),
                    Fax = GetNullableString(values[30]),
                    //CompanyRole = GetNullableInt(values[15]),
                    //Title = GetNullableString(values[16]),
                    CompanyNum = companyNum
                };

                // Display the parsed user (UserNum shows actual value in update mode, TBD in dry run mode)
                var userNumDisplay = performUpdate ? user.UserNum.ToString() : "TBD";
                var userLine = $"{user.SubscriberID,-10} {userNumDisplay,-10} {user.FirstName,-25} {user.LastName,-25} {user.EmailAddress,-30} {"N/A",-10}";
                Console.WriteLine(userLine);
                logWriter.WriteLine(userLine);

                // Only process insert if performUpdate is true
                if (performUpdate)
                {

                    // Check if ID exists in database (should not exist for new records)
                    var checkCommand = new SqlCommand("SELECT COUNT(*) FROM dbo.tUser WHERE SubscriberID = @SubscriberID AND UserNum = @UserNum", connection);
                    checkCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
                    checkCommand.Parameters.AddWithValue("@UserNum", user.UserNum);
                    var count = (int)checkCommand.ExecuteScalar();

                    if (count == 0)
                    {
                        string userName = $"{user.SubscriberID}-{user.UserNum}";
                        // Begin transaction to ensure both inserts succeed or both fail
                        using var transaction = connection.BeginTransaction();
                        try
                        {
                            var insertCommand = new SqlCommand(
                                """
                                INSERT INTO dbo.tUser 
                                (
                                  SubscriberID, 
                                  UserNum, 
                                  UserName,
                                  UserRoleID, 
                                  FirstName, 
                                  MiddleInitial, 
                                  LastName, 
                                  Salutation, 
                                  Birthdate, 
                                  LanguageCode, 
                                  EmailAddress, 
                                  Phone, 
                                  Phone2, 
                                  Phone3, 
                                  Fax, 
                                  CompanyNum,
                                  Active,
                                  WorkAddressGUID
                                )
                                VALUES 
                                (
                                  @SubscriberID, 
                                  @UserNum,
                                  @UserName,
                                  @UserRoleID, 
                                  @FirstName, 
                                  @MiddleInitial, 
                                  @LastName, 
                                  @Salutation, 
                                  @Birthdate, 
                                  @LanguageCode, 
                                  @EmailAddress, 
                                  @Phone, 
                                  @Phone2, 
                                  @Phone3, 
                                  @Fax, 
                                  @CompanyNum,
                                  @Active,
                                  @WorkAddressGUID
                                )
                                """,
                                connection,
                                transaction);
                            insertCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
                            insertCommand.Parameters.AddWithValue("@UserNum", user.UserNum);
                            insertCommand.Parameters.AddWithValue("@UserName", userName);
                            insertCommand.Parameters.AddWithValue("@UserRoleID", (object?)user.UserRoleID ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@FirstName", (object?)user.FirstName ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@MiddleInitial", (object?)user.MiddleInitial ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@LastName", (object?)user.LastName ?? DBNull.Value);
                            //insertCommand.Parameters.AddWithValue("@Generation", (object?)user.Generation ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@Salutation", (object?)user.Salutation ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@Birthdate", (object?)user.Birthdate ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@LanguageCode", (object?)user.LanguageCode ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@EmailAddress", (object?)user.EmailAddress ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@Phone", (object?)user.Phone ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@Phone2", (object?)user.Phone2 ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@Phone3", (object?)user.Phone3 ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@Fax", (object?)user.Fax ?? DBNull.Value);
                            //insertCommand.Parameters.AddWithValue("@CompanyRole", (object?)user.CompanyRole ?? DBNull.Value);
                            //insertCommand.Parameters.AddWithValue("@Title", (object?)user.Title ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@CompanyNum", user.CompanyNum);
                            insertCommand.Parameters.AddWithValue("@Active", 1);
                            insertCommand.Parameters.AddWithValue("@WorkAddressGUID", (object?)workAddressGUID ?? DBNull.Value);

                            var rowsAffected = insertCommand.ExecuteNonQuery();

                            // Insert into sub8000.tUser8000 table
                            var insertUser8000Command = new SqlCommand(
                                "INSERT INTO sub8000.tUser8000 (SubscriberID, UserNum) VALUES (@SubscriberID, @UserNum)",
                                connection,
                                transaction);
                            insertUser8000Command.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
                            insertUser8000Command.Parameters.AddWithValue("@UserNum", user.UserNum);
                            insertUser8000Command.ExecuteNonQuery();

                            // Commit transaction if both inserts succeeded
                            transaction.Commit();

                            var insertMessage = $"   --> UserNum {user.UserNum} inserted successfully. Rows affected: {rowsAffected}";
                            Console.WriteLine(insertMessage);
                            logWriter.WriteLine(insertMessage);
                            insertedCount++;
                        }
                        catch (Exception ex)
                        {
                            // Rollback transaction if either insert failed
                            transaction.Rollback();
                            var errorMessage = $"   --> UserNum {user.UserNum} insert failed. Transaction rolled back. Error: {ex.Message}";
                            Console.WriteLine(errorMessage);
                            logWriter.WriteLine(errorMessage);
                        }
                    }
                    else
                    {
                        // Display message if ID already exists (should be rare with sequence)
                        var alreadyExistsMessage = $"   --> UserNum {user.UserNum} already exists in database. Skipping...";
                        Console.WriteLine(alreadyExistsMessage);
                        logWriter.WriteLine(alreadyExistsMessage);
                        alreadyExistsCount++;
                    }
                }
                else
                {
                    // Dry run mode - just count what would be inserted
                    var dryRunMessage = $"   --> Record would be inserted with auto-generated UserNum (DRY RUN - no changes made)";
                    Console.WriteLine(dryRunMessage);
                    logWriter.WriteLine(dryRunMessage);
                    insertedCount++;
                }
            }
            catch (FormatException)
            {
                var formatErrorMessage = $"Line {lineNumber}: Skipped - Invalid data format";
                Console.WriteLine(formatErrorMessage);
                logWriter.WriteLine(formatErrorMessage);
            }
        }

        var summary1 = "Summary:";
        var summary2 = $"  Total records {(performUpdate ? "inserted" : "that would be inserted")}: {insertedCount}";
        var summary3 = $"  Total records that already exist: {alreadyExistsCount}";
        var summary4 = $"  Total records skipped due to errors: {skippedCount}";

        // Write to console
        Console.WriteLine(separator);
        Console.WriteLine(summary1);
        Console.WriteLine(summary2);
        Console.WriteLine(summary3);
        Console.WriteLine(summary4);
        Console.WriteLine(separator);

        // Write to log file
        logWriter.WriteLine(separator);
        logWriter.WriteLine(summary1);
        logWriter.WriteLine(summary2);
        logWriter.WriteLine(summary3);
        logWriter.WriteLine(summary4);
        logWriter.WriteLine(separator);
        logWriter.WriteLine(); // Add blank line for readability between runs
    }
    catch (Exception ex)
    {
        var errorMessage = $"Error reading input file: {ex.Message}";
        Console.WriteLine(errorMessage);

        // Try to log error to file if possible
        try
        {
            using var errorWriter = new StreamWriter(logFile!, append: true);
            errorWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {errorMessage}");
        }
        catch
        {
            // If we can't write to log file, just continue
        }
    }
}


static void UpdateContactsVision(string? inputFile, string? connectionString, string? logFile, int startLine = 1, bool performUpdate = false)
{
    if (string.IsNullOrEmpty(inputFile))
    {
        Console.WriteLine("Input file is not configured.");
        return;
    }
    if (!File.Exists(inputFile))
    {
        Console.WriteLine($"Input file not found: {inputFile}");
        return;
    }
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    if (string.IsNullOrEmpty(logFile))
    {
        Console.WriteLine("Log file is not configured.");
        return;
    }
    try
    {
        // Define expected number of fields in the input file
        const int expectedFieldCount = 18;

        using var logWriter = new StreamWriter(logFile, append: true);

        var modeMessage = performUpdate ? "UPDATE MODE - Changes will be applied" : "DRY RUN MODE - No changes will be applied";
        var header1 = $"Reading users from {inputFile}: ({modeMessage})";
        var separator = "".PadRight(130, '-');
        var header2 = $"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5} {"Email",-30} {"Exists",-10}";

        // Write to console
        Console.WriteLine(header1);
        Console.WriteLine(separator);
        Console.WriteLine(header2);
        Console.WriteLine(separator);

        // Write to log file with timestamp
        logWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {header1}");
        logWriter.WriteLine(separator);
        logWriter.WriteLine(header2);
        logWriter.WriteLine(separator);

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        using var reader = new StreamReader(inputFile);
        string? line;
        int lineNumber = 0;
        int updatedCount = 0;
        int nonExistingCount = 0;

        while ((line = reader.ReadLine()) != null)
        {
            lineNumber++;

            // Skip lines before the start line (e.g., header rows)
            if (lineNumber < startLine)
                continue;

            // Skip empty lines
            if (string.IsNullOrWhiteSpace(line))
                continue;

            // Split the line by comma
            var values = line.Split(';');

            if (values.Length < expectedFieldCount)
            {
                var skipMessage = $"Line {lineNumber}: Skipped - Invalid format (expected {expectedFieldCount} values, got {values.Length})";
                Console.WriteLine(skipMessage);
                logWriter.WriteLine(skipMessage);
                continue;
            }

            try
            {
                // Validate that UserNum is a valid integer before parsing
                if (!int.TryParse(values[3].Trim(), out int userNum))
                {
                    var invalidUserNumMessage = $"Line {lineNumber}: Skipped - UserNum '{values[1].Trim()}' is not a valid integer";
                    Console.WriteLine(invalidUserNumMessage);
                    logWriter.WriteLine(invalidUserNumMessage);
                    continue;
                }

                // Validate email address if specified
                var emailAddress = values[10].Trim();
                if (!string.IsNullOrEmpty(emailAddress))
                {
                    var emailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";
                    if (!Regex.IsMatch(emailAddress, emailPattern))
                    {
                        var invalidEmailMessage = $"Line {lineNumber}: Skipped - EmailAddress '{emailAddress}' is not a valid email address";
                        Console.WriteLine(invalidEmailMessage);
                        logWriter.WriteLine(invalidEmailMessage);
                        continue;
                    }
                }

                // Validate CompanyNum is a valid integer > 0
                if (!int.TryParse(values[31].Trim(), out int companyNum) || companyNum <= 0)
                {
                    var invalidCompanyNumMessage = $"Line {lineNumber}: Skipped - CompanyNum '{values[31].Trim()}' must be a valid integer greater than 0";
                    Console.WriteLine(invalidCompanyNumMessage);
                    logWriter.WriteLine(invalidCompanyNumMessage);
                    continue;
                }

                // Parse values into VisionUserModel
                var user = new VisionUserModel
                {
                    //SubscriberID = int.Parse(values[0].Trim()),
                    SubscriberID = 8000,
                    UserNum = userNum,
                    UserRoleID = GetNullableInt(values[24]),
                    FirstName = GetNullableString(values[8]),
                    MiddleInitial = GetNullableString(values[25]),
                    LastName = GetNullableString(values[9]),
                    //Generation = GetNullableString(values[6]),
                    Salutation = GetNullableString(values[26]),
                    Birthdate = values[27].IsNullOrEmpty() ? null : DateTime.Parse(values[27].Trim()),
                    LanguageCode = GetNullableString(values[28]),
                    EmailAddress = emailAddress,
                    Phone = GetNullableString(values[11]),
                    Phone2 = GetNullableString(values[12]),
                    Phone3 = GetNullableString(values[29]),
                    Fax = GetNullableString(values[30]),
                    //CompanyRole = GetNullableInt(values[24]),
                    //Title = GetNullableString(values[16]),
                    CompanyNum = companyNum
                };

                // Check if ID exists in database
                var checkCommand = new SqlCommand("SELECT COUNT(*) FROM dbo.tUser WHERE SubscriberID = @SubscriberID AND UserNum = @UserNum", connection);
                checkCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
                checkCommand.Parameters.AddWithValue("@UserNum", user.UserNum);
                var count = (int)checkCommand.ExecuteScalar();
                var existsInDb = count > 0 ? "Yes" : "No";

                // Display the parsed user with existence status
                var userLine = $"{user.SubscriberID,-10} {user.UserNum,-10} {user.FirstName,-25} {user.LastName,-25} {user.EmailAddress,-30} {existsInDb,-10}";
                Console.WriteLine(userLine);
                logWriter.WriteLine(userLine);

                // If ID exists, update the record (only if performUpdate is true)
                if (count > 0)
                {
                    if (performUpdate)
                    {
                        var updateCommand = new SqlCommand(
                            """
                            Update dbo.tUser SET
                              UserRoleID = @UserRoleID,
                              FirstName = @FirstName,
                              MiddleInitial = @MiddleInitial,
                              LastName = @LastName,
                              Salutation = @Salutation,
                              Birthdate = @Birthdate,
                              LanguageCode = @LanguageCode,
                              EmailAddress = @EmailAddress,
                              Phone = @Phone,
                              Phone2 = @Phone2,
                              Phone3 = @Phone3,
                              Fax = @Fax,
                              CompanyNum = @CompanyNum,
                              Active = @Active
                            WHERE
                              SubscriberID = @SubscriberID AND 
                              UserNum = @UserNum                          
                            """,
                            connection);
                        updateCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
                        updateCommand.Parameters.AddWithValue("@UserNum", user.UserNum);

                        updateCommand.Parameters.AddWithValue("@UserRoleID", (object?)user.UserRoleID ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@FirstName", (object?)user.FirstName ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@MiddleInitial", (object?)user.MiddleInitial ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@LastName", (object?)user.LastName ?? DBNull.Value);
                        //updateCommand.Parameters.AddWithValue("@Generation", (object?)user.Generation ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@Salutation", (object?)user.Salutation ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@Birthdate", (object?)user.Birthdate ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@LanguageCode", (object?)user.LanguageCode ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@EmailAddress", (object?)user.EmailAddress ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@Phone", (object?)user.Phone ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@Phone2", (object?)user.Phone2 ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@Phone3", (object?)user.Phone3 ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@Fax", (object?)user.Fax ?? DBNull.Value);
                        //updateCommand.Parameters.AddWithValue("@CompanyRole", (object?)user.CompanyRole ?? DBNull.Value);
                        //updateCommand.Parameters.AddWithValue("@Title", (object?)user.Title ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@CompanyNum", user.CompanyNum);
                        updateCommand.Parameters.AddWithValue("@Active", 1);

                        var rowsAffected = updateCommand.ExecuteNonQuery();
                        var updateMessage = $"   --> UserNum {user.UserNum} updated successfully. Rows affected: {rowsAffected}";
                        Console.WriteLine(updateMessage);
                        logWriter.WriteLine(updateMessage);
                    }
                    else
                    {
                        var dryRunMessage = $"   --> UserNum {user.UserNum} would be updated (DRY RUN - no changes made)";
                        Console.WriteLine(dryRunMessage);
                        logWriter.WriteLine(dryRunMessage);
                    }
                    updatedCount++;
                }
                else
                {
                    // Display message if ID doesn't exist and continue
                    var notExistsMessage = $"   --> UserNum {user.UserNum} does not exist in database. Continuing...";
                    Console.WriteLine(notExistsMessage);
                    logWriter.WriteLine(notExistsMessage);
                    nonExistingCount++;
                }
            }
            catch (FormatException)
            {
                var formatErrorMessage = $"Line {lineNumber}: Skipped - Invalid data format";
                Console.WriteLine(formatErrorMessage);
                logWriter.WriteLine(formatErrorMessage);
            }
        }

        var summary1 = "Summary:";
        var summary2 = $"  Total records {(performUpdate ? "updated" : "that would be updated")}: {updatedCount}";
        var summary3 = $"  Total non-existing records: {nonExistingCount}";

        // Write to console
        Console.WriteLine(separator);
        Console.WriteLine(summary1);
        Console.WriteLine(summary2);
        Console.WriteLine(summary3);
        Console.WriteLine(separator);

        // Write to log file
        logWriter.WriteLine(separator);
        logWriter.WriteLine(summary1);
        logWriter.WriteLine(summary2);
        logWriter.WriteLine(summary3);
        logWriter.WriteLine(separator);
        logWriter.WriteLine(); // Add blank line for readability between runs
    }
    catch (Exception ex)
    {
        var errorMessage = $"Error reading input file: {ex.Message}";
        Console.WriteLine(errorMessage);

        // Try to log error to file if possible
        try
        {
            using var errorWriter = new StreamWriter(logFile!, append: true);
            errorWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {errorMessage}");
        }
        catch
        {
            // If we can't write to log file, just continue
        }
    }
}

public struct VisionUserModel
{
    public int SubscriberID { get; set; }
    public int UserNum { get; set; }
    public int? UserRoleID { get; set; }
    public string? FirstName { get; set; }
    public string? MiddleInitial { get; set; }
    public string? LastName { get; set; }
    public string? Generation { get; set; }
    public string? Salutation { get; set; }
    public DateTime? Birthdate { get; set; }
    public string? LanguageCode { get; set; }
    public string? EmailAddress { get; set; }
    public string? Phone { get; set; }
    public string? Phone2 { get; set; }
    public string? Phone3 { get; set; }
    public string? Fax { get; set; }
    //public string SSN { get; set; }
    public int? CompanyRole { get; set; }
    public string? Title { get; set; }
    public int CompanyNum { get; set; }
}





