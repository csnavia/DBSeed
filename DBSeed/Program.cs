using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

// Build configuration
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

// Read connection strings
var sourceConnection = configuration.GetConnectionString("SourceConnection");
var destinationConnection = configuration.GetConnectionString("DestinationConnection");
var environmentLabel = configuration["EnvironmentLabel"];
var createInputFile = configuration["CreateInputFile"];
var updateInputFile = configuration["UpdateInputFile"];
var performUpdate = bool.Parse(configuration["UpdateDatabase"] ?? "false");
var createStartLine = int.Parse(configuration["CreateStartLine"] ?? "1");
var updateStartLine = int.Parse(configuration["UpdateStartLine"] ?? "1");

var createContactLog = configuration["CreateContactLog"];
var updateContactLog = configuration["UpdateContactLog"];
//var logFile = configuration["LogFile"];

Console.WriteLine($"Environment: {Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}");
Console.WriteLine($"Environment Label: {environmentLabel}");        
Console.WriteLine($"Source Connection: {sourceConnection}");
Console.WriteLine($"Destination Connection: {destinationConnection}");
Console.WriteLine($"Update Database: {performUpdate}");
//Console.WriteLine($"Log File: {logFile}");
Console.WriteLine();



UpdateContactsVision(updateInputFile, sourceConnection, updateContactLog, updateStartLine, performUpdate: performUpdate);

//CreateContactsVision(createInputFile, sourceConnection, createContactLog, createStartLine, performUpdate: performUpdate);

Console.ReadKey();

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
            var values = line.Split(',');

            if (values.Length < expectedFieldCount)
            {
                var skipMessage = $"Line {lineNumber}: Skipped - Invalid format (expected {expectedFieldCount} values, got {values.Length})";
                Console.WriteLine(skipMessage);
                logWriter.WriteLine(skipMessage);
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
                        continue;
                    }
                }

                // Validate CompanyNum is a valid integer > 0
                if (!int.TryParse(values[17].Trim(), out int companyNum) || companyNum <= 0)
                {
                    var invalidCompanyNumMessage = $"Line {lineNumber}: Skipped - CompanyNum '{values[17].Trim()}' must be a valid integer greater than 0";
                    Console.WriteLine(invalidCompanyNumMessage);
                    logWriter.WriteLine(invalidCompanyNumMessage);
                    continue;
                }

                // Retrieve AddressGUID from dbo.tAddress (required for all records)
                var addressCommand = new SqlCommand(
                    """
                      SELECT 
                        AddressGUID 
                      FROM 
                        dbo.tAddress 
                      WHERE 
                        SubscriberID = @SubscriberID AND 
                        CompanyNum = @CompanyNum AND 
                        AddressClass = @AddressClass                    
                    """,
                    connection);
                addressCommand.Parameters.AddWithValue("@SubscriberID", 8000);
                addressCommand.Parameters.AddWithValue("@CompanyNum", companyNum);
                addressCommand.Parameters.AddWithValue("@AddressClass", "MAIL");

                var result = addressCommand.ExecuteScalar();
                if (result == null || result == DBNull.Value)
                {
                    var noAddressMessage = $"Line {lineNumber}: Skipped - AddressGUID not found for CompanyNum '{companyNum}' with AddressClass 'MAIL'";
                    Console.WriteLine(noAddressMessage);
                    logWriter.WriteLine(noAddressMessage);
                    continue;
                }
                Guid workAddressGUID = (Guid)result;

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
                    UserRoleID = GetNullableInt(values[2]),
                    FirstName = GetNullableString(values[3]),
                    MiddleInitial = GetNullableString(values[4]),
                    LastName = GetNullableString(values[5]),
                    Generation = GetNullableString(values[6]),
                    Salutation = GetNullableString(values[7]),
                    Birthdate = values[8].IsNullOrEmpty() ? null : DateTime.Parse(values[8].Trim()),
                    LanguageCode = GetNullableString(values[9]),
                    EmailAddress = emailAddress,
                    Phone = GetNullableString(values[11]),
                    Phone2 = GetNullableString(values[12]),
                    Phone3 = GetNullableString(values[13]),
                    Fax = GetNullableString(values[14]),
                    CompanyRole = GetNullableInt(values[15]),
                    Title = GetNullableString(values[16]),
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
                                  Generation, 
                                  Salutation, 
                                  Birthdate, 
                                  LanguageCode, 
                                  EmailAddress, 
                                  Phone, 
                                  Phone2, 
                                  Phone3, 
                                  Fax, 
                                  CompanyRole, 
                                  Title,
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
                                  @Generation, 
                                  @Salutation, 
                                  @Birthdate, 
                                  @LanguageCode, 
                                  @EmailAddress, 
                                  @Phone, 
                                  @Phone2, 
                                  @Phone3, 
                                  @Fax, 
                                  @CompanyRole, 
                                  @Title,
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
                            insertCommand.Parameters.AddWithValue("@UserRoleID", user.UserRoleID);
                            insertCommand.Parameters.AddWithValue("@FirstName", user.FirstName);
                            insertCommand.Parameters.AddWithValue("@MiddleInitial", user.MiddleInitial);
                            insertCommand.Parameters.AddWithValue("@LastName", user.LastName);
                            insertCommand.Parameters.AddWithValue("@Generation", user.Generation);
                            insertCommand.Parameters.AddWithValue("@Salutation", user.Salutation);
                            insertCommand.Parameters.AddWithValue("@Birthdate", (object?)user.Birthdate ?? DBNull.Value);
                            insertCommand.Parameters.AddWithValue("@LanguageCode", user.LanguageCode);
                            insertCommand.Parameters.AddWithValue("@EmailAddress", user.EmailAddress);
                            insertCommand.Parameters.AddWithValue("@Phone", user.Phone);
                            insertCommand.Parameters.AddWithValue("@Phone2", user.Phone2);
                            insertCommand.Parameters.AddWithValue("@Phone3", user.Phone3);
                            insertCommand.Parameters.AddWithValue("@Fax", user.Fax);
                            insertCommand.Parameters.AddWithValue("@CompanyRole", user.CompanyRole);
                            insertCommand.Parameters.AddWithValue("@Title", user.Title);
                            insertCommand.Parameters.AddWithValue("@CompanyNum", user.CompanyNum);
                            insertCommand.Parameters.AddWithValue("@Active", 1);
                            insertCommand.Parameters.AddWithValue("@WorkAddressGUID", workAddressGUID);

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
            var values = line.Split(',');

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
                if (!int.TryParse(values[1].Trim(), out int userNum))
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
                if (!int.TryParse(values[17].Trim(), out int companyNum) || companyNum <= 0)
                {
                    var invalidCompanyNumMessage = $"Line {lineNumber}: Skipped - CompanyNum '{values[17].Trim()}' must be a valid integer greater than 0";
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
                    UserRoleID = GetNullableInt(values[2]),
                    FirstName = GetNullableString(values[3]),
                    MiddleInitial = GetNullableString(values[4]),
                    LastName = GetNullableString(values[5]),
                    Generation = GetNullableString(values[6]),
                    Salutation = GetNullableString(values[7]),
                    Birthdate = values[8].IsNullOrEmpty() ? null : DateTime.Parse(values[8].Trim()),
                    LanguageCode = GetNullableString(values[9]),
                    EmailAddress = emailAddress,
                    Phone = GetNullableString(values[11]),
                    Phone2 = GetNullableString(values[12]),
                    Phone3 = GetNullableString(values[13]),
                    Fax = GetNullableString(values[14]),
                    CompanyRole = GetNullableInt(values[15]),
                    Title = GetNullableString(values[16]),
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
                              Generation = @Generation,
                              Salutation = @Salutation,
                              Birthdate = @Birthdate,
                              LanguageCode = @LanguageCode,
                              EmailAddress = @EmailAddress,
                              Phone = @Phone,
                              Phone2 = @Phone2,
                              Phone3 = @Phone3,
                              Fax = @Fax,
                              CompanyRole = @CompanyRole,
                              Title = @Title,
                              CompanyNum = @CompanyNum,
                              Active = @Active
                            WHERE
                              SubscriberID = @SubscriberID AND 
                              UserNum = @UserNum                          
                            """,
                            connection);
                        updateCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
                        updateCommand.Parameters.AddWithValue("@UserNum", user.UserNum);

                        updateCommand.Parameters.AddWithValue("@UserRoleID", user.UserRoleID);
                        updateCommand.Parameters.AddWithValue("@FirstName", user.FirstName);
                        updateCommand.Parameters.AddWithValue("@MiddleInitial", user.MiddleInitial);
                        updateCommand.Parameters.AddWithValue("@LastName", user.LastName);
                        updateCommand.Parameters.AddWithValue("@Generation", user.Generation);
                        updateCommand.Parameters.AddWithValue("@Salutation", user.Salutation);
                        updateCommand.Parameters.AddWithValue("@Birthdate", (object?)user.Birthdate ?? DBNull.Value);
                        updateCommand.Parameters.AddWithValue("@LanguageCode", user.LanguageCode);
                        updateCommand.Parameters.AddWithValue("@EmailAddress", user.EmailAddress);
                        updateCommand.Parameters.AddWithValue("@Phone", user.Phone);
                        updateCommand.Parameters.AddWithValue("@Phone2", user.Phone2);
                        updateCommand.Parameters.AddWithValue("@Phone3", user.Phone3);
                        updateCommand.Parameters.AddWithValue("@Fax", user.Fax);
                        updateCommand.Parameters.AddWithValue("@CompanyRole", user.CompanyRole);
                        updateCommand.Parameters.AddWithValue("@Title", user.Title);
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


//static void ProcessInputFileLines(string? inputFile, string? connectionString, string? logFile, bool performUpdate = false)
//{
//    if (string.IsNullOrEmpty(inputFile))
//    {
//        Console.WriteLine("Input file is not configured.");
//        return;
//    }
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    if (string.IsNullOrEmpty(logFile))
//    {
//        Console.WriteLine("Log file is not configured.");
//        return;
//    }
//    try
//    {
//        // Define expected number of fields in the input file
//        const int expectedFieldCount = 5;

//        using var logWriter = new StreamWriter(logFile, append: true);

//        var modeMessage = performUpdate ? "UPDATE MODE - Changes will be applied" : "DRY RUN MODE - No changes will be applied";
//        var header1 = $"Reading users from {inputFile}: ({modeMessage})";
//        var separator = "".PadRight(130, '-');
//        var header2 = $"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5} {"Email",-30} {"Exists",-10}";

//        // Write to console
//        Console.WriteLine(header1);
//        Console.WriteLine(separator);
//        Console.WriteLine(header2);
//        Console.WriteLine(separator);

//        // Write to log file with timestamp
//        logWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {header1}");
//        logWriter.WriteLine(separator);
//        logWriter.WriteLine(header2);
//        logWriter.WriteLine(separator);

//        using var connection = new SqlConnection(connectionString);
//        connection.Open();

//        using var reader = new StreamReader(inputFile);
//        string? line;
//        int lineNumber = 0;
//        int updatedCount = 0;
//        int nonExistingCount = 0;

//        while ((line = reader.ReadLine()) != null)
//        {
//            lineNumber++;

//            // Skip empty lines
//            if (string.IsNullOrWhiteSpace(line))
//                continue;

//            // Split the line by comma
//            var values = line.Split(',');

//            // Ensure we have enough values (expecting: ID,FirstName,LastName,Age,Email)
//            if (values.Length < expectedFieldCount)
//            {
//                var skipMessage = $"Line {lineNumber}: Skipped - Invalid format (expected {expectedFieldCount} values, got {values.Length})";
//                Console.WriteLine(skipMessage);
//                logWriter.WriteLine(skipMessage);
//                continue;
//            }

//            try
//            {
//                // Parse values into UserModel
//                var user = new UserModel
//                {
//                    ID = int.Parse(values[0].Trim()),
//                    FirstName = values[1].Trim(),
//                    LastName = values[2].Trim(),
//                    Age = int.Parse(values[3].Trim()),
//                    Email = values[4].Trim()
//                };

//                // Check if ID exists in database
//                var checkCommand = new SqlCommand("SELECT COUNT(*) FROM User1 WHERE ID = @ID", connection);
//                checkCommand.Parameters.AddWithValue("@ID", user.ID);
//                var count = (int)checkCommand.ExecuteScalar();
//                var existsInDb = count > 0 ? "Yes" : "No";

//                // Display the parsed user with existence status
//                var userLine = $"{user.ID,-10} {user.FirstName,-25} {user.LastName,-25} {user.Age,-5} {user.Email,-30} {existsInDb,-10}";
//                Console.WriteLine(userLine);
//                logWriter.WriteLine(userLine);

//                // If ID exists, update the record (only if performUpdate is true)
//                if (count > 0)
//                {
//                    if (performUpdate)
//                    {
//                        var updateCommand = new SqlCommand(
//                            "UPDATE User1 SET FirstName = @FirstName, LastName = @LastName, Age = @Age, Email = @Email WHERE ID = @ID",
//                            connection);
//                        updateCommand.Parameters.AddWithValue("@ID", user.ID);
//                        updateCommand.Parameters.AddWithValue("@FirstName", user.FirstName);
//                        updateCommand.Parameters.AddWithValue("@LastName", user.LastName);
//                        updateCommand.Parameters.AddWithValue("@Age", user.Age);
//                        updateCommand.Parameters.AddWithValue("@Email", user.Email);

//                        var rowsAffected = updateCommand.ExecuteNonQuery();
//                        var updateMessage = $"   --> ID {user.ID} updated successfully. Rows affected: {rowsAffected}";
//                        Console.WriteLine(updateMessage);
//                        logWriter.WriteLine(updateMessage);
//                    }
//                    else
//                    {
//                        var dryRunMessage = $"   --> ID {user.ID} would be updated (DRY RUN - no changes made)";
//                        Console.WriteLine(dryRunMessage);
//                        logWriter.WriteLine(dryRunMessage);
//                    }
//                    updatedCount++;
//                }
//                else
//                {
//                    // Display message if ID doesn't exist and continue
//                    var notExistsMessage = $"   --> ID {user.ID} does not exist in database. Continuing...";
//                    Console.WriteLine(notExistsMessage);
//                    logWriter.WriteLine(notExistsMessage);
//                    nonExistingCount++;
//                }
//            }
//            catch (FormatException)
//            {
//                var formatErrorMessage = $"Line {lineNumber}: Skipped - Invalid data format";
//                Console.WriteLine(formatErrorMessage);
//                logWriter.WriteLine(formatErrorMessage);
//            }
//        }

//        var summary1 = "Summary:";
//        var summary2 = $"  Total records {(performUpdate ? "updated" : "that would be updated")}: {updatedCount}";
//        var summary3 = $"  Total non-existing records: {nonExistingCount}";

//        // Write to console
//        Console.WriteLine(separator);
//        Console.WriteLine(summary1);
//        Console.WriteLine(summary2);
//        Console.WriteLine(summary3);
//        Console.WriteLine(separator);

//        // Write to log file
//        logWriter.WriteLine(separator);
//        logWriter.WriteLine(summary1);
//        logWriter.WriteLine(summary2);
//        logWriter.WriteLine(summary3);
//        logWriter.WriteLine(separator);
//        logWriter.WriteLine(); // Add blank line for readability between runs
//    }
//    catch (Exception ex)
//    {
//        var errorMessage = $"Error reading input file: {ex.Message}";
//        Console.WriteLine(errorMessage);

//        // Try to log error to file if possible
//        try
//        {
//            using var errorWriter = new StreamWriter(logFile!, append: true);
//            errorWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {errorMessage}");
//        }
//        catch
//        {
//            // If we can't write to log file, just continue
//        }
//    }
//}



//static void ProcessInputFileLinesVision(string? inputFile, string? connectionString, string? logFile, bool performUpdate = false)
//{
//    if (string.IsNullOrEmpty(inputFile))
//    {
//        Console.WriteLine("Input file is not configured.");
//        return;
//    }
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    if (string.IsNullOrEmpty(logFile))
//    {
//        Console.WriteLine("Log file is not configured.");
//        return;
//    }
//    try
//    {
//        // Define expected number of fields in the input file
//        const int expectedFieldCount = 18;

//        using var logWriter = new StreamWriter(logFile, append: true);

//        var modeMessage = performUpdate ? "UPDATE MODE - Changes will be applied" : "DRY RUN MODE - No changes will be applied";
//        var header1 = $"Reading users from {inputFile}: ({modeMessage})";
//        var separator = "".PadRight(130, '-');
//        var header2 = $"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5} {"Email",-30} {"Exists",-10}";

//        // Write to console
//        Console.WriteLine(header1);
//        Console.WriteLine(separator);
//        Console.WriteLine(header2);
//        Console.WriteLine(separator);

//        // Write to log file with timestamp
//        logWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {header1}");
//        logWriter.WriteLine(separator);
//        logWriter.WriteLine(header2);
//        logWriter.WriteLine(separator);

//        using var connection = new SqlConnection(connectionString);
//        connection.Open();

//        using var reader = new StreamReader(inputFile);
//        string? line;
//        int lineNumber = 0;
//        int updatedCount = 0;
//        int nonExistingCount = 0;

//        while ((line = reader.ReadLine()) != null)
//        {
//            lineNumber++;

//            // Skip empty lines
//            if (string.IsNullOrWhiteSpace(line))
//                continue;

//            // Split the line by comma
//            var values = line.Split(',');

//            if (values.Length < expectedFieldCount)
//            {
//                var skipMessage = $"Line {lineNumber}: Skipped - Invalid format (expected {expectedFieldCount} values, got {values.Length})";
//                Console.WriteLine(skipMessage);
//                logWriter.WriteLine(skipMessage);
//                continue;
//            }

//            try
//            {
//                // Validate that UserNum is a valid integer before parsing
//                if (!int.TryParse(values[1].Trim(), out int userNum))
//                {
//                    var invalidUserNumMessage = $"Line {lineNumber}: Skipped - UserNum '{values[1].Trim()}' is not a valid integer";
//                    Console.WriteLine(invalidUserNumMessage);
//                    logWriter.WriteLine(invalidUserNumMessage);
//                    continue;
//                }

//                // Validate email address if specified
//                var emailAddress = values[10].Trim();
//                if (!string.IsNullOrEmpty(emailAddress))
//                {
//                    var emailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";
//                    if (!Regex.IsMatch(emailAddress, emailPattern))
//                    {
//                        var invalidEmailMessage = $"Line {lineNumber}: Skipped - EmailAddress '{emailAddress}' is not a valid email address";
//                        Console.WriteLine(invalidEmailMessage);
//                        logWriter.WriteLine(invalidEmailMessage);
//                        continue;
//                    }
//                }

//                // Parse values into VisionUserModel
//                var user = new VisionUserModel
//                {
//                    //SubscriberID = int.Parse(values[0].Trim()),
//                    SubscriberID = 8000,
//                    UserNum = userNum,
//                    UserRoleID = int.Parse(values[2].Trim()),
//                    FirstName = values[3].Trim(),
//                    MiddleInitial = values[4].Trim(),
//                    LastName = values[5].Trim(),
//                    Generation = values[6].Trim(),
//                    Salutation = values[7].Trim(),
//                    Birthdate = DateTime.Parse(values[8].Trim()),
//                    LanguageCode = values[9].Trim(),
//                    EmailAddress = emailAddress,
//                    Phone = values[11].Trim(),
//                    Phone2 = values[12].Trim(),
//                    Phone3 = values[13].Trim(),
//                    Fax = values[14].Trim(),
//                    //SSN = values[15].Trim(),
//                    CompanyRole = int.Parse(values[16].Trim()),
//                    Title = values[17].Trim()
//                };

//                // Check if ID exists in database
//                var checkCommand = new SqlCommand("SELECT COUNT(*) FROM dbo.tUser WHERE SubscriberID = @SubscriberID AND UserNum = @UserNum", connection);
//                checkCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
//                checkCommand.Parameters.AddWithValue("@UserNum", user.UserNum);
//                var count = (int)checkCommand.ExecuteScalar();
//                var existsInDb = count > 0 ? "Yes" : "No";

//                // Display the parsed user with existence status
//                var userLine = $"{user.SubscriberID,-10} {user.UserNum,-10} {user.FirstName,-25} {user.LastName,-25} {user.EmailAddress,-30} {existsInDb,-10}";
//                Console.WriteLine(userLine);
//                logWriter.WriteLine(userLine);

//                // If ID exists, update the record (only if performUpdate is true)
//                if (count > 0)
//                {
//                    if (performUpdate)
//                    {
//                        var updateCommand = new SqlCommand(
//                            """
//                            Update dbo.tUser SET
//                              UserRoleID = @UserRoleID,
//                              FirstName = @FirstName,
//                              MiddleInitial = @MiddleInitial,
//                              LastName = @LastName,
//                              Generation = @Generation,
//                              Salutation = @Salutation,
//                              Birthdate = @Birthdate,
//                              LanguageCode = @LanguageCode,
//                              EmailAddress = @EmailAddress,
//                              Phone = @Phone,
//                              Phone2 = @Phone2,
//                              Phone3 = @Phone3,
//                              Fax = @Fax,
//                              CompanyRole = @CompanyRole,
//                              Title = @Title
//                            WHERE 
//                              SubscriberID = @SubscriberID AND 
//                              UserNum = @UserNum                          
//                            """,
//                            connection);
//                        updateCommand.Parameters.AddWithValue("@SubscriberID", user.SubscriberID);
//                        updateCommand.Parameters.AddWithValue("@UserNum", user.UserNum);

//                        updateCommand.Parameters.AddWithValue("@UserRoleID", user.UserRoleID);
//                        updateCommand.Parameters.AddWithValue("@FirstName", user.FirstName);
//                        updateCommand.Parameters.AddWithValue("@MiddleInitial", user.MiddleInitial);
//                        updateCommand.Parameters.AddWithValue("@LastName", user.LastName);
//                        updateCommand.Parameters.AddWithValue("@Generation", user.Generation);
//                        updateCommand.Parameters.AddWithValue("@Salutation", user.Salutation);
//                        updateCommand.Parameters.AddWithValue("@Birthdate", user.Birthdate);
//                        updateCommand.Parameters.AddWithValue("@LanguageCode", user.LanguageCode);
//                        updateCommand.Parameters.AddWithValue("@EmailAddress", user.EmailAddress);
//                        updateCommand.Parameters.AddWithValue("@Phone", user.Phone);
//                        updateCommand.Parameters.AddWithValue("@Phone2", user.Phone2);
//                        updateCommand.Parameters.AddWithValue("@Phone3", user.Phone3);
//                        updateCommand.Parameters.AddWithValue("@Fax", user.Fax);
//                        updateCommand.Parameters.AddWithValue("@CompanyRole", user.CompanyRole);
//                        updateCommand.Parameters.AddWithValue("@Title", user.Title);

//                        var rowsAffected = updateCommand.ExecuteNonQuery();
//                        var updateMessage = $"   --> UserNum {user.UserNum} updated successfully. Rows affected: {rowsAffected}";
//                        Console.WriteLine(updateMessage);
//                        logWriter.WriteLine(updateMessage);
//                    }
//                    else
//                    {
//                        var dryRunMessage = $"   --> UserNum {user.UserNum} would be updated (DRY RUN - no changes made)";
//                        Console.WriteLine(dryRunMessage);
//                        logWriter.WriteLine(dryRunMessage);
//                    }
//                    updatedCount++;
//                }
//                else
//                {
//                    // Display message if ID doesn't exist and continue
//                    var notExistsMessage = $"   --> UserNum {user.UserNum} does not exist in database. Continuing...";
//                    Console.WriteLine(notExistsMessage);
//                    logWriter.WriteLine(notExistsMessage);
//                    nonExistingCount++;
//                }
//            }
//            catch (FormatException)
//            {
//                var formatErrorMessage = $"Line {lineNumber}: Skipped - Invalid data format";
//                Console.WriteLine(formatErrorMessage);
//                logWriter.WriteLine(formatErrorMessage);
//            }
//        }

//        var summary1 = "Summary:";
//        var summary2 = $"  Total records {(performUpdate ? "updated" : "that would be updated")}: {updatedCount}";
//        var summary3 = $"  Total non-existing records: {nonExistingCount}";

//        // Write to console
//        Console.WriteLine(separator);
//        Console.WriteLine(summary1);
//        Console.WriteLine(summary2);
//        Console.WriteLine(summary3);
//        Console.WriteLine(separator);

//        // Write to log file
//        logWriter.WriteLine(separator);
//        logWriter.WriteLine(summary1);
//        logWriter.WriteLine(summary2);
//        logWriter.WriteLine(summary3);
//        logWriter.WriteLine(separator);
//        logWriter.WriteLine(); // Add blank line for readability between runs
//    }
//    catch (Exception ex)
//    {
//        var errorMessage = $"Error reading input file: {ex.Message}";
//        Console.WriteLine(errorMessage);

//        // Try to log error to file if possible
//        try
//        {
//            using var errorWriter = new StreamWriter(logFile!, append: true);
//            errorWriter.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {errorMessage}");
//        }
//        catch
//        {
//            // If we can't write to log file, just continue
//        }
//    }
//}



//static void AddNewUser(string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    try
//    {
//        // Create an instance of UserModel with the values to insert
//        var newUser = new UserModel
//        {
//            FirstName = "John",
//            LastName = "Doe",
//            Age = 30,
//            Email = "john.doe@example.com"
//        };

//        using var connection = new SqlConnection(connectionString);
//        connection.Open();

//        // Use parameterized query with values from the UserModel instance
//        var command = new SqlCommand(
//            "INSERT INTO dbo.User1 (FirstName, LastName, Age, Email) VALUES (@FirstName, @LastName, @Age, @Email)", 
//            connection);

//        command.Parameters.AddWithValue("@FirstName", newUser.FirstName);
//        command.Parameters.AddWithValue("@LastName", newUser.LastName);
//        command.Parameters.AddWithValue("@Age", newUser.Age);
//        command.Parameters.AddWithValue("@Email", newUser.Email);

//        command.ExecuteNonQuery();
//        Console.WriteLine($"New user added successfully: {newUser.FirstName} {newUser.LastName}");
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error adding new user: {ex.Message}");
//    }
//}



//static void GetNextUserSequenceNum(string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();
//        var command = new SqlCommand("SELECT NEXT VALUE FOR dbo.Sequence1 AS NextUserID", connection);
//        var nextUserId = (int)command.ExecuteScalar();
//        Console.WriteLine($"Next User ID from sequence: {nextUserId}");
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error getting next sequence number: {ex.Message}");

//    }








//static void ReadFromTempTable(string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();
//        var command1 = new SqlCommand("SELECT *  INTO #TempTable FROM User1 WHERE Age < 49", connection);
//        command1.ExecuteNonQuery();

//        var command = new SqlCommand("SELECT * FROM #TempTable", connection);
//        using var reader = command.ExecuteReader();
//        while (reader.Read())
//        {
//            // Process the data from the temp table
//            Console.WriteLine($"ID: {reader["ID"]}, First Name: {reader["Firstname"]}, Age: {reader["Age"]}");
//        }
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error reading from temp table: {ex.Message}");
//    }
//}   









//static void DisplayEmailStatus(string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();
//        var command = new SqlCommand("SELECT ID, FirstName, LastName, Email FROM User1", connection);
//        using var reader = command.ExecuteReader();
//        Console.WriteLine("Email Verification:");
//        Console.WriteLine("".PadRight(120, '-'));
//        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Email", -30} {"E-Status",-10} {"EmailDomain",-10}");
//        Console.WriteLine("".PadRight(120, '-'));
//        var emailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";

//        int count = 0;
//        while (reader.Read())
//        {
//            var id = reader.GetInt32(reader.GetOrdinal("ID"));
//            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
//            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
//            var email = reader.IsDBNull(reader.GetOrdinal("Email")) ? "" : reader.GetString(reader.GetOrdinal("Email"));

//            var isEmailValid = Regex.IsMatch(email, emailPattern);
//            var emailStatus = isEmailValid ? "Valid" : "Invalid";

//            string fullDomain = string.Empty;
//            string emailDomain = string.Empty;

//            if (isEmailValid)
//            {
//                fullDomain = email.Split('@')[1];
//                emailDomain = fullDomain.Substring(0, fullDomain.LastIndexOf('.'));
//            }

//            Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25} {email,-30} {emailStatus,-10} {emailDomain,-10}");
//            count++;
//        }

//        if (count == 0)
//        {
//            Console.WriteLine("No users found");
//        }

//        Console.WriteLine("".PadRight(120, '-'));
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error reading from database: {ex.Message}");
//    }
//}




//Console.ReadKey();


//static void ListRecordsWithNonAlphanumericCharacters(string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();
//        var command = new SqlCommand("SELECT ID, FirstName, LastName FROM User1", connection);
//        using var reader = command.ExecuteReader();
//        Console.WriteLine("Users with non-alphanumeric characters in their names:");
//        Console.WriteLine("".PadRight(70, '-'));
//        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25}");
//        Console.WriteLine("".PadRight(70, '-'));

//        int count = 0;
//        while (reader.Read())
//        {
//            var id = reader.GetInt32(reader.GetOrdinal("ID"));
//            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
//            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
//            if (ContainsNonAlphanumeric(firstName) || ContainsNonAlphanumeric(lastName))
//            {
//                Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25}");
//                count++;
//            }
//        }

//        if (count == 0) 
//        {
//            Console.WriteLine("No users found with non-alphanumeric characters in their names.");
//        }

//        Console.WriteLine("".PadRight(70, '-'));
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error reading from database: {ex.Message}");
//    }
//}

//static void WriteRecordsWithNonAlphanumericCharactersToFile(string? connectionString, string? outputFile)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }
//    if (string.IsNullOrEmpty(outputFile))
//    {
//        Console.WriteLine("Output file is not configured.");
//        return;
//    }
//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();
//        var command = new SqlCommand("SELECT ID, FirstName, LastName FROM User1", connection);
//        using var reader = command.ExecuteReader();

//        // Create StreamWriter to write to file
//        using var writer = new StreamWriter(outputFile, true); // false = overwrite file

//        var header = "Users with non-alphanumeric characters in their names:";
//        var separator = "".PadRight(70, '-');
//        var columnHeader = $"{"ID",-10} {"FirstName",-25} {"LastName",-25}";

//        // Write to console
//        Console.WriteLine(header);
//        Console.WriteLine(separator);
//        Console.WriteLine(columnHeader);
//        Console.WriteLine(separator);

//        // Write to file
//        writer.WriteLine(header);
//        writer.WriteLine(separator);
//        writer.WriteLine(columnHeader);
//        writer.WriteLine(separator);

//        int count = 0;
//        while (reader.Read())
//        {
//            var id = reader.GetInt32(reader.GetOrdinal("ID"));
//            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
//            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
//            if (ContainsNonAlphanumeric(firstName) || ContainsNonAlphanumeric(lastName))
//            {
//                var recordLine = $"{id,-10} {firstName,-25} {lastName,-25}";

//                // Write to console
//                Console.WriteLine(recordLine);

//                // Write to file
//                writer.WriteLine(recordLine);

//                count++;
//            }
//        }

//        var resultMessage = count == 0 
//            ? "No users found with non-alphanumeric characters in their names." 
//            : $"Total records found: {count}";

//        // Write to console
//        Console.WriteLine(separator);
//        Console.WriteLine(resultMessage);

//        // Write to file
//        writer.WriteLine(separator);
//        writer.WriteLine(resultMessage);

//        Console.WriteLine($"Results written to: {outputFile}");
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error reading from database: {ex.Message}");
//    }
//}


//static bool ContainsNonAlphanumeric(string value)
//{
//   if ( string.IsNullOrEmpty(value) )        
//   { 
//        return false; 
//   }

//    return value.Any(c => !char.IsLetterOrDigit(c));

//}

//static string NormalizeAccents(string value)
//{
//    if (string.IsNullOrEmpty(value))
//        return value;

//    // Normalize to FormD (decomposed form) - separates base characters from diacritics
//    var normalizedString = value.Normalize(System.Text.NormalizationForm.FormD);

//    // Use StringBuilder to build result without diacritics
//    var stringBuilder = new System.Text.StringBuilder();

//    foreach (var c in normalizedString)
//    {
//        // Get the Unicode category of the character
//        var unicodeCategory = System.Globalization.CharUnicodeInfo.GetUnicodeCategory(c);

//        // Only add characters that are not non-spacing marks (accents/diacritics)
//        if (unicodeCategory != System.Globalization.UnicodeCategory.NonSpacingMark)
//        {
//            stringBuilder.Append(c);
//        }
//    }

//    // Normalize back to FormC (composed form)
//    return stringBuilder.ToString().Normalize(System.Text.NormalizationForm.FormC);
//}

//static void DisplayUsersLessThanAge(int age, string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Connection string is not configured.");
//        return;
//    }

//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();

//        // Create command to call stored procedure
//        using var command = new SqlCommand("GetUsersLessThanAge", connection);
//        command.CommandType = System.Data.CommandType.StoredProcedure;

//        // Add age parameter
//        command.Parameters.AddWithValue("@Age", age);

//        // Execute stored procedure and read results
//        using var reader = command.ExecuteReader();

//        Console.WriteLine($"Users less than {age} years old:");
//        Console.WriteLine("".PadRight(70, '-'));
//        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5}");
//        Console.WriteLine("".PadRight(70, '-'));

//        if (!reader.HasRows)
//        {             
//            Console.WriteLine("No users found.");
//        }
//        else
//        {
//            while (reader.Read())
//            {
//                var id = reader.GetInt32(reader.GetOrdinal("ID"));
//                var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
//                var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
//                var userAge = reader.IsDBNull(reader.GetOrdinal("Age")) ? 0 : reader.GetInt32(reader.GetOrdinal("Age"));

//                Console.WriteLine($"{id,-10} {NormalizeAccents(firstName),-25} {NormalizeAccents(lastName),-25} {userAge,-5}");
//            }
//        }
//        Console.WriteLine("".PadRight(70, '-'));
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error getting users: {ex.Message}");
//    }
//}


//static void AddUser(UserModel user, string? connectionString)
//{


//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Source connection string is not configured.");
//        return;
//    }

//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();

//        // Create command to call stored procedure
//        using var command = new SqlCommand("AddUser", connection);
//        command.CommandType = System.Data.CommandType.StoredProcedure;

//        // Add parameters
//        command.Parameters.AddWithValue("@FirstName", user.FirstName);
//        command.Parameters.AddWithValue("@LastName", user.LastName);
//        command.Parameters.AddWithValue("@Age", user.Age);

//        // Execute stored procedure
//        var rowsAffected = command.ExecuteNonQuery();

//        Console.WriteLine($"User added successfully. Rows affected: {rowsAffected}");
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error adding user: {ex.Message}");
//    }
//}

//static void DisplayUsers(string? connectionString)
//{
//    if (string.IsNullOrEmpty(connectionString))
//    {
//        Console.WriteLine("Source connection string is not configured.");
//        return;
//    }

//    try
//    {
//        using var connection = new SqlConnection(connectionString);
//        connection.Open();

//        var command = new SqlCommand("SELECT ID, FirstName, LastName, Age FROM User1", connection);
//        using var reader = command.ExecuteReader();

//        Console.WriteLine("Users from User1 table:");
//        Console.WriteLine("".PadRight(70, '-'));
//        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5}");
//        Console.WriteLine("".PadRight(70, '-'));

//        while (reader.Read())
//        {
//            // Old approach - using field positions:
//            // var id = reader.GetInt32(0);
//            // var firstName = reader.IsDBNull(1) ? "" : reader.GetString(1);
//            // var lastName = reader.IsDBNull(2) ? "" : reader.GetString(2);
//            // var age = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);

//            // New approach - using field names:
//            var id = reader.GetInt32(reader.GetOrdinal("ID"));
//            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
//            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
//            var age = reader.IsDBNull(reader.GetOrdinal("Age")) ? 0 : reader.GetInt32(reader.GetOrdinal("Age"));

//            Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25} {age,-5}");
//        }

//        Console.WriteLine("".PadRight(70, '-'));
//    }
//    catch (Exception ex)
//    {
//        Console.WriteLine($"Error reading from database: {ex.Message}");
//    }
//}


//static void DisplayUsersFromList(string? connectionString)
//    {
//        List<UserModel> users = new List<UserModel>
//    {
//        new UserModel { ID = 1, FirstName = "Alice", LastName = "Johnson", Age = 28 },
//        new UserModel { ID = 2, FirstName = "Bob", LastName = "Smith", Age = 30 },
//        new UserModel { ID = 3, FirstName = "Charlie", LastName = "Brown", Age = 22 }
//    };
//        if (string.IsNullOrEmpty(connectionString))
//        {
//            Console.WriteLine("Source connection string is not configured.");
//            return;
//        }

//        try
//        {
//            using var connection = new SqlConnection(connectionString);
//            connection.Open();

//            var command = new SqlCommand("SELECT ID, FirstName, LastName, Age FROM User1", connection);
//            using var reader = command.ExecuteReader();


//            while (reader.Read())
//            {
//                // Old approach - using field positions:
//                // var id = reader.GetInt32(0);
//                // var firstName = reader.IsDBNull(1) ? "" : reader.GetString(1);
//                // var lastName = reader.IsDBNull(2) ? "" : reader.GetString(2);
//                // var age = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);

//                // New approach - using field names:
//                var id = reader.GetInt32(reader.GetOrdinal("ID"));
//                var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
//                var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
//                var age = reader.IsDBNull(reader.GetOrdinal("Age")) ? 0 : reader.GetInt32(reader.GetOrdinal("Age"));

//                users.Add(new UserModel { ID = id, FirstName = firstName, LastName = lastName, Age = age });


//            }

//            Console.WriteLine("Users from User1 table:");
//            Console.WriteLine("".PadRight(70, '-'));
//            Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5}");
//            Console.WriteLine("".PadRight(70, '-'));

//            foreach (var user in users)
//            {
//                Console.WriteLine($"{user.ID,-10} {user.FirstName,-25} {user.LastName,-25} {user.Age,-5}");
//            }
//            Console.WriteLine("".PadRight(70, '-'));
//        }
//        catch (Exception ex)
//        {
//            Console.WriteLine($"Error reading from database: {ex.Message}");
//        }
//    }
//}



//public struct UserModel
//{
//    public int ID { get; set; }
//    public string FirstName { get; set; }
//    public string LastName { get; set; }
//    public int Age { get; set; }
//    public string Email { get; set; }
//}

//string userIDString = "321";
//int userIDOut;


//if (!int.TryParse(userIDString, out userIDOut))
//{
//    Console.WriteLine("Invalid User ID specified."); 
//    return;
//}
//else
//{
//    Console.WriteLine($"Valid User ID specified: {userIDOut}");
//}

// Display users from source database
//DisplayUsers(sourceConnection);

//AddUser(new UserModel
//{
//    FirstName = "Bob",
//    LastName = "Smith",
//    Age = 30
//}, sourceConnection);

// Below now normalizes the accents
//DisplayUsersLessThanAge(59, sourceConnection);


//ListRecordsWithNonAlphanumericCharacters(sourceConnection);

//DisplayEmailStatus(sourceConnection);

//DisplayUsersFromList(sourceConnection);

//WriteRecordsWithNonAlphanumericCharactersToFile(sourceConnection, outputFile);


//ReadFromTempTable(sourceConnection);


//GetNextUserSequenceNum(sourceConnection);

//AddNewUser(sourceConnection);

//ProcessInputFileLinesVision(updateInputFile, sourceConnection, logFile, performUpdate: false);

//ProcessInputFileLines(updateInputFile, sourceConnection, logFile, performUpdate: false);







