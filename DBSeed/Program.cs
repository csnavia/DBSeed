using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
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
var inputFile = configuration["InputFile"];


var outputFile = configuration["OutputFile"];
var logFile = configuration["LogFile"];

Console.WriteLine($"Environment: {Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}");
Console.WriteLine($"Environment Label: {environmentLabel}");        
Console.WriteLine($"Source Connection: {sourceConnection}");
Console.WriteLine($"Destination Connection: {destinationConnection}");
Console.WriteLine($"Log File: {logFile}");
Console.WriteLine();


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

//ProcessInputFileLinesVision(inputFile, sourceConnection, logFile, performUpdate: false);

ProcessInputFileLines(inputFile, sourceConnection, logFile, performUpdate: false);


 Console.ReadKey();

static void ProcessInputFileLines(string? inputFile, string? connectionString, string? logFile, bool performUpdate = false)
{
    if (string.IsNullOrEmpty(inputFile))
    {
        Console.WriteLine("Input file is not configured.");
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
        const int expectedFieldCount = 5;

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

            // Skip empty lines
            if (string.IsNullOrWhiteSpace(line))
                continue;

            // Split the line by comma
            var values = line.Split(',');

            // Ensure we have enough values (expecting: ID,FirstName,LastName,Age,Email)
            if (values.Length < expectedFieldCount)
            {
                var skipMessage = $"Line {lineNumber}: Skipped - Invalid format (expected {expectedFieldCount} values, got {values.Length})";
                Console.WriteLine(skipMessage);
                logWriter.WriteLine(skipMessage);
                continue;
            }

            try
            {
                // Parse values into UserModel
                var user = new UserModel
                {
                    ID = int.Parse(values[0].Trim()),
                    FirstName = values[1].Trim(),
                    LastName = values[2].Trim(),
                    Age = int.Parse(values[3].Trim()),
                    Email = values[4].Trim()
                };

                // Check if ID exists in database
                var checkCommand = new SqlCommand("SELECT COUNT(*) FROM User1 WHERE ID = @ID", connection);
                checkCommand.Parameters.AddWithValue("@ID", user.ID);
                var count = (int)checkCommand.ExecuteScalar();
                var existsInDb = count > 0 ? "Yes" : "No";

                // Display the parsed user with existence status
                var userLine = $"{user.ID,-10} {user.FirstName,-25} {user.LastName,-25} {user.Age,-5} {user.Email,-30} {existsInDb,-10}";
                Console.WriteLine(userLine);
                logWriter.WriteLine(userLine);

                // If ID exists, update the record (only if performUpdate is true)
                if (count > 0)
                {
                    if (performUpdate)
                    {
                        var updateCommand = new SqlCommand(
                            "UPDATE User1 SET FirstName = @FirstName, LastName = @LastName, Age = @Age, Email = @Email WHERE ID = @ID",
                            connection);
                        updateCommand.Parameters.AddWithValue("@ID", user.ID);
                        updateCommand.Parameters.AddWithValue("@FirstName", user.FirstName);
                        updateCommand.Parameters.AddWithValue("@LastName", user.LastName);
                        updateCommand.Parameters.AddWithValue("@Age", user.Age);
                        updateCommand.Parameters.AddWithValue("@Email", user.Email);

                        var rowsAffected = updateCommand.ExecuteNonQuery();
                        var updateMessage = $"   --> ID {user.ID} updated successfully. Rows affected: {rowsAffected}";
                        Console.WriteLine(updateMessage);
                        logWriter.WriteLine(updateMessage);
                    }
                    else
                    {
                        var dryRunMessage = $"   --> ID {user.ID} would be updated (DRY RUN - no changes made)";
                        Console.WriteLine(dryRunMessage);
                        logWriter.WriteLine(dryRunMessage);
                    }
                    updatedCount++;
                }
                else
                {
                    // Display message if ID doesn't exist and continue
                    var notExistsMessage = $"   --> ID {user.ID} does not exist in database. Continuing...";
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



static void ProcessInputFileLinesVision(string? inputFile, string? connectionString, string? logFile, bool performUpdate = false)
{
    if (string.IsNullOrEmpty(inputFile))
    {
        Console.WriteLine("Input file is not configured.");
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

                // Parse values into VisionUserModel
                var user = new VisionUserModel
                {
                    //SubscriberID = int.Parse(values[0].Trim()),
                    SubscriberID = 8000,
                    UserNum = userNum,
                    UserRoleID = int.Parse(values[2].Trim()),
                    FirstName = values[3].Trim(),
                    MiddleInitial = values[4].Trim(),
                    LastName = values[5].Trim(),
                    Generation = values[6].Trim(),
                    Salutation = values[7].Trim(),
                    Birthdate = DateTime.Parse(values[8].Trim()),
                    LanguageCode = values[9].Trim(),
                    EmailAddress = emailAddress,
                    Phone = values[11].Trim(),
                    Phone2 = values[12].Trim(),
                    Phone3 = values[13].Trim(),
                    Fax = values[14].Trim(),
                    //SSN = values[15].Trim(),
                    CompanyRole = int.Parse(values[16].Trim()),
                    Title = values[17].Trim()
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
                              Title = @Title
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
                        updateCommand.Parameters.AddWithValue("@Birthdate", user.Birthdate);
                        updateCommand.Parameters.AddWithValue("@LanguageCode", user.LanguageCode);
                        updateCommand.Parameters.AddWithValue("@EmailAddress", user.EmailAddress);
                        updateCommand.Parameters.AddWithValue("@Phone", user.Phone);
                        updateCommand.Parameters.AddWithValue("@Phone2", user.Phone2);
                        updateCommand.Parameters.AddWithValue("@Phone3", user.Phone3);
                        updateCommand.Parameters.AddWithValue("@Fax", user.Fax);
                        updateCommand.Parameters.AddWithValue("@CompanyRole", user.CompanyRole);
                        updateCommand.Parameters.AddWithValue("@Title", user.Title);

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



static void AddNewUser(string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    try
    {
        // Create an instance of UserModel with the values to insert
        var newUser = new UserModel
        {
            FirstName = "John",
            LastName = "Doe",
            Age = 30,
            Email = "john.doe@example.com"
        };

        using var connection = new SqlConnection(connectionString);
        connection.Open();

        // Use parameterized query with values from the UserModel instance
        var command = new SqlCommand(
            "INSERT INTO dbo.User1 (FirstName, LastName, Age, Email) VALUES (@FirstName, @LastName, @Age, @Email)", 
            connection);

        command.Parameters.AddWithValue("@FirstName", newUser.FirstName);
        command.Parameters.AddWithValue("@LastName", newUser.LastName);
        command.Parameters.AddWithValue("@Age", newUser.Age);
        command.Parameters.AddWithValue("@Email", newUser.Email);

        command.ExecuteNonQuery();
        Console.WriteLine($"New user added successfully: {newUser.FirstName} {newUser.LastName}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error adding new user: {ex.Message}");
    }
}



static void GetNextUserSequenceNum(string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var command = new SqlCommand("SELECT NEXT VALUE FOR dbo.Sequence1 AS NextUserID", connection);
        var nextUserId = (int)command.ExecuteScalar();
        Console.WriteLine($"Next User ID from sequence: {nextUserId}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error getting next sequence number: {ex.Message}");

    }








static void ReadFromTempTable(string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var command1 = new SqlCommand("SELECT *  INTO #TempTable FROM User1 WHERE Age < 49", connection);
        command1.ExecuteNonQuery();

        var command = new SqlCommand("SELECT * FROM #TempTable", connection);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            // Process the data from the temp table
            Console.WriteLine($"ID: {reader["ID"]}, First Name: {reader["Firstname"]}, Age: {reader["Age"]}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading from temp table: {ex.Message}");
    }
}   









static void DisplayEmailStatus(string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var command = new SqlCommand("SELECT ID, FirstName, LastName, Email FROM User1", connection);
        using var reader = command.ExecuteReader();
        Console.WriteLine("Email Verification:");
        Console.WriteLine("".PadRight(120, '-'));
        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Email", -30} {"E-Status",-10} {"EmailDomain",-10}");
        Console.WriteLine("".PadRight(120, '-'));
        var emailPattern = @"^[^@\s]+@[^@\s]+\.[^@\s]+$";

        int count = 0;
        while (reader.Read())
        {
            var id = reader.GetInt32(reader.GetOrdinal("ID"));
            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
            var email = reader.IsDBNull(reader.GetOrdinal("Email")) ? "" : reader.GetString(reader.GetOrdinal("Email"));

            var isEmailValid = Regex.IsMatch(email, emailPattern);
            var emailStatus = isEmailValid ? "Valid" : "Invalid";

            string fullDomain = string.Empty;
            string emailDomain = string.Empty;

            if (isEmailValid)
            {
                fullDomain = email.Split('@')[1];
                emailDomain = fullDomain.Substring(0, fullDomain.LastIndexOf('.'));
            }

            Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25} {email,-30} {emailStatus,-10} {emailDomain,-10}");
            count++;
        }

        if (count == 0)
        {
            Console.WriteLine("No users found");
        }

        Console.WriteLine("".PadRight(120, '-'));
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading from database: {ex.Message}");
    }
}




Console.ReadKey();


static void ListRecordsWithNonAlphanumericCharacters(string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var command = new SqlCommand("SELECT ID, FirstName, LastName FROM User1", connection);
        using var reader = command.ExecuteReader();
        Console.WriteLine("Users with non-alphanumeric characters in their names:");
        Console.WriteLine("".PadRight(70, '-'));
        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25}");
        Console.WriteLine("".PadRight(70, '-'));

        int count = 0;
        while (reader.Read())
        {
            var id = reader.GetInt32(reader.GetOrdinal("ID"));
            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
            if (ContainsNonAlphanumeric(firstName) || ContainsNonAlphanumeric(lastName))
            {
                Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25}");
                count++;
            }
        }

        if (count == 0) 
        {
            Console.WriteLine("No users found with non-alphanumeric characters in their names.");
        }

        Console.WriteLine("".PadRight(70, '-'));
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading from database: {ex.Message}");
    }
}

static void WriteRecordsWithNonAlphanumericCharactersToFile(string? connectionString, string? outputFile)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }
    if (string.IsNullOrEmpty(outputFile))
    {
        Console.WriteLine("Output file is not configured.");
        return;
    }
    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var command = new SqlCommand("SELECT ID, FirstName, LastName FROM User1", connection);
        using var reader = command.ExecuteReader();

        // Create StreamWriter to write to file
        using var writer = new StreamWriter(outputFile, true); // false = overwrite file

        var header = "Users with non-alphanumeric characters in their names:";
        var separator = "".PadRight(70, '-');
        var columnHeader = $"{"ID",-10} {"FirstName",-25} {"LastName",-25}";

        // Write to console
        Console.WriteLine(header);
        Console.WriteLine(separator);
        Console.WriteLine(columnHeader);
        Console.WriteLine(separator);

        // Write to file
        writer.WriteLine(header);
        writer.WriteLine(separator);
        writer.WriteLine(columnHeader);
        writer.WriteLine(separator);

        int count = 0;
        while (reader.Read())
        {
            var id = reader.GetInt32(reader.GetOrdinal("ID"));
            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
            if (ContainsNonAlphanumeric(firstName) || ContainsNonAlphanumeric(lastName))
            {
                var recordLine = $"{id,-10} {firstName,-25} {lastName,-25}";

                // Write to console
                Console.WriteLine(recordLine);

                // Write to file
                writer.WriteLine(recordLine);

                count++;
            }
        }

        var resultMessage = count == 0 
            ? "No users found with non-alphanumeric characters in their names." 
            : $"Total records found: {count}";

        // Write to console
        Console.WriteLine(separator);
        Console.WriteLine(resultMessage);

        // Write to file
        writer.WriteLine(separator);
        writer.WriteLine(resultMessage);

        Console.WriteLine($"Results written to: {outputFile}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading from database: {ex.Message}");
    }
}


static bool ContainsNonAlphanumeric(string value)
{
   if ( string.IsNullOrEmpty(value) )        
   { 
        return false; 
   }

    return value.Any(c => !char.IsLetterOrDigit(c));

}

static string NormalizeAccents(string value)
{
    if (string.IsNullOrEmpty(value))
        return value;

    // Normalize to FormD (decomposed form) - separates base characters from diacritics
    var normalizedString = value.Normalize(System.Text.NormalizationForm.FormD);

    // Use StringBuilder to build result without diacritics
    var stringBuilder = new System.Text.StringBuilder();

    foreach (var c in normalizedString)
    {
        // Get the Unicode category of the character
        var unicodeCategory = System.Globalization.CharUnicodeInfo.GetUnicodeCategory(c);

        // Only add characters that are not non-spacing marks (accents/diacritics)
        if (unicodeCategory != System.Globalization.UnicodeCategory.NonSpacingMark)
        {
            stringBuilder.Append(c);
        }
    }

    // Normalize back to FormC (composed form)
    return stringBuilder.ToString().Normalize(System.Text.NormalizationForm.FormC);
}

static void DisplayUsersLessThanAge(int age, string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Connection string is not configured.");
        return;
    }

    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        // Create command to call stored procedure
        using var command = new SqlCommand("GetUsersLessThanAge", connection);
        command.CommandType = System.Data.CommandType.StoredProcedure;

        // Add age parameter
        command.Parameters.AddWithValue("@Age", age);

        // Execute stored procedure and read results
        using var reader = command.ExecuteReader();

        Console.WriteLine($"Users less than {age} years old:");
        Console.WriteLine("".PadRight(70, '-'));
        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5}");
        Console.WriteLine("".PadRight(70, '-'));

        if (!reader.HasRows)
        {             
            Console.WriteLine("No users found.");
        }
        else
        {
            while (reader.Read())
            {
                var id = reader.GetInt32(reader.GetOrdinal("ID"));
                var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
                var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
                var userAge = reader.IsDBNull(reader.GetOrdinal("Age")) ? 0 : reader.GetInt32(reader.GetOrdinal("Age"));

                Console.WriteLine($"{id,-10} {NormalizeAccents(firstName),-25} {NormalizeAccents(lastName),-25} {userAge,-5}");
            }
        }
        Console.WriteLine("".PadRight(70, '-'));
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error getting users: {ex.Message}");
    }
}


static void AddUser(UserModel user, string? connectionString)
{


    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Source connection string is not configured.");
        return;
    }

    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        // Create command to call stored procedure
        using var command = new SqlCommand("AddUser", connection);
        command.CommandType = System.Data.CommandType.StoredProcedure;

        // Add parameters
        command.Parameters.AddWithValue("@FirstName", user.FirstName);
        command.Parameters.AddWithValue("@LastName", user.LastName);
        command.Parameters.AddWithValue("@Age", user.Age);

        // Execute stored procedure
        var rowsAffected = command.ExecuteNonQuery();

        Console.WriteLine($"User added successfully. Rows affected: {rowsAffected}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error adding user: {ex.Message}");
    }
}

static void DisplayUsers(string? connectionString)
{
    if (string.IsNullOrEmpty(connectionString))
    {
        Console.WriteLine("Source connection string is not configured.");
        return;
    }

    try
    {
        using var connection = new SqlConnection(connectionString);
        connection.Open();

        var command = new SqlCommand("SELECT ID, FirstName, LastName, Age FROM User1", connection);
        using var reader = command.ExecuteReader();

        Console.WriteLine("Users from User1 table:");
        Console.WriteLine("".PadRight(70, '-'));
        Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5}");
        Console.WriteLine("".PadRight(70, '-'));

        while (reader.Read())
        {
            // Old approach - using field positions:
            // var id = reader.GetInt32(0);
            // var firstName = reader.IsDBNull(1) ? "" : reader.GetString(1);
            // var lastName = reader.IsDBNull(2) ? "" : reader.GetString(2);
            // var age = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);

            // New approach - using field names:
            var id = reader.GetInt32(reader.GetOrdinal("ID"));
            var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
            var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
            var age = reader.IsDBNull(reader.GetOrdinal("Age")) ? 0 : reader.GetInt32(reader.GetOrdinal("Age"));

            Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25} {age,-5}");
        }

        Console.WriteLine("".PadRight(70, '-'));
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading from database: {ex.Message}");
    }
}


static void DisplayUsersFromList(string? connectionString)
    {
        List<UserModel> users = new List<UserModel>
    {
        new UserModel { ID = 1, FirstName = "Alice", LastName = "Johnson", Age = 28 },
        new UserModel { ID = 2, FirstName = "Bob", LastName = "Smith", Age = 30 },
        new UserModel { ID = 3, FirstName = "Charlie", LastName = "Brown", Age = 22 }
    };
        if (string.IsNullOrEmpty(connectionString))
        {
            Console.WriteLine("Source connection string is not configured.");
            return;
        }

        try
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();

            var command = new SqlCommand("SELECT ID, FirstName, LastName, Age FROM User1", connection);
            using var reader = command.ExecuteReader();


            while (reader.Read())
            {
                // Old approach - using field positions:
                // var id = reader.GetInt32(0);
                // var firstName = reader.IsDBNull(1) ? "" : reader.GetString(1);
                // var lastName = reader.IsDBNull(2) ? "" : reader.GetString(2);
                // var age = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);

                // New approach - using field names:
                var id = reader.GetInt32(reader.GetOrdinal("ID"));
                var firstName = reader.IsDBNull(reader.GetOrdinal("FirstName")) ? "" : reader.GetString(reader.GetOrdinal("FirstName"));
                var lastName = reader.IsDBNull(reader.GetOrdinal("LastName")) ? "" : reader.GetString(reader.GetOrdinal("LastName"));
                var age = reader.IsDBNull(reader.GetOrdinal("Age")) ? 0 : reader.GetInt32(reader.GetOrdinal("Age"));

                users.Add(new UserModel { ID = id, FirstName = firstName, LastName = lastName, Age = age });


            }

            Console.WriteLine("Users from User1 table:");
            Console.WriteLine("".PadRight(70, '-'));
            Console.WriteLine($"{"ID",-10} {"FirstName",-25} {"LastName",-25} {"Age",-5}");
            Console.WriteLine("".PadRight(70, '-'));

            foreach (var user in users)
            {
                Console.WriteLine($"{user.ID,-10} {user.FirstName,-25} {user.LastName,-25} {user.Age,-5}");
            }
            Console.WriteLine("".PadRight(70, '-'));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading from database: {ex.Message}");
        }
    }
}



public struct UserModel
{
    public int ID { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public int Age { get; set; }
    public string Email { get; set; }
}


public struct VisionUserModel
{
    public int SubscriberID { get; set; }
    public int UserNum { get; set; }
    public int UserRoleID { get; set; }
    public string FirstName { get; set; }
    public string MiddleInitial { get; set; }
    public string LastName { get; set; }
    public string Generation { get; set; }
    public string Salutation { get; set; }
    public DateTime Birthdate { get; set; }
    public string LanguageCode { get; set; }
    public string EmailAddress { get; set; }    
    public string Phone { get; set; }
    public string Phone2 { get; set; }
    public string Phone3 { get; set; }
    public string Fax { get; set; }
    //public string SSN { get; set; }
    public int CompanyRole { get; set; }
    public string Title { get; set; }
}