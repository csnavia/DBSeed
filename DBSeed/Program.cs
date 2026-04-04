using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;

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

Console.WriteLine($"Environment: {Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production"}");
Console.WriteLine($"Environment Label: {environmentLabel}");        
Console.WriteLine($"Source Connection: {sourceConnection}");
Console.WriteLine($"Destination Connection: {destinationConnection}");
Console.WriteLine();

// Display users from source database
//DisplayUsers(sourceConnection);

//AddUser(new UserModel
//{
//    FirstName = "Bob",
//    LastName = "Smith",
//    Age = 30
//}, sourceConnection);

DisplayUsersLessThanAge(59, sourceConnection);


Console.ReadKey();



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

                Console.WriteLine($"{id,-10} {firstName,-25} {lastName,-25} {userAge,-5}");
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


public struct UserModel
{
    public int ID { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public int Age { get; set; }
}

