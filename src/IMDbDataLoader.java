import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Scanner;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;

public class IMDbDataLoader {

    public static void main(String[] args) {
        // JDBC connection parameters
        String jdbcUrl = "jdbc:oracle:thin:@your-Computer:1521:xe"; // JDBC URL of Oracle database
        String username = "SYSTEM"; // Username for Oracle database
        String password = "password"; // Password for Oracle database


        // Start IMDb ID
        int startImdbId = 1832382;

        try {
            // Establish connection to Oracle database
            Connection conn = DriverManager.getConnection(jdbcUrl, username, password);

            // Loop 11 times
            for (int i = 0; i < 15; i++) {
                // IMDb data
                String imdbId = "tt" + (startImdbId + i * 2);

                // IMDb API endpoint URL for movie details
                // Note: The API key was paid for, if you find this program to not work
                // you may need a new API key.
                String movieApiUrl = "https://tv-api.com/en/API/Title/k_1c8dka1g/" + imdbId;

                // Make HTTP request to IMDb API for movie details
                JSONObject movieData = fetchMovieData(movieApiUrl);

                // Extract relevant information from JSON response
                String title = movieData.optString("title");
                int runtime = movieData.optInt("runtimeMins");
                String releaseDate = movieData.optString("releaseDate");
                releaseDate = formatReleaseDate(releaseDate); // Format release date
                String aspectRatio = movieData.optString("aspectRatio");
                String languages = movieData.optString("languages");
                String budget = movieData.optString("budget");


                // Prepare SQL statement to insert movie details into Movie table
                String insertMovieSql = "INSERT INTO Movie (Title, Runtime, Release_Date, Aspect_Ratio, Language, Budget) " +
                        "VALUES (?, ?, ?, ?, ?, ?)";

                try (PreparedStatement pstmt = conn.prepareStatement(insertMovieSql)) {
                    pstmt.setString(1, title);
                    pstmt.setInt(2, runtime);
                    pstmt.setString(3, releaseDate);
                    pstmt.setString(4, aspectRatio);
                    pstmt.setString(5, languages);
                    pstmt.setString(6, budget);

                    // Execute SQL insert statement for Movie
                    pstmt.executeUpdate();
                } catch (SQLException e) {
                    // Handle SQL exception
                    e.printStackTrace();
                }

                // try to insert genre
                String genreApiUrl = "https://tv-api.com/en/API/Title/k_1c8dka1g/" + imdbId;
                JSONArray genreList = fetchGenre(genreApiUrl);
                for (int j = 0; j < genreList.length(); j++) {
                    JSONObject genreData = genreList.getJSONObject(j);

                    // Extract the role of the actor in the current movie
                    String genre = genreData.optString("key");

                    // Insert actor into Actor table if not already present
                    insertGenre(conn, title, genre);

                }



                // IMDb API endpoint URL for full cast
                String fullCastApiUrl = "https://tv-api.com/en/API/FullCast/k_1c8dka1g/" + imdbId;

                // Make HTTP request to IMDb API for full cast
                JSONArray fullCast = fetchFullCast(fullCastApiUrl);

                // Insert actors into Actor table and relationships into Acts_In table
                for (int j = 0; j < fullCast.length(); j++) {
                    JSONObject actorData = fullCast.getJSONObject(j);

                    String[] actorNameParts = actorData.optString("name").split(" ");
                    String actorFirstName = "";
                    String actorLastName = "";
                    if (actorNameParts.length >= 1) {
                        actorFirstName = actorNameParts[0];
                        if (actorNameParts.length >= 2) {
                            actorLastName = actorNameParts[1];
                        }
                    }
                    // Check if actor's first or last name is null
                    if (actorFirstName == null || actorLastName == null || actorFirstName == "" || actorLastName == "") {
                        continue; // Skip to the next actor if first or last name is null
                    }
                    // Extract the role of the actor in the current movie
                    String role = actorData.optString("asCharacter");

                    // IMDb API endpoint URL for actor details
                    String actorApiUrl = "https://tv-api.com/API/Name/k_1c8dka1g/" + actorData.optString("id");

                    // Make HTTP request to IMDb API for actor details
                    JSONObject actorDetails = fetchActorDetails(actorApiUrl);

                    // Extract relevant information from JSON response for actor details
                    String bDate = actorDetails.optString("birthDate");
                    bDate = formatReleaseDate(bDate); // Format birth date
                    String height = actorDetails.optString("height");
                    String gender = actorDetails.optString("role");
                    int salary = actorDetails.optInt("salary");

                    if (actorDetails.optString("role").toLowerCase().contains("actress")) {
                        gender = "F";
                    } else {
                        gender = "M";
                    }
                    // Insert actor into Actor table if not already present
                    insertActor(conn, actorFirstName, actorLastName, role, bDate, height, gender, salary);

                    // Insert relationship into Acts_In table
                    insertActIn(conn, actorFirstName, actorLastName, title);
                }

                // IMDb API endpoint URL for full cast
                String directorsApiUrl = "https://tv-api.com/en/API/Title/k_1c8dka1g/" + imdbId + "/FullCast";

                // Make HTTP request to IMDb API for directors
                JSONArray directors = fetchDirectors(directorsApiUrl);

                for (int l = 0; l < directors.length(); l++) {
                    String directorId = directors.getString(l); // Fetch the director ID as a string
                    String directorDetailsApiUrl = "https://tv-api.com/API/Name/k_1c8dka1g/" + directorId;
                    JSONObject directorDetails = fetchDirectorDetails(directorDetailsApiUrl);
                    // Process director details as needed
                    String[] directorNameParts = directorDetails.optString("name").split(" ");
                    String directorFirstName = "";
                    String directorLastName = "";
                    if (directorNameParts.length >= 1) {
                        directorFirstName = directorNameParts[0];
                        if (directorNameParts.length >= 2) {
                            directorLastName = directorNameParts[1];
                        }
                    }
                    String bDate = directorDetails.optString("birthDate");
                    String height = directorDetails.optString("height");
                    String gender = directorDetails.optString("gender");
                    int salary = directorDetails.optInt("salary");
                    // Format birthdate
                    bDate = formatDBirthdate(bDate);
                    if (directorDetails.optString("summary").toLowerCase().contains("him") || directorDetails.optString("summary").toLowerCase().contains("his")) {
                        gender = "M";

                    } else if (!directorDetails.optString("summary").toLowerCase().contains("he") && !directorDetails.optString("summary").toLowerCase().contains("him") && !directorDetails.optString("summary").toLowerCase().contains("his") && !directorDetails.optString("summary").toLowerCase().contains("she") && !directorDetails.optString("summary").toLowerCase().contains("her") && !directorDetails.optString("summary").toLowerCase().contains("hers")) {
                        gender = null;
                    } else {
                        gender = "F";
                    }
                    // Insert director into Director table if not already present
                    insertDirector(conn, directorFirstName, directorLastName, salary, bDate, height, gender);

                    insertDirects(conn, directorFirstName, directorLastName, title);
                }
//Modify the main method to fetch writer details and insert them into the Writer table

                String writersApiUrl = "https://tv-api.com/en/API/Title/k_1c8dka1g/" + imdbId + "/FullCast";

                // Make HTTP request to IMDb API for writersF
                JSONArray writers = fetchWriters(writersApiUrl);

                for (int m = 0; m < writers.length(); m++) {
                    String writerId = writers.getString(m); // Fetch the writer ID as a string
                    String writerDetailsApiUrl = "https://tv-api.com/API/Name/k_1c8dka1g/" + writerId;
                    JSONObject writerDetails = fetchWriterDetails(writerDetailsApiUrl);
                    // Process writer details as needed
                    String[] writerNameParts = writerDetails.optString("name").split(" ");
                    String writerFirstName = "";
                    String writerLastName = "";
                    if (writerNameParts.length >= 1) {
                        writerFirstName = writerNameParts[0];
                        if (writerNameParts.length >= 2) {
                            writerLastName = writerNameParts[1];
                        }
                    }
                    String bDate = writerDetails.optString("birthDate");
                    String height = writerDetails.optString("height");

                    String gender = writerDetails.optString("gender");
                    if (writerDetails.optString("summary").toLowerCase().contains("him") || writerDetails.optString("summary").toLowerCase().contains("his")) {
                        gender = "M";

                    } else if (!writerDetails.optString("summary").toLowerCase().contains("he") && !writerDetails.optString("summary").toLowerCase().contains("him") && !writerDetails.optString("summary").toLowerCase().contains("his") && !writerDetails.optString("summary").toLowerCase().contains("she") && !writerDetails.optString("summary").toLowerCase().contains("her") && !writerDetails.optString("summary").toLowerCase().contains("hers")) {
                        gender = null;
                    } else {
                        gender = "F";
                    }
                    int salary = writerDetails.optInt("salary");

                    // Insert writer into Writer table if not already present
                    insertWriter(conn, writerFirstName, writerLastName, bDate, height, salary, gender);

                    // Insert relationship into Writes table
                    insertWrites(conn, writerFirstName, writerLastName, title);
                }


            }


            // Close the database connection
            conn.close();
        } catch (IOException | JSONException | SQLException e) {
            e.printStackTrace();
        }
    }

    private static String formatReleaseDate(String releaseDate) {
        if (releaseDate.equals("")) {
            return null;
        }
        // Parse IMDb date string to LocalDate
        LocalDate parsedDate = LocalDate.parse(releaseDate);

        // Format LocalDate to Oracle SQL date format (DD-MON-YY)
        return parsedDate.format(DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.ENGLISH));
    }

    // Method to fetch movie data from IMDb API
    private static JSONObject fetchMovieData(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        JSONObject jsonResponse = new JSONObject(response.toString());

        // Extract boxOffice object
        JSONObject boxOffice = jsonResponse.optJSONObject("boxOffice");
        if (boxOffice != null) {
            // Extract budget from boxOffice object
            String budget = boxOffice.optString("budget");
            budget = budget != null ? budget.replace(" (estimated)", "") : null;
            jsonResponse.put("budget", budget); // Add budget to the main JSON object
        }

        return jsonResponse;
    }

    // Method to fetch full cast data from IMDb API
    private static JSONArray fetchFullCast(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        JSONObject jsonResponse = new JSONObject(response.toString());
        return jsonResponse.getJSONArray("actors");
    }

    // Method to fetch actor details from IMDb API
    private static JSONObject fetchActorDetails(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        return new JSONObject(response.toString());
    }

    // Method to insert actor into Actor table if not already present
    private static void insertActor(Connection conn, String firstName, String lastName, String role, String bDate, String height, String gender, int salary) throws SQLException {
        String insertActorSql = "INSERT INTO Actor (Fname, Lname, Role, Bdate, Height, Gender, Salary) " +
                "SELECT ?, ?, ?, ?, ?, ?, ? FROM dual WHERE NOT EXISTS " +
                "(SELECT 1 FROM Actor WHERE Fname = ? AND Lname = ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertActorSql)) {
            pstmt.setString(1, firstName);
            pstmt.setString(2, lastName);
            pstmt.setString(3, role);

            // Check if birthdate is null or empty before formatting
            if (bDate == null || bDate.isEmpty()) {
                pstmt.setNull(4, java.sql.Types.DATE);
            } else {
                bDate = formatBirthdate(bDate); // Format birth date
                pstmt.setString(4, bDate);
            }

            // Format height without special characters
            String formattedHeight = formatHeight(height);
            pstmt.setString(5, formattedHeight);

            pstmt.setString(6, gender);
            pstmt.setInt(7, salary);
            pstmt.setString(8, firstName);
            pstmt.setString(9, lastName);

            // Execute SQL insert statement for Actor
            pstmt.executeUpdate();
        }
    }

    // Method to format height string without special characters
    private static String formatHeight(String height) {
        if (height == null || height.isEmpty()) {
            return null;
        }
        // Extract feet and inches from the height string
        String[] parts = height.split("\\s+");
        int feet = 0;
        int inches = 0;
        for (String part : parts) {
            if (part.contains("′")) {
                feet = Integer.parseInt(part.replace("′", ""));
            } else if (part.contains("″")) {
                String inchPart = part.replace("″", "");
                if (inchPart.contains("¼")) {
                    inches += 0.25;
                } else if (inchPart.contains("½")) {
                    inches += 0.5;
                } else if (inchPart.contains("¾")) {
                    inches += 0.75;
                } else {
                    inches += Integer.parseInt(inchPart);
                }
            }
        }

        // Convert feet and inches to the desired format
        return feet + "'" + inches + "\"";
    }

    private static String formatBirthdate(String birthDate) {
        if (birthDate == null || birthDate == "" || birthDate.isEmpty()) {
            return null;
        }
        // Define a custom DateTimeFormatter with the appropriate pattern
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.ENGLISH);

        // Parse the release date string using the custom formatter
        LocalDate parsedDate = LocalDate.parse(birthDate, formatter);

        // Format LocalDate to Oracle SQL date format (DD-MON-YY)
        return parsedDate.format(DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.ENGLISH));
    }

    private static String formatDBirthdate(String birthDate) {
        if (birthDate == "") {
            return null;
        }
        // Define a custom DateTimeFormatter with the appropriate pattern
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);

        // Parse the release date string using the custom formatter
        LocalDate parsedDate = LocalDate.parse(birthDate, formatter);

        // Format LocalDate to Oracle SQL date format (DD-MON-YY)
        return parsedDate.format(DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.ENGLISH));
    }

    // Method to insert relationship into Acts_In table
    private static void insertActIn(Connection conn, String actorFirstName, String actorLastName, String movieTitle) throws SQLException {
        String insertActsInSql = "INSERT INTO Acts_In (Actor_Fname, Actor_Lname, Movie_Title) " +
                "VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertActsInSql)) {
            pstmt.setString(1, actorFirstName);
            pstmt.setString(2, actorLastName);
            pstmt.setString(3, movieTitle);

            // Execute SQL insert statement for Acts_In
            pstmt.executeUpdate();
        }
    }

    // Method to fetch director details from IMDb API
    private static JSONObject fetchDirectorDetails(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        return new JSONObject(response.toString());
    }

    private static JSONArray fetchDirectors(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        JSONObject jsonResponse = new JSONObject(response.toString());

        // Extract director IDs from the full cast
        JSONArray directorsArray = jsonResponse.getJSONArray("directorList");
        JSONArray directorIds = new JSONArray();
        for (int i = 0; i < directorsArray.length(); i++) {
            JSONObject directorObj = directorsArray.getJSONObject(i);
            String directorId = directorObj.getString("id");
            directorIds.put(directorId);
        }

        return directorIds;
    }

    // Method to insert director into Director table if not already present
    private static void insertDirector(Connection conn, String firstName, String lastName, int salary, String bDate, String height, String gender) throws SQLException {
        String insertDirectorSql = "INSERT INTO Director (Fname, Lname, Salary, Bdate, Height, Gender) " +
                "SELECT ?, ?, ?, ?, ?, ? FROM dual WHERE NOT EXISTS " +
                "(SELECT 1 FROM Director WHERE Fname = ? AND Lname = ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertDirectorSql)) {
            pstmt.setString(1, firstName);
            pstmt.setString(2, lastName);

            // Set salary only if it's a valid number
            if (salary > 0) {
                pstmt.setInt(3, salary);
            } else {
                pstmt.setNull(3, java.sql.Types.INTEGER);
            }

            // Check if birthdate is null or empty before formatting
            if (bDate == null || bDate.isEmpty()) {
                pstmt.setNull(4, java.sql.Types.DATE);
            } else {
                pstmt.setString(4, bDate);
            }
            String formattedHeight = formatHeight(height);
            pstmt.setString(5, formattedHeight);
            pstmt.setString(6, gender);
            pstmt.setString(7, firstName);
            pstmt.setString(8, lastName);

            // Execute SQL insert statement for Director
            pstmt.executeUpdate();
        }
    }

    // Method to insert relationship into Directs table
    private static void insertDirects(Connection conn, String directorFirstName, String directorLastName, String movieTitle) throws SQLException {
        String insertDirectsSql = "INSERT INTO Directs (Director_Fname, Director_Lname, Movie_Title) " +
                "VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertDirectsSql)) {
            pstmt.setString(1, directorFirstName);
            pstmt.setString(2, directorLastName);
            pstmt.setString(3, movieTitle);

            // Execute SQL insert statement for Directs
            pstmt.executeUpdate();
        }
    }

    // Method to fetch writer details from IMDb API
    private static JSONObject fetchWriterDetails(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        return new JSONObject(response.toString());
    }

    // Method to insert writer into Writer table if not already present
    private static void insertWriter(Connection conn, String firstName, String lastName, String bDate, String height, int salary, String gender) throws SQLException {
        String insertWriterSql = "INSERT INTO Writer (Fname, Lname, Birth_date, Height, Salary, Gender) " +
                "SELECT ?, ?, TO_DATE(?, 'DD-MON-YY'), ?, ?, ? FROM dual WHERE NOT EXISTS " +
                "(SELECT 1 FROM Writer WHERE Fname = ? AND Lname = ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertWriterSql)) {
            pstmt.setString(1, firstName);
            pstmt.setString(2, lastName);

            // Check if birthdate is null or empty before formatting
            if (bDate == null || bDate.isEmpty()) {
                pstmt.setNull(3, java.sql.Types.DATE);
            } else {
                bDate = formatDBirthdate(bDate); // Format birth date
                pstmt.setString(3, bDate);
            }

            // Format height without special characters
            String formattedHeight = formatHeight(height);
            pstmt.setString(4, formattedHeight);

            // Set salary only if it's a valid number
            if (salary > 0) {
                pstmt.setInt(5, salary);
            } else {
                pstmt.setNull(5, java.sql.Types.INTEGER);
            }

            pstmt.setString(6, gender);
            pstmt.setString(7, firstName);
            pstmt.setString(8, lastName);

            // Execute SQL insert statement for Writer
            pstmt.executeUpdate();
        }
    }

    // Method to insert relationship into Writes table
    private static void insertWrites(Connection conn, String writerFirstName, String writerLastName, String movieTitle) throws SQLException {
        String insertWritesSql = "INSERT INTO Writes (Writer_Fname, Writer_Lname, Movie_Title) " +
                "VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertWritesSql)) {
            pstmt.setString(1, writerFirstName);
            pstmt.setString(2, writerLastName);
            pstmt.setString(3, movieTitle);

            // Execute SQL insert statement for Writes
            pstmt.executeUpdate();
        }
    }

    private static JSONArray fetchWriters(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        JSONObject jsonResponse = new JSONObject(response.toString());

        // Extract writer IDs from the writerList
        JSONArray writersArray = jsonResponse.getJSONArray("writerList");
        JSONArray writerIds = new JSONArray();
        for (int i = 0; i < writersArray.length(); i++) {
            JSONObject writerObj = writersArray.getJSONObject(i);
            String writerId = writerObj.getString("id");
            writerIds.put(writerId);
        }

        return writerIds;
    }

    private static JSONArray fetchGenre(String apiUrl) throws IOException, JSONException {
        // Create URL object
        URL url = new URL(apiUrl);

        // Create HTTP connection
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        // Read response data
        StringBuilder response = new StringBuilder();
        try (Scanner scanner = new Scanner(conn.getInputStream())) {
            while (scanner.hasNextLine()) {
                response.append(scanner.nextLine());
            }
        }

        // Parse JSON response
        JSONObject jsonResponse = new JSONObject(response.toString());

        // Extract genre list
        JSONArray genreArray = jsonResponse.getJSONArray("genreList");
        JSONArray genreList = new JSONArray();
        for (int i = 0; i < genreArray.length(); i++) {
            JSONObject genreObj = genreArray.getJSONObject(i);
            String genreName = genreObj.getString("key");
            genreList.put(genreName);
        }

        return genreArray;
    }

    private static void insertGenre(Connection conn, String movie_title, String genre) throws SQLException {
        String insertDirectsSql = "INSERT INTO Genre (Movie_Name, Genre) " +
                "VALUES (?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(insertDirectsSql)) {
            pstmt.setString(1, movie_title);
            pstmt.setString(2, genre);

            // Execute SQL insert statement for Directs
            pstmt.executeUpdate();
        }
    }




}



