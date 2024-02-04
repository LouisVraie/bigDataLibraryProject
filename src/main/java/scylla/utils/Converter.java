package scylla.utils;

import org.json.JSONArray;
import org.json.JSONObject;

import scylla.type.Author;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Converter {
    /**
     * Convert UUID to int
     * @param value Int value of the ID
     * @param tableName String value of the table name
     * @return int
     */
    public static UUID intToUUID(int value, String tableName) {
        // Concatenate tableName and integer value
        String combinedString = tableName + Integer.toString(value);

        // Convert combinedString to bytes
        byte[] bytes = combinedString.getBytes();

        // Generate UUID from bytes
        UUID uuid = UUID.nameUUIDFromBytes(bytes);
        return uuid;
    }

    
    /**
     * Format date in d/M/yyyy format to yyyy-MM-dd format
     * @param date String value of the date
     * @return String 
     */
    public static String formatDate(String date) {
        return formatDate(date, "d/M/yyyy");
    }

    /**
     * Format date in the given pattern to yyyy-MM-dd format
     * @param date String value of the date
     * @return String
     */
    public static String formatDate(String date, String pattern) {
        // Définir le format d'entrée
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern(pattern);

        // Convertir la date en LocalDate
        LocalDate localDate = LocalDate.parse(date, inputFormatter);

        // Définir le format de sortie
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // Formater la date en chaîne de caractères
        String formattedDate = localDate.format(outputFormatter);

        return formattedDate;
    }

    public static Instant stringToDate(String dateString) {

        if(dateString.isEmpty()) {
            return null;
        }
        // Convertir la chaîne de date en un objet LocalDate
        LocalDate localDate = LocalDate.parse(dateString);

        // Convertir l'objet LocalDate en un objet java.time.Instant
        Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);

        return instant;
    }

    public static Set<Map<String, String>> stringToAuthors(String authors) {

        Set<Map<String, String>> authorSet = new HashSet<>();

        try {
            JSONArray jsonArray = new JSONArray(authors);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                Map<String, String> authorMap = new HashMap<>();

                // Extract author information from JSON object
                String firstName = jsonObject.optString("firstname", "");
                String lastName = jsonObject.optString("lastname", "");

                // Add author information to the map
                authorMap.put("firstname", firstName);
                authorMap.put("lastname", lastName);

                // Add the map to the set
                authorSet.add(authorMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return authorSet;
    }

    public static Set<String> stringToSet(String key) {

        Set<String> stringSet = new HashSet<>();

        try {
            JSONArray jsonArray = new JSONArray(key);

            for (int i = 0; i < jsonArray.length(); i++) {
                String value = jsonArray.getString(i);
                stringSet.add(value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return stringSet;
    }

    public static String clearLine(String line) {
        // if the line start with a [ 
        if (line.startsWith("[")) {
            line = line.substring(1);
            return line;
        }
        // if the line  end with a ], remove it
        if (line.endsWith("]")) {
            line = line.substring(0, line.length() - 1);
            return line;
        }
        return line;
    }

    public static JSONArray getJSONFromFile(String filepath){
        // Read JSON data from file
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filepath));
            StringBuilder jsonData = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonData.append(line);
            }
            reader.close();

            // Parse JSON array
            return new JSONArray(jsonData.toString());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
