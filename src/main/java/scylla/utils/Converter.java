package scylla.utils;

import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
