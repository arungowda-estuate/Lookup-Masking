package com.masking.service.hash_lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.hash_lookup.HashLookupStore;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class HashLookupCsvGenerator {

  public ValidationResponse process(
      MultipartFile sourceCsv, MultipartFile lookupCsv, HashLookupStore hashLookupStore)
      throws Exception {
    ValidationResponse validationResponse = new ValidationResponse();
    String outputDir = "hash_lookup";
    Files.createDirectories(Paths.get(outputDir));
    String timestamp = new SimpleDateFormat("ddHHmmss").format(new Date());
    String outputCsvPath = outputDir + "/output_" + timestamp + ".csv";

    List<Map<String, String>> sourceData = readCsv(sourceCsv);
    List<Map<String, String>> lookupData = readCsv(lookupCsv);
    System.out.println("Source Data: " + sourceData);
    System.out.println("Lookup Data: " + lookupData);

    Map<String, Map<String, String>> lookupMap = buildLookupMap(lookupData, hashLookupStore);
    List<String> outputHeader =
        prepareOutputHeader(sourceData, hashLookupStore, validationResponse);
    List<Map<String, String>> outputRows = new ArrayList<>();

    for (Map<String, String> sourceRow : sourceData) {
      String hashedKey = hashValue(sourceRow, lookupData, hashLookupStore);
      String reversedLookupColumnValue =
          resolveHashToLookupValue(hashedKey, lookupData, hashLookupStore);
      System.out.println("Hashed Key: " + hashedKey);
      System.out.println("Source Key: " + reversedLookupColumnValue);

      Map<String, String> matchedRow =
          getMatchedRow(reversedLookupColumnValue, lookupMap, lookupData, hashLookupStore);
      Map<String, String> outputRow = createOutputRow(sourceRow, matchedRow, hashLookupStore);
      System.out.println("Output Row: " + outputRow);

      outputRows.add(outputRow);
    }

    writeCsv(outputCsvPath, outputHeader, outputRows);
    validationResponse.setStatus("Success");
    validationResponse.setMessages(
        List.of("CSV processing completed successfully: " + outputCsvPath));
    resetColumnStore(hashLookupStore);
    return validationResponse;
  }

  private Map<String, Map<String, String>> buildLookupMap(
      List<Map<String, String>> lookupData, HashLookupStore hashLookupStore) {
    Map<String, Map<String, String>> lookupMap = new HashMap<>();
    if (Boolean.TRUE.equals(hashLookupStore.getCacheEnabled())) {
      for (Map<String, String> row : lookupData) {
        String key = buildKey(row, hashLookupStore.getLookupSearchColumns(), hashLookupStore);
        lookupMap.put(key, row);
      }
    }
    return lookupMap;
  }

  private List<String> prepareOutputHeader(
      List<Map<String, String>> sourceData,
      HashLookupStore hashLookupStore,
      ValidationResponse validationResponse) {
    List<String> outputHeader = new ArrayList<>(sourceData.get(0).keySet());
    outputHeader.removeAll(hashLookupStore.getSourceSearchColumns());

    if (hashLookupStore.getDestinationColumns().isEmpty()) {
      outputHeader.addAll(hashLookupStore.getLookupValueColumns());
      validationResponse.setMessages(List.of("Using lookup value columns as destination columns"));
    } else {
      outputHeader.addAll(hashLookupStore.getDestinationColumns());
    }

    return outputHeader;
  }

  private Map<String, String> getMatchedRow(
      String hashedKey,
      Map<String, Map<String, String>> lookupMap,
      List<Map<String, String>> lookupData,
      HashLookupStore hashLookupStore) {
    if (Boolean.TRUE.equals(hashLookupStore.getCacheEnabled())) {
      return lookupMap.get(hashedKey);
    } else {
      for (Map<String, String> row : lookupData) {
        String key = buildKey(row, hashLookupStore.getLookupSearchColumns(), hashLookupStore);
        if (key.equals(hashedKey)) return row;
      }
    }
    return null;
  }

  private Map<String, String> createOutputRow(
      Map<String, String> sourceRow,
      Map<String, String> matchedRow,
      HashLookupStore hashLookupStore) {
    Map<String, String> outputRow = new HashMap<>();
    for (String col : sourceRow.keySet()) {
      if (!hashLookupStore.getSourceSearchColumns().contains(col)) {
        outputRow.put(col, sourceRow.get(col));
      }
    }

    List<String> destinationCols =
        !hashLookupStore.getDestinationColumns().isEmpty()
            ? hashLookupStore.getDestinationColumns()
            : hashLookupStore.getLookupValueColumns();

    for (int i = 0; i < destinationCols.size(); i++) {
      String destCol = destinationCols.get(i);
      String lookupCol = hashLookupStore.getLookupValueColumns().get(i);
      String value = matchedRow != null ? matchedRow.getOrDefault(lookupCol, "") : "";
      outputRow.put(destCol, value);
    }

    return outputRow;
  }

  private void writeCsv(String path, List<String> headers, List<Map<String, String>> data)
      throws Exception {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(path)))) {
      writer.write(String.join(",", headers));
      writer.newLine();
      for (Map<String, String> row : data) {
        List<String> values = new ArrayList<>();
        for (String header : headers) {
          values.add(row.getOrDefault(header, ""));
        }
        writer.write(String.join(",", values));
        writer.newLine();
      }
    }
  }

  private List<Map<String, String>> readCsv(MultipartFile file) throws Exception {
    List<Map<String, String>> result = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
      String[] headers = reader.readLine().split(",");
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split(",", -1);
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
          row.put(headers[i], i < values.length ? values[i] : "");
        }
        result.add(row);
      }
    }
    return result;
  }

  private String buildKey(
      Map<String, String> row, List<String> columns, HashLookupStore hashLookupStore) {
    StringBuilder sb = new StringBuilder();
    for (String col : columns) {
      String val = row.getOrDefault(col, "");
      if ("TRIM".equalsIgnoreCase(hashLookupStore.getTrimCharacters())) val = val.trim();
      if ("UPPER".equalsIgnoreCase(hashLookupStore.getTrimCharacters())) val = val.toUpperCase();
      sb.append(val);
    }
    return sb.toString();
  }

  private String hashValue(
      Map<String, String> sourceRow,
      List<Map<String, String>> lookupData,
      HashLookupStore hashLookupStore)
      throws Exception {
    // Step 1: Get raw key from source row
    String sourceKey =
        buildKey(sourceRow, hashLookupStore.getSourceSearchColumns(), hashLookupStore);

    // Step 2: Hash using SHA-256
    String finalInput =
        (hashLookupStore.getSeed() != null ? hashLookupStore.getSeed() : "") + sourceKey;
    MessageDigest md =
        MessageDigest.getInstance(
            hashLookupStore.getAlgorithm() != null ? hashLookupStore.getAlgorithm() : "SHA-256");
    byte[] hash = md.digest(finalInput.getBytes("UTF-8"));

    // Step 3: Convert to hex string
    StringBuilder hex = new StringBuilder();
    for (byte b : hash) hex.append(String.format("%02x", b));
    return hex.toString();
  }

  private String resolveHashToLookupValue(
      String hashedKey, List<Map<String, String>> lookupData, HashLookupStore hashLookupStore)
      throws Exception {
    for (Map<String, String> lookupRow : lookupData) {
      // Build key from lookup row
      String lookupKey =
          buildKey(lookupRow, hashLookupStore.getLookupSearchColumns(), hashLookupStore);

      // Apply hashing logic (same as hashValue)
      String finalInput =
          (hashLookupStore.getSeed() != null ? hashLookupStore.getSeed() : "") + lookupKey;
      MessageDigest md =
          MessageDigest.getInstance(
              hashLookupStore.getAlgorithm() != null ? hashLookupStore.getAlgorithm() : "SHA-256");
      byte[] hash = md.digest(finalInput.getBytes("UTF-8"));

      // Convert to hex string
      StringBuilder hex = new StringBuilder();
      for (byte b : hash) hex.append(String.format("%02x", b));
      String calculatedHash = hex.toString();

      // If match found, return the original lookup key
      if (calculatedHash.equals(hashedKey)) {
        return lookupKey; // or return a string representation of original values if needed
      }
    }

    return null; // No match found
  }

  public static void resetColumnStore(HashLookupStore hashLookupStore) {
    hashLookupStore.setSourceSearchColumns(new ArrayList<>());
    hashLookupStore.setDestinationColumns(new ArrayList<>());
    hashLookupStore.setLookupTableName(null);
    hashLookupStore.setLookupSearchColumns(new ArrayList<>());
    hashLookupStore.setLookupValueColumns(new ArrayList<>());
    hashLookupStore.setTrimCharacters(null);
    hashLookupStore.setAlgorithm(null);
    hashLookupStore.setSeed(null);
    hashLookupStore.setPreserveOptions(new ArrayList<>());
    hashLookupStore.setCacheEnabled(false);
  }
}
