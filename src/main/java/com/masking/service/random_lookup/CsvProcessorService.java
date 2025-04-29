package com.masking.service.random_lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.random_lookup.RandomLookupStore;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class CsvProcessorService {

  public ValidationResponse validateAndGenerateCsv(
      MultipartFile sourceFile, MultipartFile lookupFile, RandomLookupStore randomLookupStore) {
    List<String> messages = new ArrayList<>();

    try {
      // Extract headers from lookup CSV
      Set<String> lookupHeaders = extractHeaders(lookupFile);

      // Validate lookupValueColumns against lookup CSV headers
      boolean missingLookupColumn = false;
      for (String col : randomLookupStore.getLookupValueColumns()) {
        if (!lookupHeaders.contains(col)) {
          messages.add("❌ Missing lookup value column in lookup CSV: " + col);
          missingLookupColumn = true;
        }
      }
      if (missingLookupColumn) {
        return new ValidationResponse("FAILED", messages);
      }

      // Extract headers from source CSV
      Set<String> sourceHeaders = extractHeaders(sourceFile);

      // Validate destinationColumns against source CSV headers
      boolean missingDestColumn = false;
      for (String destCol : randomLookupStore.getDestinationColumns()) {
        if (!sourceHeaders.contains(destCol)) {
          messages.add("❌ Missing destination column in source CSV: " + destCol);
          missingDestColumn = true;
        }
      }
      if (missingDestColumn) {
        return new ValidationResponse("FAILED", messages);
      }

      // If no missing columns, proceed to generate output CSV
      return generateOutputCsv(sourceFile, lookupFile, randomLookupStore);

    } catch (Exception e) {
      messages.add("❌ Error: " + e.getMessage());
      return new ValidationResponse("FAILED", messages);
    }
  }

  public ValidationResponse generateOutputCsv(
      MultipartFile sourceFile, MultipartFile lookupFile, RandomLookupStore randomLookupStore) {
    List<String> messages = new ArrayList<>();

    try {
      List<String> sourceHeaders = extractHeadersList(sourceFile);
      List<Map<String, String>> lookupData = extractLookupData(lookupFile);

      String timestamp = new SimpleDateFormat("ddHHmmss").format(new Date());
      String outputFilePath = "random_lookup/output_" + timestamp + ".csv";

      Files.createDirectories(Paths.get("random_lookup"));

      try (BufferedReader sourceReader =
              new BufferedReader(
                  new InputStreamReader(sourceFile.getInputStream(), StandardCharsets.UTF_8));
          CSVParser sourceParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(sourceReader);
          BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath));
          CSVPrinter csvPrinter =
              new CSVPrinter(
                  writer, CSVFormat.DEFAULT.withHeader(sourceHeaders.toArray(new String[0])))) {

        Random random = new Random();

        for (CSVRecord sourceRecord : sourceParser) {
          List<String> row = new ArrayList<>();

          for (String header : sourceHeaders) {
            if (randomLookupStore.getDestinationColumns().contains(header)) {
              // Destination column: Perform random picking 'limit' times
              String randomValue = "";
              int limit = randomLookupStore.getLimit();
              if (randomLookupStore.getLimit() == 0) {
                limit = 1;
              }
              for (int i = 0; i < limit; i++) {
                Map<String, String> randomLookupRow =
                    lookupData.get(random.nextInt(lookupData.size()));
                randomValue =
                    randomLookupRow.get(
                        randomLookupStore
                            .getLookupValueColumns()
                            .get(0)); // Assuming one lookup column for simplicity
              }
              row.add(randomValue);
            } else {
              // Non-destination column: Copy source value
              row.add(sourceRecord.get(header));
            }
          }

          csvPrinter.printRecord(row);
        }

        messages.add("✅ Output file generated successfully: " + outputFilePath);
        return new ValidationResponse("SUCCESS", messages);

      } catch (IOException e) {
        messages.add("❌ Error generating CSV file: " + e.getMessage());
        return new ValidationResponse("FAILED", messages);
      }

    } catch (IOException e) {
      messages.add("❌ Error reading files: " + e.getMessage());
      return new ValidationResponse("FAILED", messages);
    }
  }

  private Set<String> extractHeaders(MultipartFile file) throws IOException {
    try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
      return parser.getHeaderMap().keySet();
    }
  }

  private List<String> extractHeadersList(MultipartFile file) throws IOException {
    try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
      return new ArrayList<>(parser.getHeaderMap().keySet());
    }
  }

  private List<Map<String, String>> extractLookupData(MultipartFile file) throws IOException {
    List<Map<String, String>> lookupData = new ArrayList<>();
    try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

      for (CSVRecord record : parser) {
        Map<String, String> row = new HashMap<>();
        for (String header : parser.getHeaderMap().keySet()) {
          row.put(header, record.get(header));
        }
        lookupData.add(row);
      }
    }
    return lookupData;
  }
}
