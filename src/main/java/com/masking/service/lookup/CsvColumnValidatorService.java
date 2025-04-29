package com.masking.service.lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.lookup.LookUpStore;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class CsvColumnValidatorService {

  private final ExecutorService executorService =
      Executors.newFixedThreadPool(
          2); // Thread pool to process source and lookup files concurrently

  public ValidationResponse validateColumns(
      MultipartFile sourceFile, MultipartFile lookupFile, LookUpStore lookUpStore) {
    List<String> errors =
        Collections.synchronizedList(
            new ArrayList<>()); // Synchronized list to handle thread safety

    // CompletableFuture to process files concurrently
    CompletableFuture<Void> sourceValidation =
        CompletableFuture.runAsync(
            () -> {
              try {
                // Validate source file columns
                Set<String> sourceHeaders = extractHeaders(sourceFile);
                validateMissingColumns(
                    sourceHeaders, lookUpStore.getSourceSearchColumns(), "source", errors);
              } catch (Exception e) {
                errors.add("Error reading source CSV file: " + e.getMessage());
              }
            },
            executorService);

    CompletableFuture<Void> lookupValidation =
        CompletableFuture.runAsync(
            () -> {
              try {
                // Validate lookup file columns
                Set<String> lookupHeaders = extractHeaders(lookupFile);
                validateMissingColumns(
                    lookupHeaders, lookUpStore.getLookupSearchColumns(), "lookup search", errors);
                validateMissingColumns(
                    lookupHeaders, lookUpStore.getLookupValueColumns(), "lookup value", errors);
              } catch (Exception e) {
                errors.add("Error reading lookup CSV file: " + e.getMessage());
              }
            },
            executorService);

    // Wait for both validations to complete
    CompletableFuture<Void> combinedFuture =
        CompletableFuture.allOf(sourceValidation, lookupValidation);

    try {
      combinedFuture.get(); // Ensure both tasks are completed before proceeding
    } catch (InterruptedException | ExecutionException e) {
      errors.add("Error in concurrent execution: " + e.getMessage());
    }

    return errors.isEmpty()
        ? new ValidationResponse("SUCCESS", null)
        : new ValidationResponse("FAILED", errors);
  }

  private void validateMissingColumns(
      Set<String> headers, List<String> columns, String columnType, List<String> errors) {
    // Convert headers to lowercase for case-insensitive comparison
    Set<String> normalizedHeaders =
        headers.stream().map(String::toLowerCase).collect(Collectors.toSet());

    // Normalize the expected column names to lowercase as well
    List<String> normalizedColumns = columns.stream().map(String::toLowerCase).toList();

    for (String col : normalizedColumns) {
      if (!normalizedHeaders.contains(col)) {
        errors.add("Missing " + columnType + " column in CSV: " + col);
      }
    }
  }

  private Set<String> extractHeaders(MultipartFile file) throws Exception {
    try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
        CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {
      return parser.getHeaderMap().keySet().stream()
          .map(String::trim)
          .map(String::toLowerCase) // Normalize headers to lowercase
          .collect(Collectors.toSet());
    }
  }
}
