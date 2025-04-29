package com.masking.controller;

import com.masking.component.ValidationResponse;
import com.masking.model.hash_lookup.HashLookupStore;
import com.masking.model.lookup.LookUpStore;
import com.masking.model.random_lookup.RandomLookupStore;
import com.masking.service.hash_lookup.HashLookupCsvGenerator;
import com.masking.service.hash_lookup.HashLookupFunctionValidator;
import com.masking.service.lookup.CsvColumnValidatorService;
import com.masking.service.lookup.CsvOutputGenerator;
import com.masking.service.lookup.LookUpFunctionValidator;
import com.masking.service.random_lookup.CsvProcessorService;
import com.masking.service.random_lookup.RandomLookupFunctionValidator;
import io.swagger.v3.oas.annotations.Operation;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api")
public class LookUpController {

  @Autowired private LookUpFunctionValidator lookUpFunctionValidator;
  @Autowired private CsvColumnValidatorService csvColumnValidatorService;
  @Autowired private CsvOutputGenerator csvOutputGenerator;
  @Autowired private LookUpStore lookUpStore;
  @Autowired private RandomLookupStore randomLookupStore;
  @Autowired private RandomLookupFunctionValidator randomLookupFunctionValidator;
  @Autowired private CsvProcessorService csvProcessorService;

  // Asynchronous method to validate lookup function
  @Async
  @Operation(
      summary = "Validate lookup function",
      description =
          "Validates the lookup function and generate the CSV files using various lookup function")
  @PostMapping(value = "/lookup", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public CompletableFuture<ResponseEntity<ValidationResponse>> validateLookupFunction(
      @RequestParam("sourceCsvPath") MultipartFile sourceCsvPath,
      @RequestParam("lookupCsvPath") MultipartFile lookupCsvPath,
      @RequestParam("lookupFunction") String lookupFunction) {

    try {
      ValidationResponse functionResponse =
          validateAndExtractLookupFunction(lookupFunction, lookUpStore);
      if (isInvalidResponse(functionResponse)) {
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(functionResponse));
      }

      ValidationResponse columnValidationResponse =
          csvColumnValidatorService.validateColumns(sourceCsvPath, lookupCsvPath, lookUpStore);
      if (isInvalidResponse(columnValidationResponse)) {
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(columnValidationResponse));
      }

      return CompletableFuture.completedFuture(generateOutputCsv(sourceCsvPath, lookupCsvPath));
    } catch (Exception e) {
      // Log error and handle the exception
      return CompletableFuture.completedFuture(handleError(e));
    }
  }

  // Asynchronous method to validate random lookup function
  @Async
  @Operation(
      summary = "Validate random lookup function",
      description =
          "VValidates the random lookup function and generate the CSV files using various random lookup function")
  @PostMapping(value = "/random_lookup", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public CompletableFuture<ResponseEntity<ValidationResponse>> validateRandomLookupFunction(
      @RequestParam("sourceCsvPath") MultipartFile sourceCsvPath,
      @RequestParam("lookupCsvPath") MultipartFile lookupCsvPath,
      @RequestParam("randomLookupFunction") String randomLookupFunction) {

    try {
      ValidationResponse functionResponse =
          validateAndExtractRandomLookupFunction(randomLookupFunction, randomLookupStore);
      if (isInvalidResponse(functionResponse)) {
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(functionResponse));
      }

      ValidationResponse columnValidationResponse =
          csvProcessorService.validateAndGenerateCsv(
              sourceCsvPath, lookupCsvPath, randomLookupStore);
      if (isInvalidResponse(columnValidationResponse)) {
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(columnValidationResponse));
      }

      return CompletableFuture.completedFuture(
          generateRandomLookupCsv(sourceCsvPath, lookupCsvPath));
    } catch (Exception e) {
      // Log error and handle the exception
      return CompletableFuture.completedFuture(handleError(e));
    }
  }

  @Autowired private HashLookupFunctionValidator hashLookupFunctionValidator;

  @Autowired private HashLookupCsvGenerator hashLookupCsvGenerator;

  @Autowired private HashLookupStore hashLookupStore;

  @Async
  @Operation(
      summary = "Validate hash lookup function",
      description =
          "Validates the hash lookup function and generates the CSV files using various hash lookup function")
  @PostMapping(value = "/hash_lookup", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public CompletableFuture<ResponseEntity<ValidationResponse>> validateHashLookupFunction(
      @RequestParam("sourceCsvPath") MultipartFile sourceCsvPath,
      @RequestParam("lookupCsvPath") MultipartFile lookupCsvPath,
      @RequestParam("hashLookupFunction") String hashLookupFunction) {

    ValidationResponse functionValidationResponse =
        hashLookupFunctionValidator.validateAndExtract(hashLookupFunction);

    if (functionValidationResponse.getStatus().equalsIgnoreCase("SUCCESS")) {
      try {
        ValidationResponse processValidationResponse =
            hashLookupCsvGenerator.process(sourceCsvPath, lookupCsvPath, hashLookupStore);
        return CompletableFuture.completedFuture(
            ResponseEntity.ok().body(processValidationResponse));
      } catch (Exception e) {
        throw new RuntimeException("Error during CSV processing", e);
      }
    }

    // Return function validation errors
    return CompletableFuture.completedFuture(
        ResponseEntity.badRequest().body(functionValidationResponse));
  }

  // Common function to validate lookup functions
  private ValidationResponse validateAndExtractLookupFunction(
      String lookupFunction, LookUpStore lookUpStore) {
    return lookUpFunctionValidator.validateAndExtract(lookupFunction, lookUpStore);
  }

  // Common function to validate random lookup functions
  private ValidationResponse validateAndExtractRandomLookupFunction(
      String randomLookupFunction, RandomLookupStore randomLookupStore) {
    return randomLookupFunctionValidator.validateAndExtract(
        randomLookupFunction, randomLookupStore);
  }

  // Helper function to check if response is invalid
  private boolean isInvalidResponse(ValidationResponse response) {
    return response.getMessages() != null && !response.getMessages().isEmpty();
  }

  // Generate CSV output for Lookup
  private ResponseEntity<ValidationResponse> generateOutputCsv(
      MultipartFile sourceCsvPath, MultipartFile lookupCsvPath) {
    ValidationResponse outputResponse =
        csvOutputGenerator.generateOutputCsv(sourceCsvPath, lookupCsvPath, lookUpStore);
    return buildResponse(outputResponse);
  }

  // Generate CSV output for Random Lookup
  private ResponseEntity<ValidationResponse> generateRandomLookupCsv(
      MultipartFile sourceCsvPath, MultipartFile lookupCsvPath) {
    ValidationResponse outputCsvResponse =
        csvProcessorService.generateOutputCsv(sourceCsvPath, lookupCsvPath, randomLookupStore);
    return buildResponse(outputCsvResponse);
  }

  // Helper method to construct the response
  private ResponseEntity<ValidationResponse> buildResponse(ValidationResponse response) {
    if ("SUCCESS".equals(response.getStatus())) {
      String outputFilePath =
          response.getMessages().get(0); // Assuming message contains the file path
      return ResponseEntity.ok(
          new ValidationResponse("SUCCESS", List.of("File generated \n " + outputFilePath)));
    }
    return ResponseEntity.status(500).body(response);
  }

  // Method to handle errors globally
  private ResponseEntity<ValidationResponse> handleError(Exception e) {
    // You can log the exception here
    e.printStackTrace();
    return ResponseEntity.status(500)
        .body(new ValidationResponse("ERROR", List.of("Internal Server Error: " + e.getMessage())));
  }
}
