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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api")
public class LookUpController {

  private static final Logger logger = LoggerFactory.getLogger(LookUpController.class);

  @Autowired private LookUpFunctionValidator lookUpFunctionValidator;
  @Autowired private RandomLookupFunctionValidator randomLookupFunctionValidator;
  @Autowired private HashLookupFunctionValidator hashLookupFunctionValidator;

  @Autowired private CsvColumnValidatorService csvColumnValidatorService;
  @Autowired private CsvOutputGenerator csvOutputGenerator;
  @Autowired private CsvProcessorService csvProcessorService;
  @Autowired private HashLookupCsvGenerator hashLookupCsvGenerator;

  @Autowired private LookUpStore lookUpStore;
  @Autowired private RandomLookupStore randomLookupStore;
  @Autowired private HashLookupStore hashLookupStore;

  @Async
  @PostMapping(value = "/lookup", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public CompletableFuture<ResponseEntity<ValidationResponse>> validateLookupFunction(
      @RequestParam("sourceCsvPath") MultipartFile sourceCsv,
      @RequestParam("lookupCsvPath") MultipartFile lookupCsv,
      @RequestParam("lookupFunction") String function) {

    logger.info("Received /lookup request with function: {}", function);
    try {
      ValidationResponse functionValidation =
          lookUpFunctionValidator.validateAndExtract(function, lookUpStore);
      if (isInvalid(functionValidation)) {
        logger.warn("Function validation failed: {}", functionValidation.getMessages());
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(functionValidation));
      }

      ValidationResponse columnValidation =
          csvColumnValidatorService.validateColumns(sourceCsv, lookupCsv, lookUpStore);
      if (isInvalid(columnValidation)) {
        logger.warn("Column validation failed: {}", columnValidation.getMessages());
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(columnValidation));
      }

      logger.info("Generating output CSV for /lookup...");
      return CompletableFuture.completedFuture(
          buildCsvResponse(
              csvOutputGenerator.generateOutputCsv(sourceCsv, lookupCsv, lookUpStore)));

    } catch (Exception e) {
      logger.error("Error processing /lookup request", e);
      return CompletableFuture.completedFuture(handleException(e));
    }
  }

  @Async
  @PostMapping(value = "/random_lookup", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public CompletableFuture<ResponseEntity<ValidationResponse>> validateRandomLookupFunction(
      @RequestParam("sourceCsvPath") MultipartFile sourceCsv,
      @RequestParam("lookupCsvPath") MultipartFile lookupCsv,
      @RequestParam("randomLookupFunction") String function) {

    logger.info("Received /random_lookup request with function: {}", function);
    try {
      ValidationResponse functionValidation =
          randomLookupFunctionValidator.validateAndExtract(function, randomLookupStore);
      if (isInvalid(functionValidation)) {
        logger.warn(
            "Random lookup function validation failed: {}", functionValidation.getMessages());
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(functionValidation));
      }

      ValidationResponse processValidation =
          csvProcessorService.validateAndGenerateCsv(sourceCsv, lookupCsv, randomLookupStore);
      if (isInvalid(processValidation)) {
        logger.warn("Random lookup CSV validation failed: {}", processValidation.getMessages());
        return CompletableFuture.completedFuture(
            ResponseEntity.badRequest().body(processValidation));
      }

      logger.info("Generating output CSV for /random_lookup...");
      return CompletableFuture.completedFuture(
          buildCsvResponse(
              csvProcessorService.generateOutputCsv(sourceCsv, lookupCsv, randomLookupStore)));

    } catch (Exception e) {
      logger.error("Error processing /random_lookup request", e);
      return CompletableFuture.completedFuture(handleException(e));
    }
  }

  @Async
  @PostMapping(value = "/hash_lookup", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  public CompletableFuture<ResponseEntity<ValidationResponse>> validateHashLookupFunction(
      @RequestParam("sourceCsvPath") MultipartFile sourceCsv,
      @RequestParam("lookupCsvPath") MultipartFile lookupCsv,
      @RequestParam("hashLookupFunction") String function) {

    logger.info("Received /hash_lookup request with function: {}", function);
    ValidationResponse functionValidation =
        hashLookupFunctionValidator.validateAndExtract(function);
    if (!"SUCCESS".equalsIgnoreCase(functionValidation.getStatus())) {
      logger.warn("Hash lookup function validation failed: {}", functionValidation.getMessages());
      return CompletableFuture.completedFuture(
          ResponseEntity.badRequest().body(functionValidation));
    }

    try {
      logger.info("Generating output CSV for /hash_lookup...");
      return CompletableFuture.completedFuture(
          buildCsvResponse(hashLookupCsvGenerator.process(sourceCsv, lookupCsv, hashLookupStore)));
    } catch (Exception e) {
      logger.error("Error processing /hash_lookup request", e);
      return CompletableFuture.completedFuture(handleException(e));
    }
  }

  private boolean isInvalid(ValidationResponse response) {
    return response.getMessages() != null && !response.getMessages().isEmpty();
  }

  private ResponseEntity<ValidationResponse> buildCsvResponse(ValidationResponse response) {
    if ("SUCCESS".equalsIgnoreCase(response.getStatus())) {
      String outputPath = response.getMessages().get(0);
      logger.info("CSV generated successfully at: {}", outputPath);
      return ResponseEntity.ok(
          new ValidationResponse("SUCCESS", List.of("File Generated" + outputPath)));
    }
    logger.error("CSV generation failed: {}", response.getMessages());
    return ResponseEntity.status(500).body(response);
  }

  private ResponseEntity<ValidationResponse> handleException(Exception e) {
    logger.error("Unhandled exception: {}", e.getMessage(), e);
    return ResponseEntity.status(500)
        .body(new ValidationResponse("ERROR", List.of("Internal Server Error: " + e.getMessage())));
  }
}
