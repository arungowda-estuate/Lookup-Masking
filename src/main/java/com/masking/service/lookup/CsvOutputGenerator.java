package com.masking.service.lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.lookup.LookUpStore;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class CsvOutputGenerator {

  public ValidationResponse generateOutputCsv(
      MultipartFile sourceCsv, MultipartFile lookupCsv, LookUpStore lookUpStore) {

    ValidationResponse response = new ValidationResponse();

    try (Reader sourceReader = new InputStreamReader(sourceCsv.getInputStream());
        Reader lookupReader = new InputStreamReader(lookupCsv.getInputStream())) {

      List<String> sourceSearch = lookUpStore.getSourceSearchColumns();
      List<String> lookupSearch = lookUpStore.getLookupSearchColumns();
      List<String> lookupValue = lookUpStore.getLookupValueColumns();
      List<String> destinationColumns = lookUpStore.getDestinationColumns();

      Iterable<CSVRecord> sourceRecords =
          CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(sourceReader);
      Iterable<CSVRecord> lookupRecords =
          CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(lookupReader);

      Map<String, Map<String, String>> lookupMap = new HashMap<>();

      // Create lookup map from lookup CSV
      for (CSVRecord record : lookupRecords) {
        String key = buildKey(record, lookupSearch);
        Map<String, String> valueMap = new HashMap<>();
        for (String valCol : lookupValue) {
          valueMap.put(valCol, record.get(valCol));
        }
        lookupMap.put(key, valueMap);
      }

      // Prepare output folder and filename
      String timestamp = new SimpleDateFormat("ddHHmmss").format(new Date());
      String outputFilePath = "output/output_" + timestamp + ".csv";
      Files.createDirectories(Paths.get("output"));
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath));

      Iterator<CSVRecord> sourceIterator = sourceRecords.iterator();
      if (!sourceIterator.hasNext()) {
        response.setStatus("Error: Source CSV is empty.");
        return response;
      }

      CSVRecord firstRecord = sourceIterator.next();
      List<String> sourceHeaders = new ArrayList<>(firstRecord.toMap().keySet());

      // Remove sourceSearchColumns from headers
      sourceHeaders.removeAll(sourceSearch);

      // Add destination columns if present, else lookup value columns
      List<String> extraColumns;
      if (destinationColumns != null && !destinationColumns.isEmpty()) {
        extraColumns = destinationColumns;
      } else {
        extraColumns = lookupValue;
      }

      List<String> outputHeaders = new ArrayList<>(sourceHeaders);
      outputHeaders.addAll(extraColumns);

      CSVPrinter printer =
          new CSVPrinter(
              writer, CSVFormat.DEFAULT.withHeader(outputHeaders.toArray(new String[0])));

      // Process first record separately
      processRecord(
          printer,
          firstRecord,
          sourceSearch,
          sourceHeaders,
          lookupMap,
          extraColumns,
          sourceSearch,
          lookupSearch);

      // Process remaining records
      while (sourceIterator.hasNext()) {
        CSVRecord srcRecord = sourceIterator.next();
        processRecord(
            printer,
            srcRecord,
            sourceSearch,
            sourceHeaders,
            lookupMap,
            extraColumns,
            sourceSearch,
            lookupSearch);
      }

      printer.flush();
      printer.close();

      response.setStatus("SUCCESS");
      response.setMessages(List.of("CSV generation successful: " + outputFilePath));
      resetColumnStore(lookUpStore);

    } catch (Exception e) {
      response.setStatus("Error during CSV generation");
      response.setMessages(Collections.singletonList(e.getMessage()));
    }

    return response;
  }

  private void processRecord(
      CSVPrinter printer,
      CSVRecord srcRecord,
      List<String> sourceSearch,
      List<String> sourceHeaders,
      Map<String, Map<String, String>> lookupMap,
      List<String> extraColumns,
      List<String> sourceSearchColumns,
      List<String> lookupSearchColumns) {

    Map<String, String> row = new LinkedHashMap<>(srcRecord.toMap());

    // Build source key
    String srcKey = buildKey(srcRecord, sourceSearchColumns);

    // Remove sourceSearch columns from row
    for (String col : sourceSearch) {
      row.remove(col);
    }

    // Prepare the output row
    List<String> outputRow = new ArrayList<>();

    // Add source columns values
    for (String col : sourceHeaders) {
      outputRow.add(row.getOrDefault(col, ""));
    }

    // Add lookup or destination columns values
    Map<String, String> matched = lookupMap.get(srcKey);
    if (matched != null) {
      for (String col : extraColumns) {
        outputRow.add(matched.getOrDefault(col, ""));
      }
    } else {
      for (int i = 0; i < extraColumns.size(); i++) {
        outputRow.add("");
      }
    }

    // 👉 IMPORTANT: Now print the record!
    try {
      printer.printRecord(outputRow);
    } catch (IOException e) {
      throw new RuntimeException("Error writing CSV record: " + e.getMessage(), e);
    }
  }

  private String buildKey(CSVRecord record, List<String> keyColumns) {
    return keyColumns.stream()
        .map(col -> record.isMapped(col) ? record.get(col).trim() : "")
        .reduce((a, b) -> a + "::" + b)
        .orElse("");
  }

  private void resetColumnStore(LookUpStore lookUpStore) {
    lookUpStore.setSourceSearchColumns(new ArrayList<>());
    lookUpStore.setDestinationColumns(new ArrayList<>());
    lookUpStore.setLookupTableName(null);
    lookUpStore.setLookupSearchColumns(new ArrayList<>());
    lookUpStore.setLookupValueColumns(new ArrayList<>());
    lookUpStore.setCacheEnabled(false);
    lookUpStore.setPreserveOptions(new ArrayList<>());
  }
}