package com.masking.service.lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.lookup.LookUpStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class LookUpFunctionValidator {

  public ValidationResponse validateAndExtract(String lookupFunction, LookUpStore lookUpStore) {
    List<String> errors = new ArrayList<>();

    if (lookupFunction == null || !lookupFunction.trim().toUpperCase().startsWith("LOOKUP(")) {
      errors.add("Function must start with 'LOOKUP('.");
      return new ValidationResponse("FAILED", errors);
    }

    try {
      // Extract inside of LOOKUP(...)
      String inner = lookupFunction.trim();
      inner = inner.substring(inner.indexOf("(") + 1, inner.lastIndexOf(")")).trim();

      // === Step 1: Parse Top Level Comma-separated Parts ===
      List<String> topParts = splitTopLevel(inner);

      if (topParts.isEmpty()) {
        errors.add("Function body is empty inside LOOKUP().");
        return new ValidationResponse("FAILED", errors);
      }

      int partIndex = 0;

      // === Step 2: Parse SourceSearch ===
      String firstPart = topParts.get(partIndex++);
      if (firstPart.toUpperCase().startsWith("SRCSEARCH=")) {
        String srcColumns =
            firstPart.substring(firstPart.indexOf("(") + 1, firstPart.lastIndexOf(")"));
        lookUpStore.setSourceSearchColumns(Arrays.asList(srcColumns.split("\\s*,\\s*")));
      } else {
        lookUpStore.setSourceSearchColumns(Arrays.asList(firstPart.trim()));
      }

      // === Step 3: Parse DEST if present ===
      if (partIndex < topParts.size()
          && topParts.get(partIndex).toUpperCase().startsWith("DEST=")) {
        String destPart = topParts.get(partIndex++);
        String destColumns =
            destPart.substring(destPart.indexOf("(") + 1, destPart.lastIndexOf(")"));
        lookUpStore.setDestinationColumns(Arrays.asList(destColumns.split("\\s*,\\s*")));
      }

      // === Step 4: Parse Lookup Table ===
      if (partIndex < topParts.size()) {
        String tablePart = topParts.get(partIndex++);

        int tableNameEnd = tablePart.indexOf("(");
        if (tableNameEnd == -1) {
          errors.add("Lookup table block malformed: missing opening '(' after table name.");
          return new ValidationResponse("FAILED", errors);
        }
        String tableName = tablePart.substring(0, tableNameEnd).trim();
        lookUpStore.setLookupTableName(tableName);

        String tableContent =
            tablePart.substring(tableNameEnd + 1, tablePart.lastIndexOf(")")).trim();

        // Now inside the table block: split carefully
        List<String> tableParts = splitTopLevel(tableContent);

        if (tableParts.isEmpty() || tableParts.size() < 2) {
          errors.add("Lookup table block must have search column and value column.");
          return new ValidationResponse("FAILED", errors);
        }

        // Search column
        String lkpSearch = tableParts.get(0).trim();
        if (lkpSearch.toUpperCase().startsWith("LKPSEARCH=")) {
          String lkpCols =
              lkpSearch.substring(lkpSearch.indexOf("(") + 1, lkpSearch.lastIndexOf(")"));
          lookUpStore.setLookupSearchColumns(Arrays.asList(lkpCols.split("\\s*,\\s*")));
        } else {
          lookUpStore.setLookupSearchColumns(Arrays.asList(lkpSearch));
        }

        // Value columns
        String valPart = tableParts.get(1).trim();
        if (valPart.toUpperCase().startsWith("VALUES=")) {
          String valCols = valPart.substring(valPart.indexOf("(") + 1, valPart.lastIndexOf(")"));
          lookUpStore.setLookupValueColumns(Arrays.asList(valCols.split("\\s*,\\s*")));
        } else {
          lookUpStore.setLookupValueColumns(Arrays.asList(valPart));
        }

        // === Step 5: Handle CACHE/NOCACHE inside lookup table ===
        for (String extra : tableParts.subList(2, tableParts.size())) {
          if (extra.trim().equalsIgnoreCase("CACHE")) {
            lookUpStore.setCacheEnabled(true);
          } else if (extra.trim().equalsIgnoreCase("NOCACHE")) {
            lookUpStore.setCacheEnabled(false);
          }
        }
      }

      // === Step 6: Handle PRESERVE block ===
      while (partIndex < topParts.size()) {
        String part = topParts.get(partIndex++);
        if (part.toUpperCase().startsWith("PRESERVE=")) {
          String preserveCols = part.substring(part.indexOf("(") + 1, part.lastIndexOf(")"));
          lookUpStore.setPreserveOptions(Arrays.asList(preserveCols.split("\\s*,\\s*")));
        }
      }

      if (!errors.isEmpty()) {
        return new ValidationResponse("FAILED", errors);
      }

      return new ValidationResponse("SUCCESS", null);

    } catch (Exception e) {
      errors.add("Error while validating: " + e.getMessage());
      return new ValidationResponse("FAILED", errors);
    }
  }

  private List<String> splitTopLevel(String input) {
    List<String> parts = new ArrayList<>();
    int level = 0;
    StringBuilder current = new StringBuilder();

    for (char c : input.toCharArray()) {
      if (c == '(') {
        level++;
        current.append(c);
      } else if (c == ')') {
        level--;
        current.append(c);
      } else if (c == ',' && level == 0) {
        parts.add(current.toString().trim());
        current.setLength(0);
      } else {
        current.append(c);
      }
    }
    if (current.length() > 0) {
      parts.add(current.toString().trim());
    }
    return parts;
  }
}
