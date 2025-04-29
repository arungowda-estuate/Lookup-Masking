package com.masking.service.random_lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.random_lookup.RandomLookupStore;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class RandomLookupFunctionValidator {

  public ValidationResponse validateAndExtract(
      String lookupFunction, RandomLookupStore randomLookupStore) {
    ValidationResponse response = new ValidationResponse();
    List<String> messages = new ArrayList<>();

    lookupFunction = lookupFunction.trim();
    lookupFunction = lookupFunction.replaceAll("\\s+", "");

    // Validate the function format
    if (!lookupFunction.startsWith("RAND_LOOKUP(") || !lookupFunction.endsWith(")")) {
      messages.add("❌ Invalid function format. It must start with RAND_LOOKUP( and end with ).");
      response.setStatus("FAILED");
      response.setMessages(messages);
      return response;
    }

    // Extract inner content from the function
    String innerContent =
        lookupFunction.substring("RAND_LOOKUP(".length(), lookupFunction.length() - 1).trim();

    if (innerContent.isEmpty()) {
      messages.add("❌ Empty function body inside RAND_LOOKUP().");
      response.setStatus("FAILED");
      response.setMessages(messages);
      return response;
    }

    // Split inner content respecting parentheses
    List<String> parts = splitRespectingParentheses(innerContent);

    if (parts.isEmpty()) {
      messages.add("❌ No parts found inside RAND_LOOKUP.");
      response.setStatus("FAILED");
      response.setMessages(messages);
      return response;
    }

    // Extract table name (first part)
    String tableName = parts.get(0).trim();
    if (tableName.isEmpty()) {
      messages.add("❌ Missing lookup table name.");
    } else {
      randomLookupStore.setLookupTableName(tableName); // Set lookup table name
    }

    // Initialize values for destination columns, value columns, and options
    String destColumns = null;
    String valuesColumns = null;
    String limit = null;
    String ignore = null;
    String preserve = null;

    // Iterate over the remaining parts to extract options
    for (int i = 1; i < parts.size(); i++) {
      String part = parts.get(i).trim();

      if (part.startsWith("DEST=")) {
        destColumns = extractColumns(part.substring(5));
        if (destColumns == null) {
          messages.add("❌ Invalid DEST block format. Expected DEST=(col1,col2).");
        } else {
          randomLookupStore.setDestinationColumns(
              List.of(destColumns.split("\\s*,\\s*"))); // Set destination columns
        }
      } else if (part.startsWith("VALUES=")) {
        valuesColumns = extractColumns(part.substring(7));
        if (valuesColumns == null) {
          messages.add("❌ Invalid VALUES block format. Expected VALUES=(val1,val2).");
        } else {
          randomLookupStore.setLookupValueColumns(
              List.of(valuesColumns.split("\\s*,\\s*"))); // Set value columns
        }
      } else if (part.matches("^\\d+$")) {
        limit = part;
        try {
          randomLookupStore.setLimit(Integer.valueOf(limit)); // Set the limit (if valid)
        } catch (NumberFormatException e) {
          messages.add("❌ Invalid LIMIT value. It must be a number.");
        }
      } else if (part.startsWith("IGNORE=")) {
        ignore = extractColumns(part.substring(7));
        if (ignore == null || !validateIgnoreOrPreserve(ignore)) {
          messages.add(
              "❌ Invalid IGNORE block format. Expected IGNORE=(colname(spaces,null,zero_len)).");
        } else {
          randomLookupStore.setIgnoreOptions(
              List.of(ignore.split("\\s*,\\s*"))); // Set ignore options
        }
      } else if (part.startsWith("PRESERVE=")) {
        preserve = extractColumns(part.substring(9));
        if (preserve == null || !validateIgnoreOrPreserve(preserve)) {
          messages.add(
              "❌ Invalid PRESERVE block format. Expected PRESERVE=(colname(spaces,null,zero_len)).");
        } else {
          randomLookupStore.setPreserveOptions(
              List.of(preserve.split("\\s*,\\s*"))); // Set preserve options
        }
      }
      // 🛠️ New logic for simple column names
      else if (isSimpleColumnName(part)) {
        // Only if DEST and VALUES are not already set
        if (destColumns == null && valuesColumns == null) {
          destColumns = part;
          valuesColumns = part;
          randomLookupStore.setDestinationColumns(List.of(destColumns)); // Set destination columns
          randomLookupStore.setLookupValueColumns(List.of(valuesColumns)); // Set value columns
        } else {
          messages.add("❌ Multiple simple columns found without DEST= or VALUES=.");
        }
      } else {
        messages.add("❌ Unrecognized or invalid part: " + part);
      }
    }

    // Validate that DEST and VALUES columns match in length
    if (destColumns != null && valuesColumns != null) {
      String[] destArray = destColumns.split("\\s*,\\s*");
      String[] valuesArray = valuesColumns.split("\\s*,\\s*");

      if (destArray.length != valuesArray.length) {
        messages.add("❌ DEST and VALUES must have the same number of columns.");
      }
    }

    // Log the state of randomLookupStore for debugging
    System.out.println(randomLookupStore);

    // Set response status and messages
    if (messages.isEmpty()) {
      response.setStatus("SUCCESS");
    } else {
      response.setStatus("FAILED");
    }
    response.setMessages(messages);
    return response;
  }

  private String extractColumns(String part) {
    part = part.trim();
    if (part.startsWith("(") && part.endsWith(")")) {
      return part.substring(1, part.length() - 1).trim();
    }
    return null;
  }

  private boolean validateIgnoreOrPreserve(String block) {
    if (!block.contains("(") || !block.endsWith(")")) {
      return false;
    }
    int openIdx = block.indexOf('(');
    String colName = block.substring(0, openIdx).trim();
    String conditions = block.substring(openIdx + 1, block.length() - 1).trim();

    if (colName.isEmpty() || conditions.isEmpty()) {
      return false;
    }

    String[] allowedConditions = {"spaces", "null", "zero_len"};
    String[] condArray = conditions.split("\\s*,\\s*");

    for (String cond : condArray) {
      boolean match = false;
      for (String allowed : allowedConditions) {
        if (allowed.equalsIgnoreCase(cond)) {
          match = true;
          break;
        }
      }
      if (!match) {
        return false;
      }
    }
    return true;
  }

  private List<String> splitRespectingParentheses(String input) {
    List<String> parts = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int parentheses = 0;

    for (char c : input.toCharArray()) {
      if (c == ',' && parentheses == 0) {
        parts.add(current.toString().trim());
        current.setLength(0);
      } else {
        if (c == '(') parentheses++;
        else if (c == ')') parentheses--;
        current.append(c);
      }
    }
    if (!current.isEmpty()) {
      parts.add(current.toString().trim());
    }
    return parts;
  }

  // 🛠️ Helper to check simple column name (alphanumeric + _ allowed)
  private boolean isSimpleColumnName(String part) {
    return part.matches("^[a-zA-Z0-9_]+$");
  }
}
