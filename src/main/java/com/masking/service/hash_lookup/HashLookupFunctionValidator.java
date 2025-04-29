package com.masking.service.hash_lookup;

import com.masking.component.ValidationResponse;
import com.masking.model.hash_lookup.HashLookupStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HashLookupFunctionValidator {

  @Autowired private HashLookupStore hashLookupStore;

  @Autowired private ValidationResponse response;

  public ValidationResponse validateAndExtract(String input) {
    List<String> messages = new ArrayList<>();
    hashLookupStore.clear(); // Clear previous state

    if (input == null || input.trim().isEmpty()) {
      messages.add("Input cannot be empty.");
      return buildErrorResponse(messages);
    }

    input = input.trim();

    // Basic structure check
    if (!input.startsWith("HASH_LOOKUP(") || !input.endsWith(")")) {
      messages.add("Function must start with HASH_LOOKUP( and end with ).");
      return buildErrorResponse(messages);
    }

    String inside = input.substring("HASH_LOOKUP(".length(), input.length() - 1).trim();
    List<String> tokens = splitTokens(inside); // Split by commas respecting nested parentheses

    boolean firstTokenHandled = false;

    for (String token : tokens) {
      token = token.trim();
      if (token.isEmpty()) continue;

      // Handle first token: either SRCSEARCH=... or direct column name
      if (!firstTokenHandled) {
        if (token.startsWith("SRCSEARCH=")) {
          String cols = token.substring(token.indexOf('=') + 1).replaceAll("[()]", "").trim();
          hashLookupStore.setSourceSearchColumns(
              new ArrayList<>(Arrays.asList(cols.split("\\s*,\\s*"))));
        } else if (!token.contains("=") && !token.contains("(")) {
          hashLookupStore.setSourceSearchColumns(new ArrayList<>(List.of(token)));
        } else {
          messages.add("First token must be a column name or SRCSEARCH=(...). Found: " + token);
        }
        firstTokenHandled = true;
        continue;
      }

      // Keyword-specific parsing (must be done BEFORE generic (..)-block parsing)
      if (token.startsWith("SRCSEARCH=")) {
        String cols = token.substring(token.indexOf('=') + 1).replaceAll("[()]", "").trim();
        hashLookupStore.setSourceSearchColumns(
            new ArrayList<>(Arrays.asList(cols.split("\\s*,\\s*"))));
      } else if (token.startsWith("TRIM=")) {
        String trimChars = token.substring(token.indexOf('=') + 1).replaceAll("[()]", "").trim();
        hashLookupStore.setTrimCharacters(trimChars);
      } else if (token.startsWith("DEST=")) {
        String cols = token.substring(token.indexOf('=') + 1).replaceAll("[()]", "").trim();
        hashLookupStore.setDestinationColumns(
            new ArrayList<>(Arrays.asList(cols.split("\\s*,\\s*"))));
      } else if (token.startsWith("PRESERVE=")) {
        String preserve = token.substring(token.indexOf('=') + 1).replaceAll("[()]", "").trim();
        hashLookupStore.setPreserveOptions(
            new ArrayList<>(Arrays.asList(preserve.split("\\s*,\\s*"))));
      } else if (token.startsWith("ALGO=")) {
        String algo = token.substring(token.indexOf('=') + 1).trim();
        hashLookupStore.setAlgorithm(algo);
      } else if (token.equalsIgnoreCase("CACHE")) {
        hashLookupStore.setCacheEnabled(true);
      } else if (token.equalsIgnoreCase("NOCACHE")) {
        hashLookupStore.setCacheEnabled(false);
      } else if (token.startsWith("SEED=")) {
        String seed = token.substring(token.indexOf('=') + 1).trim();
        hashLookupStore.setSeed(seed);
      }

      // Generic lookup table function: tablename(col1, col2)
      else if (token.contains("(") && token.endsWith(")")) {
        String tableName = token.substring(0, token.indexOf('(')).trim();
        String inner = token.substring(token.indexOf('(') + 1, token.lastIndexOf(')'));
        String[] lookupParts = inner.split("\\s*,\\s*");

        if (lookupParts.length >= 1) {
          hashLookupStore.setLookupTableName(tableName);
          hashLookupStore.setLookupSearchColumns(new ArrayList<>(List.of(lookupParts[0])));
          if (lookupParts.length > 1) {
            hashLookupStore.setLookupValueColumns(new ArrayList<>(List.of(lookupParts[1])));
          }
        } else {
          messages.add("Invalid lookup table format: " + token);
        }
      }

      // Unknown format
      else {
        messages.add("Unknown token: " + token);
      }
    }

    if (hashLookupStore.getSourceSearchColumns().isEmpty()) {
      messages.add("SRCSEARCH columns are required.");
    }

    System.out.println(hashLookupStore);

    if (!messages.isEmpty()) {
      return buildErrorResponse(messages);
    }

    response.setStatus("SUCCESS");
    response.setMessages(new ArrayList<>(List.of("Validation passed successfully.")));
    return response;
  }

  private ValidationResponse buildErrorResponse(List<String> messages) {
    response.setStatus("ERROR");
    response.setMessages(messages);
    return response;
  }

  private List<String> splitTokens(String input) {
    List<String> tokens = new ArrayList<>();
    int depth = 0;
    StringBuilder current = new StringBuilder();

    for (char ch : input.toCharArray()) {
      if (ch == ',' && depth == 0) {
        tokens.add(current.toString());
        current.setLength(0);
      } else {
        if (ch == '(') depth++;
        if (ch == ')') depth--;
        current.append(ch);
      }
    }

    if (current.length() > 0) {
      tokens.add(current.toString());
    }

    return tokens;
  }
}
