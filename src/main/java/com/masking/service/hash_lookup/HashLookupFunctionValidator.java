package com.masking.service.hash_lookup;

import static com.masking.service.hash_lookup.HashLookupCsvGenerator.resetColumnStore;

import com.masking.component.ValidationResponse;
import com.masking.model.hash_lookup.HashLookupStore;
import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HashLookupFunctionValidator {

  @Autowired private HashLookupStore hashLookupStore;
  @Autowired private ValidationResponse response;

  public ValidationResponse validateAndExtract(String input) {
    List<String> messages = new ArrayList<>();
    resetColumnStore(hashLookupStore);

    if (input == null || input.trim().isEmpty()) {
      return error("Input cannot be empty.");
    }

    input = input.trim();

    if (!input.startsWith("HASH_LOOKUP(") || !input.endsWith(")")) {
      return error("Function must start with HASH_LOOKUP( and end with ).");
    }

    String inner = input.substring("HASH_LOOKUP(".length(), input.length() - 1).trim();
    List<String> tokens = splitTokens(inner);

    if (tokens.isEmpty()) {
      return error("HASH_LOOKUP must contain at least one argument.");
    }

    boolean firstHandled = false;

    for (int i = 0; i < tokens.size(); i++) {
      String token = tokens.get(i).trim();

      if (!firstHandled) {
        if (token.startsWith("SRCSEARCH=")) {
          List<String> cols = parseListValue(token, "SRCSEARCH", messages);
          if (!cols.isEmpty()) {
            hashLookupStore.setSourceSearchColumns(cols);
          }
        } else if (!token.contains("=") && !token.contains("(")) {
          hashLookupStore.setSourceSearchColumns(List.of(token));
        } else {
          messages.add("First token must be a column name or SRCSEARCH=(...). Found: " + token);
        }
        firstHandled = true;
        continue;
      }

      String upperToken = token.toUpperCase();

      if (upperToken.startsWith("DEST=")) {
        hashLookupStore.setDestinationColumns(parseListValue(token, "DEST", messages));
      } else if (token.matches("[a-zA-Z_][a-zA-Z0-9_]*\\(.*\\)")) {
        Map<String, List<String>> parts = parseTableBlockArgs(token, messages);
        if (!parts.isEmpty()) {
          hashLookupStore.setLookupTableName(parts.get("tableName").get(0));
          hashLookupStore.setLookupSearchColumns(
              parts.getOrDefault("lookupSearchColumns", List.of()));
          hashLookupStore.setLookupValueColumns(
              parts.getOrDefault("lookupValueColumns", List.of()));
        }
      } else {
        messages.add("Unknown or invalid token: " + token);
      }
    }

    if (hashLookupStore.getSourceSearchColumns().isEmpty()) {
      messages.add("SRCSEARCH or source column must be provided.");
    }
    if (hashLookupStore.getDestinationColumns().isEmpty()) {
      messages.add("DEST must be specified and contain at least one column.");
    }
    if (hashLookupStore.getLookupTableName() == null) {
      messages.add("Lookup table block must be present and valid.");
    }

    if (!messages.isEmpty()) {
      return buildErrorResponse(messages);
    }

    response.setStatus("SUCCESS");
    response.setMessages(List.of("✅ Validation passed successfully."));
    return response;
  }

  private List<String> splitTokens(String input) {
    List<String> tokens = new ArrayList<>();
    int depth = 0;
    StringBuilder sb = new StringBuilder();
    for (char c : input.toCharArray()) {
      if (c == ',' && depth == 0) {
        tokens.add(sb.toString());
        sb.setLength(0);
      } else {
        if (c == '(') depth++;
        if (c == ')') depth--;
        sb.append(c);
      }
    }
    if (sb.length() > 0) {
      tokens.add(sb.toString());
    }
    return tokens;
  }

  private List<String> parseListValue(String token, String key, List<String> messages) {
    int idx = token.indexOf('=');
    if (idx == -1) {
      messages.add("Missing '=' in " + key + " token.");
      return List.of();
    }
    String value = token.substring(idx + 1).trim();

    // Handle format like DEST=(a, b) or DEST=a,b
    if (value.startsWith("(") && value.endsWith(")")) {
      value = value.substring(1, value.length() - 1).trim();
    }

    if (value.isEmpty()) {
      messages.add(key + " cannot be empty.");
      return List.of();
    }

    return Arrays.asList(value.split("\\s*,\\s*"));
  }

  private Map<String, List<String>> parseTableBlockArgs(String input, List<String> messages) {
    Map<String, List<String>> result = new HashMap<>();
    int firstParen = input.indexOf('(');
    int lastParen = input.lastIndexOf(')');

    if (firstParen == -1 || lastParen == -1 || lastParen <= firstParen) {
      messages.add(
          "❌ Invalid table block format. Must be in form tableName(search_col, values=(...))");
      return result;
    }

    String tableName = input.substring(0, firstParen).trim();
    String inner = input.substring(firstParen + 1, lastParen).trim();
    List<String> parts = splitTokens(inner);

    if (parts.isEmpty()) {
      messages.add("❌ Lookup table must contain at least one search column.");
      return result;
    }

    String searchCol = parts.get(0).trim();
    if (!searchCol.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
      messages.add("❌ Invalid search column: " + searchCol);
      return result;
    }

    List<String> searchCols = List.of(searchCol);
    List<String> valueCols = new ArrayList<>();

    if (parts.size() > 1) {
      String second = parts.get(1).trim();

      if (second.startsWith("values=")) {
        int eqIdx = second.indexOf('=');
        String valuesPart = second.substring(eqIdx + 1).trim();
        if (valuesPart.startsWith("(") && valuesPart.endsWith(")")) {
          valuesPart = valuesPart.substring(1, valuesPart.length() - 1);
        } else {
          messages.add("❌ Invalid values format. Expected: values=(val1, val2)");
          return result;
        }
        for (String col : valuesPart.split("\\s*,\\s*")) {
          col = col.trim();
          if (!col.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            messages.add("❌ Invalid value column: " + col);
            return result;
          }
          valueCols.add(col);
        }
      } else {
        messages.add("❌ Second argument in table block must be values=(...). Found: " + second);
        return result;
      }
    }

    result.put("tableName", List.of(tableName));
    result.put("lookupSearchColumns", searchCols);
    result.put("lookupValueColumns", valueCols);
    return result;
  }

  private ValidationResponse error(String msg) {
    response.setStatus("ERROR");
    response.setMessages(List.of("❌ " + msg));
    return response;
  }

  private ValidationResponse buildErrorResponse(List<String> messages) {
    response.setStatus("ERROR");
    response.setMessages(messages);
    return response;
  }
}
