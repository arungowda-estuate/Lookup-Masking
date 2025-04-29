package com.masking.model.hash_lookup;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
@AllArgsConstructor
@Data
public class HashLookupStore {
  private List<String> sourceSearchColumns = new ArrayList<>();
  private List<String> destinationColumns = new ArrayList<>();
  private String lookupTableName;
  private List<String> lookupSearchColumns = new ArrayList<>();
  private List<String> lookupValueColumns = new ArrayList<>();
  private String trimCharacters;
  private String algorithm;
  private Boolean cacheEnabled;
  private List<String> preserveOptions = new ArrayList<>();
  private String seed;

  public void clear() {
    sourceSearchColumns.clear();
    destinationColumns.clear();
    lookupTableName = null;
    lookupSearchColumns.clear();
    lookupValueColumns.clear();
    trimCharacters = null;
    algorithm = null;
    cacheEnabled = null;
    preserveOptions.clear();
    seed = null;
  }
}
