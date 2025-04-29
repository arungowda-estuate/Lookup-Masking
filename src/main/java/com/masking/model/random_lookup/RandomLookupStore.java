package com.masking.model.random_lookup;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Component
public class RandomLookupStore {
    private String lookupTableName; // Table to pick random data from
    private List<String> destinationColumns = new ArrayList<>(); // Where random values will go
    private List<String> lookupValueColumns = new ArrayList<>(); // What columns to pick randomly
    private Integer limit; // Limit number of rows (optional)
    private List<String> preserveOptions = new ArrayList<>(); // Optional PRESERVE options
    private List<String> ignoreOptions = new ArrayList<>();
}
