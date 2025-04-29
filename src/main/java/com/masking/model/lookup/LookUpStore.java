package com.masking.model.lookup;

import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
public class LookUpStore {
    private List<String> sourceSearchColumns = new ArrayList<>();
    private List<String> destinationColumns= new ArrayList<>();
    private String lookupTableName;
    private List<String> lookupSearchColumns= new ArrayList<>();
    private List<String> lookupValueColumns= new ArrayList<>();
    private boolean cacheEnabled;
    private List<String> preserveOptions= new ArrayList<>();
}
