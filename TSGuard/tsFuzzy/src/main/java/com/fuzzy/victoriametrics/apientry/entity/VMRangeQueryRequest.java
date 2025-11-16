package com.fuzzy.victoriametrics.apientry.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VMRangeQueryRequest {
    private String query;
    private String step;
    private long start;
    private long end;
}
