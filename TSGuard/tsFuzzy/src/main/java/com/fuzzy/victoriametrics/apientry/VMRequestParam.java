package com.fuzzy.victoriametrics.apientry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VMRequestParam {
    // 请求参数类型
    private VMRequestType type;
    // 参数体
    private String body;
}
