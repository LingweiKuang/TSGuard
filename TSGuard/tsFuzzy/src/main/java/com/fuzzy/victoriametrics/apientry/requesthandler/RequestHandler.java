package com.fuzzy.victoriametrics.apientry.requesthandler;

import com.fuzzy.victoriametrics.apientry.VMApiEntry;

public interface RequestHandler {
    String execute(VMApiEntry apiEntry, String body);
}
