package com.entity.server;

import java.io.*;
import java.util.*;
import java.net.*;

import org.json.*;

import com.entity.util.*;
import com.entity.indexing.*;
import org.eclipse.jetty.server.*;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

class IndexDataHandler extends BaseHandler {

    public static final int DEFAULT_RESULTS_LIMIT = 100000;
    public static final String FF = "_filterFields_";
    public static final String LIMIT = "_limit_";
    public static final String SORT = "_sort_";
    public static final String OUTPUT = "_output_";
    public static final String DOC_ID_PATTERN = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";
    private boolean DB_DEBUG = false;

    public IndexDataHandler(Map<String, Object> config) {
        super(config);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {

        String s = System.getProperty("db.debug");
        DB_DEBUG = StringUtils.validateString("(?i)(true|t|1)", s);

        Searcher searcher = (Searcher) config.get("searcher");
        if (searcher == null) {
            throw new RuntimeException("Index searcher is missing.");
        }
        baseRequest.setHandled(true);
        OutputStream out = response.getOutputStream();
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(HDR_JSON);

        if (!request.getMethod().equalsIgnoreCase("POST")) {
            response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            String msg = String.format("method %s not allowed. Use POST.", request.getMethod());
            System.out.printf("\n\n!!! WRONG HTTP METHOD %s !!!\n\n", request.getMethod());
            bytesout(out, String.format(ERR_TEMPLATE, msg).getBytes(DEFAULT_CHARSET));
            return;
        }
        String body = IOUtils.toString(request.getInputStream());

        if (body.length() < 2) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            System.out.printf("\n\n!!! MISSING REQUEST BODY !!!\n\n");
            bytesout(out, String.format(ERR_TEMPLATE, "missing request body").getBytes(DEFAULT_CHARSET));
            return;
        }

        if (DB_DEBUG) {
            System.out.printf("RAW QUERY:\n%s\n", body);
        }
        // query object
        //
        JSONObject qo = new JSONObject(body);
        Set<String> ff = new HashSet<>();

        if (qo.optJSONArray(FF) != null) {
            List<Object> ffList = qo.getJSONArray(FF).toList();
            for (Object o : ffList) {
                if (o != null)
                    ff.add(o.toString());
            }
            if (ff.size() < 1)
                ff.add("content");
        }

        setHeaders(response, config);
        DocumentCollector dc = new HttpStreamDocumentCollector(0, ff, null);

        dc.setOutStream(out);

        try {
            runQuery(searcher, createQuery(qo), dc, out);
        } catch (Exception e) {
            bytesout(out, String.format(ERR_TEMPLATE, e).getBytes(DEFAULT_CHARSET));
        }
    }

    // -------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private void setHeaders(HttpServletResponse response, Map<String, Object> config) {
        Map<String, Object> headers = (Map<String, Object>) config.get("headers");

        if (headers == null || headers.size() < 1)
            return;
        for (Map.Entry<String, Object> e : headers.entrySet()) {
            response.addHeader(e.getKey(), e.getValue().toString());
        }
    }

    // -------------------------------------------------------------------------
    private String createQuery(JSONObject qo) throws Exception {
        Map<String, String> parts = new LinkedHashMap<String, String>();
        int limit = 0;
        String queryType = qo.optString("_type_", "AND").equalsIgnoreCase("AND") ? "+" : "*";

        parts.put("sort", String.format("sort=%s\n", qo.optBoolean(SORT)));

        if (qo.optInt("_limit_") > 0) {
            limit = qo.optInt(LIMIT);
            parts.put("limit",
                    String.format("limit=%d\n", limit > DEFAULT_RESULTS_LIMIT ? DEFAULT_RESULTS_LIMIT : limit));
        } else {
            parts.put("limit", "all\n");
        }

        StringBuilder sb = new StringBuilder(String.format("(%s ", queryType));

        for (String key : qo.keySet()) {
            if (key.matches("_\\p{Alpha}+_"))
                continue;

            JSONArray vals = qo.getJSONArray(key);

            for (int i = 0, k = vals.length(); i < k; i++) {
                String val = vals.getString(i);
                if (StringUtils.validateString("[\\w~*\"\\s\\[\\]\\{\\}]+", val) ||
                        StringUtils.validateString(DOC_ID_PATTERN, val)) {
                    sb.append(String.format("(* %s:%s ) ", key, URLDecoder.decode(val, "UTF-8")));
                }
            }

        }
        sb.append(")\n");
        for (Map.Entry<String, String> e : parts.entrySet()) {
            sb.append(e.getValue());
        }
        sb.append(".\n");
        if (DB_DEBUG) {
            System.out.printf("\nQUERY:\n%s\n\n", sb.toString());
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    private void runQuery(Searcher searcher, String query, DocumentCollector dc, OutputStream os)
            throws Exception {
        try {
            searcher.runCompoundQuery(query, dc);
        } catch (Exception e) {
            e.printStackTrace();
            DataStreamUtils.sendDataChunk(os, String.format(ERR_TEMPLATE, e).getBytes(), false);
            DataStreamUtils.sendDataChunk(os, null, false);
        }
    }
}

/*
 *
 * {
 * "filterFields": ["fieldName1", ... "fieldNameN"]
 * "field1":["value1", "value2", ... "valueN"]
 *
 * }
 */

/**
 * {
 * "_filterFields_":[],
 * "_limit_": 100,
 * "_sort_": true,
 * "_collector_": "content",
 * "name":["mustafa~2", "abdul*"]
 * }
 *
 * {
 * "_filterFields_":[name],
 * "_limit_": 100,
 * "_sort_": true,
 * "_collector_": "plain",
 * "name":["mustafa~2", "abdul*"]
 * }
 */