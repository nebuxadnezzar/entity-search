package com.entity.server;

import java.util.*;

import java.io.*;
import java.nio.*;
import java.nio.file.Paths;

import org.json.*;
import org.eclipse.jetty.server.*;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.entity.util.*;

public class CgiHandler extends BaseHandler {

    private static String BODY_TAG = "~body~";
    private static String SCRIPT_TAG = "~script~";
    private static String PARAMS_TAG = "~params~";

    public CgiHandler(Map<String, Object> config) {
        super(config);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        System.out.printf("\n!!! CGI SERVLET config: %s\n", config);
        baseRequest.setHandled(true);

        System.out.printf("CONTEXT PATH: %s\nPATH TRANSLATED: %s\nPATH INFO: %s\n", request.getContextPath(),
                request.getPathTranslated(), request.getPathInfo());
        OutputStream out = response.getOutputStream();
        String body = IOUtils.toString(request.getInputStream(), DEFAULT_CHARSET.name());
        Map<String, String[]> paramMap = request.getParameterMap();
        JSONObject params = new JSONObject(request.getParameterMap());
        String paramStr = SimpleUtils.mapToParamString(SimpleUtils.convertToSingleValueMap(paramMap));
        String scriptPath = getScriptPath(request.getContextPath());
        JSONObject responseBody = new JSONObject().append("body", body)
                .append("params", params).append("paramString", paramStr)
                .append("scriptPath", scriptPath);

        setHeaders(response, config);
        run(out, scriptPath, body, paramStr);
        bytesout(out, responseBody.toString(2).getBytes(DEFAULT_CHARSET));
    }

    private void run(OutputStream out, String scriptPath, String body, String params) {
        String scriptFolder = Paths.get((String) config.get("content")).toAbsolutePath().toString();
        String cmd = (String) config.get("command");
        // String[] cmd = createCmdArray(((String) config.get("command")),
        // scriptPath,body, params);

        if (SimpleUtils.isEmpty(cmd)) {
            throw new RuntimeException("cgi command is missing in config");
        }
        try {/*
              * ProcessBuilder builder = new ProcessBuilder(cmd);
              * builder.redirectErrorStream(true);
              * builder.redirectInput(ProcessBuilder.Redirect.PIPE);
              * builder.redirectOutput(ProcessBuilder.Redirect.PIPE);
              * builder.directory(new File(scriptFolder));
              * // builder.inheritIO();
              * Process proc = builder.start();
              */
            Process proc = createAndRunProcess(cmd, scriptPath, body, params);
            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("--> " + line);
                bytesout(out, line.getBytes(DEFAULT_CHARSET));
            }

            reader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            while ((line = reader.readLine()) != null) {
                System.out.println("+-> " + line);
                bytesout(out, line.getBytes(DEFAULT_CHARSET));
            }

            int exitVal = proc.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
            } else {
                System.out.printf("Exit error code: %d\n", exitVal);
            }
        } catch (Exception e) {
            e.printStackTrace(new PrintStream(out));
        }
    }

    private Process createAndRunProcess(String cmdString, String scriptPath, String body,
            String params) throws IOException {
        String[] ss = cmdString.split("\\|");
        List<ProcessBuilder> builders = new ArrayList<>();
        for (String s : ss) {

            String[] cmd = createCmdArray(s, scriptPath, body, params);
            System.out.printf("\nSUBCOMMAND: %s AND [%s]\n", s, String.join("], [", cmd));
            ProcessBuilder builder = new ProcessBuilder(cmd);
            builder.redirectErrorStream(true).redirectInput(ProcessBuilder.Redirect.PIPE)
                    .redirectOutput(ProcessBuilder.Redirect.PIPE);
            // builder.directory(new File(scriptFolder));
            builders.add(builder);
        }

        List<Process> processes = ProcessBuilder.startPipeline(builders);
        return processes.get(processes.size() - 1);
    }

    private String[] createCmdArray(String cmdString, String scriptPath, String body, String params) {
        String[] ss = cmdString.trim().replaceAll("\\s+", " ").split("\\s+");
        String[] cmd = new String[ss.length];
        for (int i = 0, k = ss.length; i < k; i++) {
            cmd[i] = ss[i].replace(SCRIPT_TAG, scriptPath).replace(BODY_TAG, body).replace(PARAMS_TAG, params);
        }
        return cmd;
    }

    // the last part of context path is script name so context path should consist
    // at least of 2 parts
    // i.e. /abc/bdc or /abc/e/ddd etc.
    //
    private String getScriptPath(String contextPath) {
        String scriptFolder = (String) config.get("content");
        if (SimpleUtils.isEmpty(scriptFolder)) {
            throw new RuntimeException("missing content folder");
        }
        String[] ss = contextPath.split("\\/", -1);
        if (ss.length < 2) {
            throw new RuntimeException("missing cgi script name in context path");
        }
        String scriptName = ss[ss.length - 1];
        return Paths.get(scriptFolder, scriptName).toAbsolutePath().toString();
    }

}
