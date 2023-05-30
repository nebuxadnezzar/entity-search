package com.entity.server;

import java.util.*;

import java.io.*;

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

        OutputStream out = response.getOutputStream();
        String body = IOUtils.toString(request.getInputStream(), DEFAULT_CHARSET.name());
        Map<String, String[]> paramMap = request.getParameterMap();

        String paramStr = SimpleUtils.mapToParamString(SimpleUtils.convertToSingleValueMap(paramMap));
        String scriptPath = getScriptPath(request.getContextPath());

        setHeaders(response, config);
        run(out, scriptPath, body, paramStr);
    }

    private void run(OutputStream out, String scriptPath, String body, String params) {

        String cmd = (String) config.get("command");

        if (SimpleUtils.isEmpty(cmd)) {
            throw new RuntimeException("cgi command is missing in config");
        }
        try {
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
}
