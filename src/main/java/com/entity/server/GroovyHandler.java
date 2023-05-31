package com.entity.server;

import java.util.*;

import java.io.*;
import org.json.*;
import java.nio.file.*;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.http.MimeTypes;
import groovy.lang.*;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.entity.util.*;

public class GroovyHandler extends BaseHandler {

    public GroovyHandler(Map<String, Object> config) {
        super(config);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        System.out.printf("\n!!! GROOVY SERVLET config: %s\n", config);
        baseRequest.setHandled(true);
        response.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
        response.setStatus(HttpServletResponse.SC_OK);
        String scriptPath = getScriptPath(request.getContextPath());
        String body = IOUtils.toString(request.getInputStream(), DEFAULT_CHARSET.name());
        JSONObject params = new JSONObject(request.getParameterMap());

        Binding binding = new Binding();
        binding.setProperty("body", body);
        binding.setProperty("params", params.toMap());
        binding.setProperty("request", request);
        binding.setProperty("response", response);
        binding.setProperty("out", response.getOutputStream());

        setHeaders(response, config);
        Script script = createScript(scriptPath);
        script.setBinding(binding);
        script.run();
    }

    private Script createScript(String scriptPath) throws IOException {
        String text = new String(Files.readAllBytes(Paths.get(scriptPath)), DEFAULT_CHARSET);
        GroovyShell gs = new GroovyShell(new Binding());
        return gs.parse(text);
    }

}
