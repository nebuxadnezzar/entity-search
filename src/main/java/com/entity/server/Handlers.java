package com.entity.server;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.json.JSONObject;

import com.entity.util.IOUtils;
import org.eclipse.jetty.server.*;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

class ServerInfoHandler extends BaseHandler {

    public ServerInfoHandler(Map<String, Object> config) {

        super(config);
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        baseRequest.setHandled(true);
        response.setContentType(HDR_TEXT);
        response.setStatus(HttpServletResponse.SC_OK);
        OutputStream out = response.getOutputStream();
        byte[] bytes = new JSONObject(config).getJSONArray("endpoints").toString(0).getBytes(DEFAULT_CHARSET);
        bytesout(out, bytes);
    }
}

class BaseHandler extends AbstractHandler {

    protected static String ERR_TEMPLATE = "{\"data\":[{\"exception\":\"%s\"}],\"count\":0,\"status\":\"FAIL\"}\n";
    public static String HDR_TEXT = "text/plain;charset=UTF-8";
    public static String HDR_JSON = "application/json;charset=UTF-8";
    protected Map<String, Object> config;
    protected static Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public BaseHandler(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        baseRequest.setHandled(true);
        response.setContentType(HDR_TEXT);
        response.setStatus(HttpServletResponse.SC_OK);
        OutputStream out = response.getOutputStream();
        System.out.println("\n!!! ECHOING REQUEST !!!\n");
        bytesout(out, "\n\nBODY:\n".getBytes(DEFAULT_CHARSET));
        IOUtils.copy(request.getInputStream(), out);
        bytesout(out, "\n\nURL PARAMS:\n".getBytes(DEFAULT_CHARSET));
        byte[] bytes = new JSONObject(request.getParameterMap()).toString(2).getBytes(DEFAULT_CHARSET);
        System.out.println(new String(bytes));
        bytesout(out, bytes);
    }

    protected void bytesout(OutputStream out, byte[] b) throws IOException {
        out.write(b, 0, b.length);
    }
}