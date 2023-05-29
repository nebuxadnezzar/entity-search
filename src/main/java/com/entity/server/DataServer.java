package com.entity.server;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.util.resource.*;
import org.json.*;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.*;
import java.util.*;

import com.entity.indexing.*;

public class DataServer {
    final private static int DEFAULT_PORT = 8080;
    final private static String DEFAULT_HOST = "127.0.0.1";
    final private static String SERVER_CONF_SECTION = "_interface_";
    final private static String IS_INDEX_HANDLER_SECTION = "indexHandler";
    final private static String IS_GROOVY_HANDLER_SECTION = "groovy";
    final private static String IS_CGI_HANDLER_SECTION = "cgi";
    final private static String OPEN_SEARCHERS = "openSearchers";

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("config file is missing");
            System.exit(1);
        }

        if (System.getProperty("jetty.home") == null)
            System.setProperty("jetty.home", "./jetty.home");
        // SimpleUtils.printEnv();
        String json = new String(Files.readAllBytes(Paths.get(args[0])), Charset.forName("UTF8"));
        final Map<String, Object> config = createConfig(json);
        System.out.println(config);

        Server server = createServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                Map<String, Object> svrConfig = getSvrConfig(config);
                System.out.printf("\n\n!!! OPEN SEARCHERS: %s\n\n",
                        (ArrayList<Searcher>) svrConfig.get(OPEN_SEARCHERS));
                List<Searcher> searchers = (ArrayList<Searcher>) svrConfig.get(OPEN_SEARCHERS);
                for (Searcher sr : searchers) {
                    try {
                        System.out.printf("\n\nCLOSING INDEX: %s\n", sr.toString());
                        sr.close();
                    } catch (IOException e) {
                        System.out.printf("\n\nSearcher closing exception: %s\n", e);
                    }
                }
                System.out.println(DataServer.class.getName() + " exited!");
            }
        });
        server.start();
        server.join();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> createConfig(String json) {
        Map<String, Object> config = new JSONObject(json).toMap();
        Map<String, Object> svrConfig = (Map<String, Object>) config.get(SERVER_CONF_SECTION);
        if (svrConfig == null) {
            svrConfig = new HashMap<String, Object>();
            svrConfig.put("port", DEFAULT_PORT);
            svrConfig.put("host", DEFAULT_HOST);
            config.put(SERVER_CONF_SECTION, svrConfig);
        } else {
            if (svrConfig.get("port") == null) {
                svrConfig.put("port", DEFAULT_PORT);
            }
            if (svrConfig.get("host") == null) {
                svrConfig.put("host", DEFAULT_HOST);
            }
        }
        return config;
    }

    private static Server createServer(Map<String, Object> config) {
        Map<String, Object> svrConfig = getSvrConfig(config);
        int port = (Integer) svrConfig.get("port");
        String host = (String) svrConfig.get("host");

        Server server = new Server();
        server.setStopAtShutdown(true);
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSendServerVersion(false);
        HttpConnectionFactory httpFactory = new HttpConnectionFactory(httpConfig);
        ServerConnector connector = new ServerConnector(server, httpFactory);
        connector.setHost(host);
        connector.setPort(port);
        server.setConnectors(new Connector[] { connector });

        server.setAttribute("config", config);
        server.setHandler(processEndPoints(config));
        return server;
    }

    @SuppressWarnings("unchecked")
    private static HandlerList processEndPoints(Map<String, Object> config) {
        Map<String, Object> svrConfig = getSvrConfig(config);
        List<String> endpoints = new ArrayList<String>(Arrays.asList(new String[] { "/echo/", "/info/" }));
        List<Handler> lh = new ArrayList<>();
        List<Searcher> openSearchers = new ArrayList<>();
        ContextHandler ch = null;

        svrConfig.put(OPEN_SEARCHERS, openSearchers);

        // do other end points here
        //
        for (Map.Entry<String, Object> endPointConfig : config.entrySet()) {
            final String endpoint = endPointConfig.getKey();
            if (endpoint.equalsIgnoreCase(SERVER_CONF_SECTION))
                continue;

            final Map<String, Object> m = (Map<String, Object>) endPointConfig.getValue();
            final String contentPath = (String) m.get("content");

            endpoints.add(endpoint);
            ch = new ContextHandler(endpoint);
            System.out.printf("\n !!! %s %s\n", endpoint, isSpecialHandler(m, IS_CGI_HANDLER_SECTION));

            if (isSpecialHandler(m, IS_INDEX_HANDLER_SECTION)) {
                try {
                    Searcher sr = new Searcher(contentPath, null, true);
                    m.put("searcher", sr);
                    openSearchers.add(sr);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                ch.setHandler(new IndexDataHandler(Collections.unmodifiableMap(m)));
                lh.add(ch);
            } else if (isSpecialHandler(m, IS_GROOVY_HANDLER_SECTION)) {
                ch.setHandler(new GroovyHandler(Collections.unmodifiableMap(m)));
                lh.add(ch);
            } else if (isSpecialHandler(m, IS_CGI_HANDLER_SECTION)) {
                ch.setHandler(new CgiHandler(Collections.unmodifiableMap(m)));
                lh.add(ch);
            } else {
                ResourceHandler rh = new ResourceHandler();
                rh.setCacheControl("no-cache, no-store, must-revalidate");
                System.out.printf("\n\n !!! CONTENT PATH %s %s\n\n", contentPath, (String) m.get("welcomeFile"));
                rh.setWelcomeFiles(new String[] { (String) m.get("welcomeFile") });
                rh.setAcceptRanges(false);
                try {
                    Path webRootPath = new File(contentPath).toPath().toRealPath();
                    rh.setBaseResource(new PathResource(webRootPath));
                    ch.setHandler(rh);

                    lh.add(ch);
                } catch (IOException e) {
                    System.err.printf("\n!!! Failed to locate resource: %s %s\n\n", contentPath, e);
                }
            }
        }

        svrConfig.put("endpoints", endpoints);
        ch = new ContextHandler("/echo");
        ch.setHandler(new BaseHandler(Collections.unmodifiableMap(svrConfig)));
        lh.add(ch);
        ch = new ContextHandler("/info");
        ch.setHandler(new ServerInfoHandler(Collections.unmodifiableMap(svrConfig)));
        lh.add(ch);

        HandlerList handlerList = new HandlerList();
        handlerList.setHandlers(lh.toArray(new Handler[lh.size()]));
        return handlerList;
    }

    private static boolean isSpecialHandler(Map<String, Object> config, String type) {
        if (Objects.isNull(config))
            return false;
        Object o = config.get(type);
        return !Objects.isNull(o) && o instanceof Boolean && ((Boolean) o).booleanValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getSvrConfig(Map<String, Object> config) {
        return (Map<String, Object>) config.get(SERVER_CONF_SECTION);
    }
}
