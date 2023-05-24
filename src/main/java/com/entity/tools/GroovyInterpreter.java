package com.entity.tools;

import groovy.lang.Binding;
import groovy.lang.Script;
import groovy.lang.GroovyShell;
//import groovy.util.GroovyScriptEngine;
import java.util.*;
import com.entity.util.*;

public class GroovyInterpreter {
    public static void runScript(String... args) throws Exception {
        runScript(new Binding(), args);
    }

    // --------------------------------------------------------------------------
    public static void runScript(Binding binding, String... args)
            throws Exception {
        if (binding == null) {
            binding = new Binding();
        }
        GroovyShell gs = new GroovyShell(binding);
        Script script = null;

        Map<String, String> params = new HashMap<String, String>();
        List<String> args_ = new ArrayList<String>();

        for (int i = 0; i < args.length; i++) {
            // System.out.printf( "ARG%d %s\n", i, args[i] );
            String[] ss = args[i].split("=", 2);
            if (ss != null && ss.length > 1) {
                params.put(ss[0], ss[1]);
            } else {
                args_.add(args[i]);
            }
        }

        String[] a = args_.toArray(new String[args_.size()]);
        String text = SimpleUtils.slurpFile(SimpleUtils.pathToInStream(a[0]));

        script = gs.parse(text);

        script.setProperty("args", a);
        script.setProperty("named_args", params);

        script.run();
    }

    // --------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("First argument is full path to groovy script file");
            return;
        }

        if (args[0].startsWith("-e")) {
            // evaluate one-liner
            //
            GroovyShell gs = new GroovyShell(new Binding());
            String command = args.length > 1 ? args[1] : args[0].replace("-e", "");
            gs.evaluate(command);
        } else {
            // ... or run script
            //
            runScript(args);
        }
    }
}
