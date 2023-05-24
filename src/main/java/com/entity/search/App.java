package com.entity.search;

import com.entity.util.*;

import java.util.Arrays;

import com.entity.indexing.*;
import com.entity.server.*;
import com.entity.tools.GroovyInterpreter;

public class App {
    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            usage();
        }
        String feature = args[0];
        String[] newArgs = adjustArgs(args);

        if (feature.equalsIgnoreCase("server")) {
            DataServer.main(newArgs);
        } else if (feature.equalsIgnoreCase("gi")) {
            GroovyInterpreter.main(newArgs);
        } else if (feature.equalsIgnoreCase("searcher")) {
            Searcher.main(newArgs);
        } else {
            System.out.println("unknown feature selected");
            usage();
        }
    }

    private static String[] adjustArgs(String[] args) {
        return args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[] {};
    }

    private static void usage() throws Exception {
        System.out.println(ResourceUtils.slurpResourceFile("help.txt"));
        System.exit(1);
    }
}
