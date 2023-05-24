package com.entity.util;

import java.io.*;

public class ResourceUtils {
    private ResourceUtils() {
    }

    public static String slurpResourceFile(String rscPath) throws Exception {
        InputStream ins = SimpleUtils.pathToInStream(rscPath);
        return IOUtils.toString(ins);
    }
}
