package com.mybit.matching.core.utils;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FileTraversal {

    public static Collection<String> enumFiles(String path, String contains, String prefix, String ext) {
        File file = new File(path);
        if (!file.exists()) return Collections.emptyList();

        List<String> list = Lists.newArrayList();
        enumFiles(file, contains, prefix, ext, list);
        return list;
    }

    private static void enumFiles(File folder, String contains, String prefix, String ext, List<String> list) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles(file -> {
                if (file.isDirectory()) {
                    return true;
                }
                String fileName = file.getName();
                if (prefix != null && !fileName.startsWith(prefix)) {
                    return false;
                }
                if (contains != null && !fileName.contains(contains)) {
                    return false;
                }
                if (ext != null && !fileName.endsWith(ext)) {
                    return false;
                }
                return true;
            });

            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        enumFiles(file, contains, prefix, ext, list);
                    } else {
                        list.add(file.getAbsolutePath());
                    }
                }
            }
        }
    }

}
