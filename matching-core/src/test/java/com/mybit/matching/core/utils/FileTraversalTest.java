package com.mybit.matching.core.utils;

import org.junit.jupiter.api.Test;

import java.util.Collection;

public class FileTraversalTest {

    @Test
    public void testEnumFiles() {
        String path = "C:\\tmp\\matching\\"; // 指定要遍历的文件夹路径

        Collection<String> files = FileTraversal.enumFiles(path,  null,"offset-", ".log");
        System.out.println(files);

        files = FileTraversal.enumFiles(path, null,"offset-", null);
        System.out.println(files);

        files = FileTraversal.enumFiles(path, null,null, ".log");
        System.out.println(files);

        files = FileTraversal.enumFiles(path, null,"orderbook-", ".log");
        System.out.println(files);

        files = FileTraversal.enumFiles(path, "ETH2","orderbook-", null);
        System.out.println(files);

    }
}
