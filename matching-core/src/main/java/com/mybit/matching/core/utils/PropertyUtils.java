package com.mybit.matching.core.utils;

import java.util.Iterator;
import java.util.Properties;

public class PropertyUtils {

    /**
     * 合并参数配置
     *
     * @param dst
     * @param src
     * @return
     */
    public static Properties merge(Properties dst, Properties src) {
        if (dst == null) {
            return src;
        } else if (src == null) {
            return dst;
        } else {
            Iterator<Object> iterator = src.keys().asIterator();
            while (iterator.hasNext()) {
                Object key = iterator.next();
                if (!dst.containsKey(key)) {
                    dst.put(key, src.get(key));
                }
            }
        }
        return dst;
    }

}
