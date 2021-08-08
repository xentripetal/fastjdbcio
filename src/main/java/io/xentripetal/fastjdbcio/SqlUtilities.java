package io.xentripetal.fastjdbcio;

import java.util.List;

public class SqlUtilities {
    public static String toColumnSelect(List<String> columns) {
        StringBuilder selector = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            selector.append(columns.get(i));
            if (i < columns.size() - 1) {
                selector.append(", ");
            }
        }
        return selector.toString();
    }
}
