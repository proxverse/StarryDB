package org.apache.spark.sql.execution.columnar.expressions.convert;

public final class StringUtils {
  static final char[] REGEX_CHARS = ".$|()[{^?*+\\".toCharArray();


  public static boolean isRegex(String regex) {
    for (char c : REGEX_CHARS) {
      if (regex.contains(String.valueOf(c))) {
        return true;
      }
    }
    return false;
  }
}
