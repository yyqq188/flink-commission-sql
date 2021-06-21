package gientech.common;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * author: yhl
 * time: 2021/6/18 下午4:40
 * company: gientech
 */
public class MyBlinkStringUtils {
    public static String[] splitIgnoreQuotaBrackets(String str, String delimter){
        String splitPatternStr = delimter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)";
        return str.split(splitPatternStr);
    }

    public static List<String> splitIgnoreQuota(String str, char delimiter){
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if(c == delimiter){
                if (inQuotes) {
                    b.append(c);
                } else if(inSingleQuotes){
                    b.append(c);
                }else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            }else if(c == '\"'){
                inQuotes = !inQuotes;
                b.append(c);
            }else if(c == '\''){
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            }else{
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    public static List<String> splitSemiColon(String str) {
        boolean inQuotes = false;
        boolean escape = false;

        List<String> ret = new ArrayList<>();

        char quoteChar = '"';
        int beginIndex = 0;
        for (int index = 0; index < str.length(); index++) {
            char c = str.charAt(index);
            switch (c) {
                case ';':
                    if (!inQuotes) {
                        ret.add(str.substring(beginIndex, index));
                        beginIndex = index + 1;
                    }
                    break;
                case '"':
                case '\'':
                    if (!escape) {
                        if (!inQuotes) {
                            quoteChar = c;
                            inQuotes = !inQuotes;
                        } else {
                            if (c == quoteChar) {
                                inQuotes = !inQuotes;
                            }
                        }
                    }
                    break;
                default:
                    break;
            }

            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            }
        }

        if (beginIndex < str.length()) {
            ret.add(str.substring(beginIndex));
        }

        return ret;
    }

    public static boolean isContain(String regex, String sql){
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql);
        if(matcher.find()){
            return true;
        }
        return false;
    }

    public static boolean isChinese(String str) {
        if (str == null) return false;
        for (char c : str.toCharArray()) {
            if (isChinese(c)) return true;
        }
        return false;
    }

    private static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS

                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS

                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A

                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION

                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION

                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {

            return true;
        }
        return false;
    }
}
