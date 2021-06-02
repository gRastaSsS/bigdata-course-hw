package bigdata;

import org.apache.spark.sql.api.java.UDF1;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public final class Utils {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("d.M.yyyy");
    private static final Pattern COMPILE = Pattern.compile("\\p{P}");

    private Utils() {
    }

    private static Integer calcAge(CharSequence bdate) {
        if (bdate == null) {
            return null;
        }

        try {
            LocalDate birthdate = LocalDate.parse(bdate, TIME_FORMATTER);
            LocalDate now = LocalDate.now();
            return Period.between(birthdate, now).getYears();
        } catch (RuntimeException e) {
            return null;
        }
    }

    public static UDF1<String, Integer> AGE_CALCULATOR = (UDF1<String, Integer>) Utils::calcAge;

    public static UDF1<String, String> PUNCTUATION_REMOVER = (UDF1<String, String>) s -> COMPILE.matcher(s)
        .replaceAll("");
}
