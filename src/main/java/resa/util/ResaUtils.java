package resa.util;

/**
 * Created by ding on 14-6-26.
 */
public class ResaUtils {

    public static <T> T newInstance(String className, Class<? extends T> castClass) {
        try {
            return Class.forName(className).asSubclass(castClass).newInstance();
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * Create a new instance, throw RuntimeException if failed.
     *
     * @param className
     * @param castClass
     * @param <T>
     * @return
     */
    public static <T> T newInstanceThrow(String className, Class<? extends T> castClass) {
        try {
            return Class.forName(className).asSubclass(castClass).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
