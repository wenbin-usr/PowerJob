package tech.powerjob.server.auth.common.utils;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Objects;

/**
 * 获取常用的请求头
 *
 * @author tjq
 * @since 2025/1/21
 */
public class AuthHeaderUtils {

    public static String fetchAppId(HttpServletRequest request) {
        return HttpServletUtils.fetchFromHeader("AppId", request);
    }
    public static Long fetchAppIdL(HttpServletRequest request) {
        return Long.valueOf(Objects.requireNonNull(HttpServletUtils.fetchFromHeader("AppId", request), "AppId from header is null!"));
    }

    public static String fetchNamespaceId(HttpServletRequest request) {
        return HttpServletUtils.fetchFromHeader("NamespaceId", request);
    }
    public static Long fetchNamespaceIdL(HttpServletRequest request) {
        return Long.valueOf(Objects.requireNonNull(HttpServletUtils.fetchFromHeader("NamespaceId", request), "NamespaceId from header is null!"));
    }
}