package com.spring.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.flink.api.common.state.MapState;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zt
 */
@Data
public class SpelUtil {
    private static ThreadLocal<EvaluationContext> testContext = new ThreadLocal<>();

    private static void initThreadLocal() {
        if (testContext.get() == null) {
            testContext.set(new StandardEvaluationContext());
        }
    }

    public static EvaluationContext getContext() {
        initThreadLocal();
        return testContext.get();
    }

    public static void setContextVariable(String key, Object value) {
        getContext().setVariable(key, value);
    }

    public static void removeContextVariable(String key) {
        getContext().setVariable(key, null);
    }

    public static MapState<String, JSONObject> getState(String key) {
        return (MapState<String, JSONObject>) getContext().lookupVariable(key);
    }

}
