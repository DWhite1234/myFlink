package com.spring.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.common.state.MapState;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

/**
 * @author zt
 */
@Data
public class SpelUtil {
    private static ThreadLocal<EvaluationContext> CONTEXT = new ThreadLocal<>();
    /** el表达式解析器*/
    private static ThreadLocal<ExpressionParser> PARSER = new ThreadLocal<>();
    private static void initThreadLocal() {
        if (CONTEXT.get() == null) {
            CONTEXT.set(new StandardEvaluationContext());
        }
        if (PARSER.get()==null) {
            PARSER.set(new SpelExpressionParser());
        }
    }

    public static EvaluationContext getContext() {
        initThreadLocal();
        return CONTEXT.get();
    }

    public static ExpressionParser getParser() {
        initThreadLocal();
        return PARSER.get();
    }

    /**
     * 解析el表达式
     * @param expression
     * @return
     */
    public static boolean parseEl(String expression) {

        return BooleanUtils.isTrue(getParser().parseExpression(expression).getValue(getContext(),Boolean.class));
    }

    /**
     * 将对象存入CONTEXT
     * @param key
     * @param value
     */
    public static void setContextVariable(String key, Object value) {
        getContext().setVariable(key, value);
    }

    /**
     *
     * @param key
     * @return
     */
    public static Object getContextVariable(String key) {
        return getContext().lookupVariable(key);
    }

    public static void removeContextVariable(String key) {
        getContext().setVariable(key, null);
    }

    public static MapState<String, JSONObject> getState(String key) {
        return (MapState<String, JSONObject>) getContext().lookupVariable(key);
    }

}
