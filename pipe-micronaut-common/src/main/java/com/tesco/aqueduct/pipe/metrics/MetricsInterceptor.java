package com.tesco.aqueduct.pipe.metrics;

import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Singleton;

@Singleton
public class MetricsInterceptor implements MethodInterceptor<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger("metrics");

    @Override
    public Object intercept(MethodInvocationContext<Object, Object> context) {
        long methodStartTime = System.currentTimeMillis();
        try {
            return context.proceed();
        } finally {
            MDC.put("timeMs", Long.toString(System.currentTimeMillis() - methodStartTime));
            MDC.put("class", context.getDeclaringType().getCanonicalName());
            MDC.put("method", context.getMethodName());
            LOG.info(null);
            MDC.clear();
        }
    }
}
