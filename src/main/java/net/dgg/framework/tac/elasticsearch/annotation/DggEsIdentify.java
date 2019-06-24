package net.dgg.framework.tac.elasticsearch.annotation;

/**
 * 索引es对象的id标识
 *
 * @author liuliwei
 * @create 2018-11-21
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DggEsIdentify {
}