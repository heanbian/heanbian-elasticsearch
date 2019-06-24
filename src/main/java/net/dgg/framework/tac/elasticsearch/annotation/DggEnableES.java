package net.dgg.framework.tac.elasticsearch.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({DggESRestBuilderConfiguration.class})
@Documented
@Inherited
public @interface DggEnableES {
}
