package com.atguigu.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD) //注解作用域
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {

}
