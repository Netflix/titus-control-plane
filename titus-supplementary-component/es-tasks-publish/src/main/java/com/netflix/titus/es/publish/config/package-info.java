/**
 * Package by convention in which {@link org.springframework.context.annotation.Configuration} classes
 * will reside. It is recommended to keep all of this in a single location for readability, as well as
 * ease of classpath-scanning when used in test cases. The {@link org.springframework.boot.autoconfigure.SpringBootApplication} 
 * annotation will automatically detect these configurations. 
 * @see <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-java-basic-concepts">Spring @Configuration Reference Docs</a>
 * @see <a href="https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#using-boot-structuring-your-code">Spring Boot Project Structure Best Practices</a>
 */
package com.netflix.titus.es.publish.config;