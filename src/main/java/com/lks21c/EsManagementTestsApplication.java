package com.lks21c;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;

/**
 *
 * @author lks21c
 */
@SpringBootApplication
public class EsManagementTestsApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(EsManagementTestsApplication.class);
        application.addListeners(new ApplicationPidFileWriter());
        application.run(args);

    }
}
