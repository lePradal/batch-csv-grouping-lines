package com.prads.batch.csv.config;

import com.prads.batch.csv.listener.JobCompletionListener;
import com.prads.batch.csv.task.ReaderTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job processJob() {
        return jobBuilderFactory.get("processJob")
                .incrementer(new RunIdIncrementer()).listener(listener()).start(readCsvStep()).build();
    }

    @Bean
    public Step readCsvStep() {
        return stepBuilderFactory.get("readCsvStep").tasklet(readerTasklet()).build();
    }

    @Bean
    public ReaderTasklet readerTasklet() {
        return new ReaderTasklet();
    }

    @Bean
    public JobExecutionListener listener() {
        return new JobCompletionListener();
    }

}
