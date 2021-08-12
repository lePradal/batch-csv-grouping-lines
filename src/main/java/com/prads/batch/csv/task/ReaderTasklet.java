package com.prads.batch.csv.task;

import com.opencsv.CSVWriter;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.MappingStrategy;
import com.prads.batch.csv.model.SomeDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class ReaderTasklet implements Tasklet {
  private final static Logger LOGGER = LoggerFactory.getLogger(ReaderTasklet.class);

  @Value("${file.extension}")
  private String fileExtension;

  @Value("${file.path}")
  private String filePath;


  @Override
  public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) {

    String odate = chunkContext.getStepContext().getJobParameters().get("odate") != null
        ? chunkContext.getStepContext().getJobParameters().get("odate").toString()
        : "*";

    try {
      PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
      Resource[] resources = resolver.getResources(String.format("file:%s/file*%s.%s", filePath, odate, fileExtension));

      for (Resource resource : resources) {
        MappingStrategy ms = new HeaderColumnNameMappingStrategy<SomeDto>();
        ms.setType(SomeDto.class);
        Reader reader = new BufferedReader(new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8));
        List<SomeDto> someDtoList = new CsvToBeanBuilder(reader)
            .withSeparator(';')
            .withMappingStrategy(ms)
            .build().parse();

        someDtoList.stream().collect(Collectors.groupingBy(dto -> dto.getName())).forEach((name, someDtos) -> {
          createCsv(name, someDtos, odate);
        });

        resource.getFile().delete();
      }


    } catch (IOException e) {
      LOGGER.error(":: Não foi possível ler o arquivo. {}", e.getMessage());
      return null;
    }

    return RepeatStatus.FINISHED;
  }

  private void createCsv(String name, List<SomeDto> dtos, String odate) {

    String date;
    DateTimeFormatter formatterOut = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    if ("*".equals(odate)) {
      date = LocalDateTime.now().format(formatterOut);;
    } else {
      DateTimeFormatter formatterIn = DateTimeFormatter.ofPattern("yyyyMMdd");
      date = LocalDate.parse(odate, formatterIn).atStartOfDay().format(formatterOut);
    }

    String filename = String.format("%s/output/%s.%s.csv", filePath, name, date);

    try (Writer writer = Files.newBufferedWriter(Paths.get(filename));
         CSVWriter csvWriter = new CSVWriter(writer, ';', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {

      csvWriter.writeNext(SomeDto.getFieldNames());

      for (SomeDto dto : dtos) {
        csvWriter.writeNext(dto.toCsv());
      }

    } catch (IOException e) {
      LOGGER.error(":: Não foi possível escrever o arquivo para o name: {}.", name, e);
    }
  }
}
