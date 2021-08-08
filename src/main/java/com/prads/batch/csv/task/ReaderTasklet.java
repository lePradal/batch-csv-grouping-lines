package com.prads.batch.csv.task;

import com.opencsv.CSVWriter;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import com.opencsv.bean.MappingStrategy;
import com.prads.batch.csv.model.SomeDto;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class ReaderTasklet implements Tasklet {

  @Value("${file.path}")
  private String filePath;

  @Override
  public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) {

    try {
      MappingStrategy ms = new HeaderColumnNameMappingStrategy<SomeDto>();
      ms.setType(SomeDto.class);
      Reader reader = Files.newBufferedReader(Paths.get(String.format("%s/file.csv", filePath)));
      List<SomeDto> someDtoList = new CsvToBeanBuilder(reader)
          .withSeparator(';')
          .withMappingStrategy(ms)
          .build().parse();

      someDtoList.stream().collect(Collectors.groupingBy(dto -> dto.getName())).forEach((name, someDtos) -> {
        createCsv(name, someDtos);
      });

    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }

    return RepeatStatus.FINISHED;
  }

  private void createCsv(String name, List<SomeDto> dtos) {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");
    String date = LocalDateTime.now().format(formatter);
    String filename = String.format("%s/output/%s.%s.csv", filePath, name, date);

    try (Writer writer = Files.newBufferedWriter(Paths.get(filename));
         CSVWriter csvWriter = new CSVWriter(writer, ';', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END)) {

      csvWriter.writeNext(SomeDto.getFieldNames());

      for (SomeDto dto : dtos) {
        csvWriter.writeNext(dto.toCsv());
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
