package com.nus.iss.bd.coviddataproducer.service;

import com.nus.iss.bd.coviddataproducer.dto.CaseRecordDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class CovidCaseApiService {

    @Value(value = "${covidCase.endpoint}")
    private String covidCaseEndpoint;

    @Value(value = "${spring.mvc.format.date-time}")
    private String dateTimeFormat;

    @Autowired
    RestTemplate restTemplate;

    public List<CaseRecordDto> getCases(final LocalDateTime dateTime){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateTimeFormat);
        String url = covidCaseEndpoint + "?datetime=" + dateTime.format(formatter);
        ResponseEntity<CaseRecordDto[]> responseEntity = restTemplate.getForEntity(url,CaseRecordDto[].class);
        CaseRecordDto[] cases = responseEntity.getBody();
        return Stream.of(cases).collect(Collectors.toList());
    }

}
