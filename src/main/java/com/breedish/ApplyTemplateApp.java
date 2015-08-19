package com.breedish;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class ApplyTemplateApp implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ApplyTemplateApp.class);

    public static void main(String[] args) {
        SpringApplication.run(ApplyTemplateApp.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        if (strings.length == 0) {
            log.error("No inventory URL is specified");
            return;
        }
        final String url = strings[0];
        final String host = strings.length > 1 ? strings[1] : "vin-web.poc-vin.cloud.edmunds.com:8080";

        final String templatesEndpoint = String.format("http://%s/vinspy/v1/templates", host);

        ResponseEntity<Templates> templates = new RestTemplate().getForEntity(
            templatesEndpoint,
            Templates.class
        );

        List<Template> templatesList = templates.getBody().items;

        Collections.sort(templatesList, new Comparator<Template>() {
            @Override
            public int compare(Template o1, Template o2) {
                return o1.id.compareTo(o2.id);
            }
        });

        int templatesCount = templatesList.size();
        log.info("Total templates: {}", templatesCount);

        final ExecutorService pool = Executors.newFixedThreadPool(15);
        final ExecutorCompletionService<Triplet<Long, String, String>> completionService = new ExecutorCompletionService<>(pool);

        for (final Template template : templatesList) {
            completionService.submit(new Callable<Triplet<Long, String, String>>() {
                @Override
                public Triplet<Long, String, String> call() throws Exception {
                    Inventory inventory = new RestTemplate()
                        .postForObject(
                            String.format("http://%s/vinspy/v1/templates/%s/test", host, template.id),
                            ImmutableMap.of("url", url),
                            Inventory.class
                        );
                    return new Triplet<>(template.id, inventory.vin, inventory.title);
                }
            });
        }

        TreeMap<Long, Pair<String, String>> results = new TreeMap<>();

        for (int i = 0; i < templatesCount; ++i) {
            final Future<Triplet<Long, String, String>> future = completionService.take();
            try {
                final Triplet<Long, String, String> content = future.get();
                results.put(content.getValue0(), new Pair<>(content.getValue1(), content.getValue2()));
            } catch (ExecutionException e) {
                log.warn("Error while applying template", e.getCause());
            }
        }

        for (Map.Entry<Long, Pair<String, String>> entry : results.entrySet()) {
            log.info("{}: {} - {}", entry.getKey(), entry.getValue().getValue0(), entry.getValue().getValue1());
        }

        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Templates {
        Integer total;
        List<Template> items;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Template {
        Long id;
        String quote;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Inventory {
        @JsonProperty("VIN")
        String vin;
        @JsonProperty("TITLE")
        String title;
    }
}
