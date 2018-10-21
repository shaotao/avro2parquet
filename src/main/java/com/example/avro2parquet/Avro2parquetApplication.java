package com.example.avro2parquet;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class Avro2parquetApplication {

    private static final String OPTION_AVRO2PARQUET = "avro2parquet";
    private static final String OPTION_JSON2AVRO = "json2avro";
    private static final String OPTION_READ_AVRO = "read_avro";
    private static final String OPTION_READ_PARQUET = "read_parquet";

    private static final String OPTION_AVRO = "avro";
    private static final String OPTION_PARQUET = "parquet";
    private static final String OPTION_NUM = "num";
    private static final String OPTION_JSON = "json";
    private static final String OPTION_AVSC = "avsc";

    private static final long DEFAULT_RECORDS_TO_READ = 10;

    private static final HelpFormatter helpFormatter = new HelpFormatter();

	public static void main(String[] args) throws Exception {
		//SpringApplication.run(Avro2parquetApplication.class, args);
		System.out.println("=== avro to parquet ===");

        CommandLineParser parser = new DefaultParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption(OPTION_AVRO2PARQUET)) {
            log.info("=== action is avro2parquet");
            String avroFilePath = cmd.getOptionValue(OPTION_AVRO);
            String parquetFilePath = cmd.getOptionValue(OPTION_PARQUET);
            if (StringUtils.isBlank(avroFilePath)) {
                log.error("avro input file path should not be null!");
                return;
            } else if (StringUtils.isBlank(parquetFilePath)) {
                parquetFilePath = String.format("%s.parquet", avroFilePath);
                log.warn(String.format("parquet output file path is null, use default: %s", parquetFilePath));
            }
            log.info(String.format("avro file path = %s", avroFilePath));
            log.info(String.format("parquet file path = %s", parquetFilePath));
            FileFormatUtil.convertAvroToParquet(avroFilePath, parquetFilePath);
        } else if (cmd.hasOption(OPTION_READ_AVRO)) {
            log.info("=== action is read-avro");
            String avroFilePath = cmd.getOptionValue(OPTION_AVRO);
            if (StringUtils.isBlank(avroFilePath)) {
                log.error("avro input file path should not be null!");
                return;
            }
            String numString = cmd.getOptionValue(OPTION_NUM);
            long num = DEFAULT_RECORDS_TO_READ;
            if (numString != null) {
                num = Long.parseLong(numString);
                if (num <= 0) {
                    log.error("# of records to read should be positive!");
                    return;
                }
            } else {
                log.info(String.format("no num option used. use default # of records to read: %d", num));
            }
            log.info(String.format("avro file path = %s", avroFilePath));
            log.info(String.format("# of records to read = %d", num));
            String ret = AvroUtil.readAvro(avroFilePath, num);
            log.info(ret);
        } else if (cmd.hasOption(OPTION_READ_PARQUET)) {
            String numString = cmd.getOptionValue(OPTION_NUM);
            long num = DEFAULT_RECORDS_TO_READ;
            if (numString != null) {
                num = Long.parseLong(numString);
                if (num <= 0) {
                    log.error("# of records to read should be positive!");
                    return;
                }
            } else {
                log.info(String.format("no num option used. use default # of records to read: %d", num));
            }
            log.info("=== action is read-parquet");
            String parquetFilePath = cmd.getOptionValue(OPTION_PARQUET);
            if (StringUtils.isBlank(parquetFilePath)) {
                log.error("parquet input file path should not be null!");
                return;
            }
            log.info(String.format("parquet file path = %s", parquetFilePath));
            log.info(String.format("# of records to read = %d", num));
            String ret = ParquetUtil.readParquet(parquetFilePath, num);
            log.info(ret);
        } else {
            helpFormatter.printHelp("java -jar avro2parquet-1.0.jar", options);
        }
	}

	public static Options buildOptions() {
	    Options options = new Options();
	    OptionGroup optionGroup = new OptionGroup();

        optionGroup.addOption(new Option(OPTION_AVRO2PARQUET, false, "perform avro2parquet convertion"));
        optionGroup.addOption(new Option(OPTION_JSON2AVRO, false, "perform json2avro convertion"));
        optionGroup.addOption(new Option(OPTION_READ_AVRO, false, "read avro file"));
        optionGroup.addOption(new Option(OPTION_READ_PARQUET, false, "read parquet file"));
        options.addOptionGroup(optionGroup);

        // for avro2parquet
	    options.addOption(new Option(OPTION_AVRO, true, "for avro2parquet: input avro file"));
		options.addOption(new Option(OPTION_PARQUET, true, "for avro2parquet: output parquet file"));

		// for json2avro
        options.addOption(new Option(OPTION_JSON, true, "for json2avro: input json file"));
        options.addOption(new Option(OPTION_AVSC, true, "for json2avro: input avro schema file"));

        // for read_avro and read_parquet
        options.addOption(new Option(OPTION_NUM, true, "for read_avro and read_paquet: # of records to read"));

	    return options;
    }
}
