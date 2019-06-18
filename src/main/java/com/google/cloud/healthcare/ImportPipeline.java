// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.healthcare;

import com.google.cloud.healthcare.config.CsvConfiguration;
import com.google.cloud.healthcare.config.GcpConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.devtools.common.options.Converter;
import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingException;
import java.io.IOException;
import java.util.Collections;

/**
 * This is the entrance class for the importing pipeline.
 */
public class ImportPipeline {

  /** For converting string input to characters */
  public static class CharOptionsConverter implements Converter<Character> {
    @Override
    public Character convert(String input) throws OptionsParsingException {
      if (Strings.isNullOrEmpty(input) || input.length() > 1) {
        throw new OptionsParsingException("Not a character.");
      }

      return input.charAt(0);
    }

    @Override
    public String getTypeDescription() {
      return "CSV delimiter";
    }
  }

  /** Customized option parser. */
  public static class PipelineOptions extends OptionsBase {

    @Option(
        name = "help",
        abbrev = 'h',
        help = "Print usage information.",
        defaultValue = "false")
    public boolean help;

    @Option(
        name = "csv_delimiter",
        help = "The delimiter used to separator CSV columns.",
        category = "csv",
        converter = CharOptionsConverter.class,
        defaultValue = ",")
    public char csvDelimiter;

    @Option(
        name = "csv_quote",
        help = "The quote character used in the file.",
        category = "csv",
        converter = CharOptionsConverter.class,
        defaultValue = "\"")
    public char csvQuoteChar;

    @Option(
        name = "csv_record_separator",
        help = "Row separator, typically this is either '\n' or '\r\n'.",
        category = "csv",
        defaultValue = "\n")
    public String csvRecordSeparator;

    @Option(
        name = "csv_delimiter_regex",
        help = "Optional: A regular expression used to separate fields in a record. Lookahead and"
            + "lookbehind can be used. This has to be used in combination with"
            + "csv_record_separator_regex.",
        category = "csv",
        defaultValue = "")
    public String csvDelimiterRegex;

    @Option(
        name = "csv_record_separator_regex",
        help = "Optional: A regular expression used to separate records. This has to be used in"
            + "combination wtih csv_delimiter_regex.",
        category = "csv",
        defaultValue = "")
    public String csvRecordSeparatorRegex;

    // TODO(b/120795556): update this to take a list of URIs (file or directory, can have
    // wildcards).
    @Option(
        name = "gcs_uri",
        help = "The URI of the source file on GCS.",
        defaultValue = "")
    public String gcsUri;

    @Option(
        name = "bq_dataset",
        help = "The BigQuery dataset to import the data.",
        defaultValue = "")
    public String bigQueryDataset;

    @Option(
        name = "temp_bucket",
        help = "Used to store temporary files.",
        defaultValue = "")
    public String tempBucket;

    @Option(
        name = "gcp_project_id",
        help = "The project id used to run the pipeline.",
        defaultValue = "")
    public String projectId;

    @Option(
        name = "gcp_credentials",
        help = "Path to the credentials (usually a .json file) of a service account used to access"
            + "resources (GCS, Dataflow, BigQuery), current users credentials will be used if not"
            + "specified.",
        defaultValue = "")
    public String gcpCredentials;

    @Option(
        name = "dataflow_controller_service_account",
        help = "Customized Dataflow controller service account, see"
            + "https://cloud.google.com/dataflow/docs/concepts/security-and-permissions"
            + "#controller_service_account. The default will be used if not specified.",
        defaultValue = "")
    public String dataflowControllerServiceAccount;

    @Option(
        name = "verbose",
        abbrev = 'v',
        help = "Whether to output verbose messages.",
        defaultValue = "false")
    public boolean verbose;

    public void printUsage(OptionsParser parser) {
      System.out.println("Usage: java -jar import.jar OPTIONS");
      System.out.println(
          parser.describeOptions(
              Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
    }
  }

  private static void validateAndConstructOptions(PipelineOptions options) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.gcsUri),
        "GCS URI is required to provide the source file.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.bigQueryDataset),
        "BigQuery dataset and table are required.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(options.tempBucket),
        "Temporary bucket is required.");
    boolean isCsvRecordSeparatorRegexSet = !Strings.isNullOrEmpty(options.csvRecordSeparatorRegex);
    boolean isCsvDelimiterRegexSet = !Strings.isNullOrEmpty(options.csvDelimiterRegex);
    Preconditions.checkArgument(!(isCsvDelimiterRegexSet ^ isCsvRecordSeparatorRegexSet),
        "csv_delimiter_regex and csv_record_separator_regex need to be specified together.");

    CsvConfiguration.getInstance()
        .withRecordSeparator(options.csvRecordSeparator)
        .withDelimiter(options.csvDelimiter)
        .withQuote(options.csvQuoteChar);

    if (isCsvDelimiterRegexSet && isCsvRecordSeparatorRegexSet) {
      CsvConfiguration.getInstance()
          .withDelimiterRegex(options.csvDelimiterRegex)
          .withRecordSeparatorRegex(options.csvRecordSeparatorRegex);
    }

    GcpConfiguration.getInstance().withCredentials(options.gcpCredentials);
  }

  public static void main(String[] argv) {
    OptionsParser parser = OptionsParser.builder().optionsClasses(PipelineOptions.class).build();
    parser.parseAndExitUponError(argv);
    PipelineOptions options = parser.getOptions(PipelineOptions.class);

    if (options.help) {
      options.printUsage(parser);
      return;
    }

    try {
      validateAndConstructOptions(options);
      PipelineRunner.run(options.projectId, options.dataflowControllerServiceAccount,
          options.bigQueryDataset, options.tempBucket, options.gcsUri);
    } catch (Exception e) {
      if (options.verbose) {
        throw new RuntimeException(e);
      } else {
        System.out.println(e.getMessage());
      }
    }
  }
}
