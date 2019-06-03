# BigQuery Data Importer

The purpose of this tool is to import raw CSV (or CSV-like) data in
[GCS](https://cloud.google.com/storage/) to
[BigQuery](https://cloud.google.com/bigquery/).

At times the autodetect mode in BigQuery fails to detect the expected schema of
the source data, in which case it is required to iterate over all the data to
determine the correct one.

This tool tries to first decompress the source file if necessary, then attempts
to split (see details below) the source file into multiple small chunks and
infer schema for each chunk, at last merge all the schema into a final one. With
the help of the schema, the source data is converted to
[AVRO](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro)
format to accelerate importing.

[Dataflow](https://cloud.google.com/dataflow/) is used to parallelize the import
process.

### File Split

For large files, a series of preliminary split points are chosen by calculating
the number of chunks to split (based on the estimated chunk size, which is 2MB),
a search is initiated starting from each split point, looking for a point which
doesn't lie in the middle of a logical row (rows can span multiple lines).

In order to tell whether a point is a valid split point, quotes on each line are
categorized: opening (which starts a field in quotes), closing (which finishes a
field in quotes) or unknown. It is easy to determine the split points by looking
at the types of quotes on each line. For lines without quotes, the headers are
used to assist. Each chunk is processed independently going forward.

### Advanced Mode

Sometimes the files to import are not standard CSVs, an advanced mode is
provided, where two regular expressions can be provided to describe how to split
records and fields respectively, the tool will use these regular expressions to
break the files into chunks and fields.

Please see the Usage section for how to use the advance mode.

## Usage

### Prerequisites

*   A GCP (Google Cloud Platform) project.
*   GCS, BigQuery and Dataflow APIs are enabled.
    *   The runner (either end user or service account as recommended below)
        needs to have the following roles at the project level:
        -   `roles/bigquery.dataViewer`
        -   `roles/bigquery.jobUser`
        -   `roles/bigquery.user`
        -   `roles/compute.viewer`
        -   `roles/dataflow.developer`
    *   The dataflow
        [controller service account](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#controller_service_account)
        needs `roles/storage.admin` on the temporary bucket (provided to the
        pipeline by flag `--temp_bucket`, see below). Besides, it needs
        `roles/bigquery.dataEditor` on the target BigQuery dataset.
        *   Alternatively, you could use a customized controller service account
            `--dataflow_controller_service_account` (which has to be
            roles/dataflow.worker). In this case you only have to manage one
            service account.
*   [Google Cloud SDK](https://cloud.google.com/sdk/) is installed.
*   JDK 8+ is installed.
*   Gradle is installed.

### Import

For security reasons, it is recommended to run this tool with a
[service account](https://cloud.google.com/iam/docs/understanding-service-accounts).
It is assumed that you have a service account configured, and the JSON key has
been downloaded to your disk, for how to do that, please follow the tutorials
[here](https://cloud.google.com/iam/docs/creating-managing-service-accounts).

All the following should run on a console unless otherwise specified.

*   Switch the default project. Note you need to replace the project name with
    yours. You can skip this step if you only have one project.

`gcloud config set project <my-project>`

*   Run the import command. Note you need to replace the GCS URIs and BQ dataset
    with yours.

```shell
./gradlew run -PappArgs="[\
'--gcp_project_id', 'my-project',\
'--gcs_uri', 'gs://my-bucket/my-file.gz',\
'--bq_dataset', 'my-dataset',\
'--temp_bucket', 'my-temp-bucket',\
'--gcp_credentials', 'my-project-some-hash.json',\
'--dataflow_controller_service_account', 'my-dataflow@my-project-gserviceaccount.com',\
'--verbose', 'true'
]"
```

*   Leave the command running. Now you can track the import progress on the
    [Dataflow tab](https://console.cloud.google.com/dataflow).

#### Explanation of Arguments:

*   `--gcp_project_id`: The GCP project in which the pipeline will be running.
*   `--gcs_uri`: The URI of the input to import, it has to start with `gs://`
    since this is expected to be a GCS URI.
*   `--bq_dataset`: The BigQuery dataset to import the data to, the BigQuery
    tables are created automatically with the names of files.
*   `--temp_bucket`: A GCS bucket to store temporary artifacts, for example:
    decompressed data, compiled Cloud Dataflow pipeline code etc. The data will
    be removed after the pipeline finishes.
*   `--gcp_credentials`: Credentials of the service account, this can be
    downloaded from the console. Note using a service account is strongly
    recommended. Please find the link to how to set up a service account at the
    beginning of this README.
*   `--dataflow_controller_service_account`: Optional. Set the Cloud Dataflow
    service account which is used by the workers to access resources. Usually
    this is set to be the same as the service account created to run this
    pipeline. You don't have to set it if using the default GCE service account
    is desired, but make sure the default service account has access to the
    resources required to run the pipeline.
*   `--verbose`: print verbose error messages for debugging, can be omitted.

#### Custom CSV Options

*   `--csv_delimiter`: Character used to separate columns, typically ','. For
    multiple character delimiter (non-standard), use advanced mode.
*   `--csv_quote`: Quote character, typically '"' in standard CSV.
*   `--csv_record_seperator`: Single or multiple characters to separate rows,
    typically CR, LF, or CRLF depending on the platform.

#### Advanced Mode

All the regular expression should conform to the
[Java spec](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html).

*   `--csv_delimiter_regex`: A regular expression used to separate columns.
    Usually with a lookahead and lookbehind group (but not mandatory). For
    example: "(?<=\\\\d{5, 10}),(?=\\\\w{2})", the tool will break each row with
    this regular expression, i.e. the comma will be stripped.
*   `--csv_record_separator_regex`: A regular expression used to separate
    records. Usually with a forward- and a behind- looking group (but not
    mandatory). For example: "\\r\\n(?=\\\\d{5})", the tool will set the split
    point after the new line character.

Note that in terms of splitting the files, advanced mode is typically slower
than normal mode, and these options are not compatible with those of normal
mode.

## Limitations

*   Right now this tool processes standard CSVs (i.e. follows
    [RFC4180](https://tools.ietf.org/html/rfc4180)) and CSV-like files which
    have meaningful record and field separators (meaning can be written as
    regular expressions in Advanced mode).
*   This tool takes one file at a time, but you can zip or tar multiple files
    for the tool to process.
*   All files are required to have headers, which will be used as column names
    in BigQuery, headers will be transformed into proper format accepted by
    BigQuery if necessary.
*   The base names of the files are used as the table names in BigQuery, so make
    sure there are no files share the same name(s).
