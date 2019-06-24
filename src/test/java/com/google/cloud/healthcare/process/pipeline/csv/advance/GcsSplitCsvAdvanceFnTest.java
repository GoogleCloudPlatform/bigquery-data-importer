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

package com.google.cloud.healthcare.process.pipeline.csv.advance;

import com.google.cloud.healthcare.config.CsvConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Test for {@link GcsSplitCsvAdvanceFn}. */
@RunWith(Parameterized.class)
public class GcsSplitCsvAdvanceFnTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Parameter
  public String testInputFilename;

  @Parameter(1)
  public Set<Long> expectedSplitPoints;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {"test_input_no_split.csv", Sets.newHashSet(22L, 218L)},
        {"test_input_no_split2.csv", Sets.newHashSet(22L, 871L)},
        {"test_input_with_quotes.csv", Sets.newHashSet(22L, 517L, 1006L)},
        {"test_input_without_quotes.csv", Sets.newHashSet(22L, 514L, 882L)},
        {"test_input_mixed_quotes.csv", Sets.newHashSet(22L, 537L, 1045L, 1558L, 2055L, 2332L)},
        {"test_input_all_lines_have_new_lines.csv", Sets.newHashSet(22L, 571L, 1059L, 1181L)},
        {"test_input_advance_split.csv", Sets.newHashSet(31L, 743L, 1091L, 1459L)}
    });
  }

  @Test
  public void split() {
    URL url = this.getClass().getClassLoader().getResource(testInputFilename);

    Map<String, String[]> headers = ImmutableMap.<String, String[]>builder()
        .put(url.getPath(), new String[] {"Year", "Make", "Description"}).build();
    PCollectionView<Map<String, String[]>> headersView = p.apply(Create.of(headers))
        .apply(View.asMap());

    PCollection<ReadableFile> input = p
        .apply(FileIO.match().filepattern(url.toString()))
        .apply(FileIO.readMatches());
    PCollection<KV<String, Set<Long>>> output = input
        .apply(ParDo.of(new TestGcsSplitCsvAdvanceFn(
            CsvConfiguration.getInstance().withRecordSeparatorRegex("\n(?=\\d{4},)"), headersView))
            .withSideInputs(headersView));
    PAssert.thatSingleton(output).isEqualTo(KV.of(url.getPath(), expectedSplitPoints));
    p.run();
  }

  static class TestGcsSplitCsvAdvanceFn extends GcsSplitCsvAdvanceFn {
    TestGcsSplitCsvAdvanceFn(CsvConfiguration config,
        PCollectionView<Map<String, String[]>> headersView) {
      super(config, headersView);
      CHUNK_SIZE = 512;
    }
  }
}