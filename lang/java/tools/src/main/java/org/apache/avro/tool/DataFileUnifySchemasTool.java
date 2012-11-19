package org.apache.avro.tool;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;

public class DataFileUnifySchemasTool implements Tool {

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    if (args.size() < 1) {
      err.println("Expected at least 1 argument: input_file1 [input_file2 [input_file3 ...]]");
      return 1;
    }
    Schema unified = null;
    for (String arg : args) {
      DataFileReader<Void> reader =
        new DataFileReader<Void>(new File(arg),
                                 new GenericDatumReader<Void>());
      if (unified == null) {
        unified = reader.getSchema();
      }
      else {
        unified = unified.unify(reader.getSchema());
      }
    }
    out.println(unified.toString(true));
    return 0;
  }

  @Override
  public String getName() {
    return "unifyschemas";
  }

  @Override
  public String getShortDescription() {
    return "Prints out a unified schema for reading any of the data files.";
  }

}
