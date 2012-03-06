package org.sooo;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class AvroTest {
	String avscPath = "src/main/resources/Pair.avsc";
	Parser schemaParser = new Schema.Parser();
	Schema schema;

	@Before
	public void setUp() throws IOException {
		String avscContents = Files
				.toString(new File(avscPath), Charsets.UTF_8);
		schema = schemaParser.parse(avscContents);
	}

	@Test
	public void prettifySchema() throws IOException {
		System.out.println(schema.toString(true));
	}

	@Test
	public void createGenericRecordAndSerializeThenDeserialize()
			throws IOException {
		// given
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("left", new Utf8("L"));
		datum.put("right", new Utf8("R"));

		// serialize
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(
				schema);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();

		// deserialize
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
				schema);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
				null);
		GenericRecord result = reader.read(null, decoder);

		// then
		assertThat(result.get("left").toString(), is("L"));
		assertThat(result.get("right").toString(), is("R"));
	}

	@Test
	public void createPairAndSerializeThenDeserialize() throws IOException {
		// given
		Pair datum = new Pair();
		datum.setLeft(new Utf8("L"));
		datum.setRight(new Utf8("R"));

		// serialize
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<Pair> writer = new SpecificDatumWriter<Pair>(Pair.class);
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(datum, encoder);
		encoder.flush();
		out.close();

		// deserialize
		DatumReader<Pair> reader = new SpecificDatumReader<Pair>(Pair.class);
		Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
				null);
		Pair result = reader.read(null, decoder);

		// then
		assertThat(result.getLeft().toString(), is("L"));
		assertThat(result.getRight().toString(), is("R"));
	}
}
