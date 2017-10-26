package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
	private String fileName;

	// parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		File file = new File(this.fileName);
		if (!file.exists()) {
			throw new FileNotFoundException();
		}
	}

	/*
	 * implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file. Note: Return type of the method will be
	 * Header
	 */
	@Override
	public Header getHeader() throws IOException {
		// read the first line
		// populate the header object with the String array containing the header names
		if (null == fileName || fileName.isEmpty()) {
			return null;
		}

		Header header = new Header();
		Path filePath = FileSystems.getDefault().getPath(fileName);
		try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				header.setHeaders(line.split(","));
				return header;
			}
		} catch (IOException ex) {
			System.err.format("IOException occured: {}", ex);
		}
		return null;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {
	}

	/*
	 * implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. Note: Return Type of the method will be
	 * DataTypeDefinitions
	 */
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		if (null == fileName || fileName.isEmpty()) {
			return null;
		}

		Path filePath = FileSystems.getDefault().getPath(fileName);
		String[] data = null;
		try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
			String line = null;
			int count = 0;
			while ((line = reader.readLine()) != null) {
				if (count > 0) {
					data = line.split(",", -1);
					break;
				}
				count++;
			}
		} catch (IOException ex) {
			System.err.format("IOException occured: {}", ex);
		}

		DataTypeDefinitions dataTypeDefinitions = null;
		if (null != data) {
			dataTypeDefinitions = new DataTypeDefinitions();
			List<String> dataTypes = new ArrayList<String>();
			for (String string : data) {
				dataTypes.add(this.isIntegerOrString(string));
			}

			dataTypeDefinitions.setDataTypes(dataTypes.toArray(new String[dataTypes.size()]));
			return dataTypeDefinitions;
		}
		return null;
	}

	private String isIntegerOrString(String string) {
		if (null == string || string.isEmpty()) {
			return String.class.toString().split("class ")[1];
		}

		try {
			Integer.parseInt(string);
			return Integer.class.toString().split("class ")[1];
		} catch (NumberFormatException ex) {
			return String.class.toString().split("class ")[1];
		}
	}
}
