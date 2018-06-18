package spark_etl.translate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class FldExcelToJson {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Please in put source file and target file");
			System.exit(1);
			return;
		}

		String sourceExcelFile = args[1];
		String targetJsonFileFolder = args[2];

		File excelFile = new File(sourceExcelFile);
		File jsonFileFolder = new File(targetJsonFileFolder);
		
		if (excelFile.isFile() && excelFile.exists()&& jsonFileFolder.isDirectory()) {
			convertExcelToJson(excelFile, new File(targetJsonFileFolder));
		} else {
			System.err.println("source file is not a file or not exists");
			System.exit(1);
		}

	}

	private static void convertExcelToJson(File excelFile, File jsonFile) {
		// throw new NotSupportedException("convertExcelToJson is a TODO list");
		Workbook wb = null;
		try {
			wb = WorkbookFactory.create(excelFile);
		} catch (EncryptedDocumentException e) {
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
		
		
		
		try {
			wb.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
