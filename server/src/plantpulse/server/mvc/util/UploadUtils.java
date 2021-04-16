package plantpulse.server.mvc.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.io.IOUtils;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

public class UploadUtils {
	
	public static File getTempFile(MultipartFile multipartFile)
	{
	    CommonsMultipartFile commonsMultipartFile = (CommonsMultipartFile) multipartFile;
	    FileItem fileItem = commonsMultipartFile.getFileItem();
	    DiskFileItem diskFileItem = (DiskFileItem) fileItem;
	    String absPath = diskFileItem.getStoreLocation().getAbsolutePath();
	    return new File(absPath);
	}
	
	public static String readText(File file)
	{
		String everything = "";
		FileInputStream inputStream = null;
		try {
			inputStream = new FileInputStream(file);
		    everything = IOUtils.toString(inputStream, "UTF-8");
		}catch(Exception ex){
			ex.printStackTrace();
		} finally {
		    try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return everything;
	}

	public static void write(FileInputStream in, String output) {

		InputStream inputStream = null;
		OutputStream outputStream = null;

		try {
			// read this file into InputStream
			inputStream = in;

			// write the inputStream to a FileOutputStream
			outputStream = new FileOutputStream(new File(output));

			int read = 0;
			byte[] bytes = new byte[1024];

			while ((read = inputStream.read(bytes)) != -1) {
				outputStream.write(bytes, 0, read);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (outputStream != null) {
				try {
					// outputStream.flush();
					outputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}

	}

}
