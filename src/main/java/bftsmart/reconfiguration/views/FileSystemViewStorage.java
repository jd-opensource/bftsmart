/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration.views;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Objects;

/**
 *
 * @author eduardo
 */
public class FileSystemViewStorage implements ViewStorage {
	
	private static Logger LOGGER = LoggerFactory.getLogger(FileSystemViewStorage.class);

	private File storageFile;

	private View view;
	
	public FileSystemViewStorage(View initView, String viewStorageDir) {
		this(initView, new File(viewStorageDir));
	}

	public FileSystemViewStorage(View initView, File viewStorageDir) {
		if (viewStorageDir.isFile()) {
			throw new IllegalArgumentException(
					"The view storage dir is acturally a file! --" + viewStorageDir.getAbsolutePath());
		}
		if (!viewStorageDir.exists()) {
			viewStorageDir.mkdirs();
		}
		storageFile = new File(viewStorageDir, "current.view");
		if (initView != null) {
			storeView(initView);
		}else if(storageFile.isFile()) {
			this.view = readView();
		}
	}

	@Override
	public boolean storeView(View view) {
		if (!Objects.equals(this.view,view)) {
			try {
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(storageFile));
				oos.writeObject(view);
				oos.flush();
				oos.close();
				
				this.view = view;
				return true;
			} catch (Exception e) {
				LOGGER.error("Store view error! -- {}, {}", e.getMessage(), e);
				return false;
			}
		}
		return true;
	}

	@Override
	public View readView() {
		if (this.view != null) {
			return view;
		}
		if (!storageFile.exists()) {
			return null;
		}
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(storageFile));
			View ret = (View) ois.readObject();
			ois.close();
				
			this.view = ret;
			return ret;
		} catch (Exception e) {
			LOGGER.error("Read view error! -- {}, {}", e.getMessage(), e);
			return null;
		}
	}

	public byte[] getBytes(View view) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream(4);
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(view);
			return baos.toByteArray();
		} catch (Exception e) {
			return null;
		}
	}

	public View getView(byte[] bytes) {
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return (View) ois.readObject();
		} catch (Exception e) {
			return null;
		}
	}
}
