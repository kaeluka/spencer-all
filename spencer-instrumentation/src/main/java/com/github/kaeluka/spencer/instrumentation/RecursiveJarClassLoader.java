package com.github.kaeluka.spencer.instrumentation;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

@Deprecated
public class RecursiveJarClassLoader extends URLClassLoader {
	private boolean cleanUp = true;

	public RecursiveJarClassLoader(ClassLoader launcherClassLoader) {

        super(getUrls(launcherClassLoader), launcherClassLoader.getParent());
		for (final URL url : super.getURLs()) {
			try {
				if (url.getFile().endsWith(".jar")) {
					JarFile jarFile = new JarFile(url.getPath());
					System.out.println("extracting jar files from "+url.toString().replace("file:", ""));
					for (JarEntry e : Collections.list(jarFile.entries())) {
						final String name = Paths.get("./"+e.getName()).getFileName().toString();
						if (name.endsWith(".jar")) {
							System.out.println(name+ "src/main");
							final URL recUrl = new URL("file:./"+name);
							final InputStream inputStream = jarFile.getInputStream(e);
							final byte[] byteArray = IOUtils.toByteArray(inputStream);
							final File recJarFile = new File(recUrl.getPath());
							if (!recJarFile.exists()) {
								recJarFile.createNewFile();
							}
							if (this.cleanUp) {
								recJarFile.deleteOnExit();
							}
							FileOutputStream recJarFileStream = new FileOutputStream(recJarFile);
							recJarFileStream.write(byteArray);
							recJarFileStream.close();
							this.addURL(recUrl);
						}
					}
					jarFile.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
    }

	public void unsetCleanUp() {
		this.cleanUp = false;
	}

	public void setCleanUp() {
		this.cleanUp = true;
	}

    private static URL[] getUrls(ClassLoader cl) {
        final URL[] urls = ((URLClassLoader) cl).getURLs();
        return urls;
    }

//	public RecursiveJarClassLoader(URL urls[]) {
//		this(urls, true);
//	}

}
